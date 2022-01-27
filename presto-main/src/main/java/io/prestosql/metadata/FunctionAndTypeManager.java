/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.metadata;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.prestosql.Session;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.window.WindowFunctionSupplier;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.ArrayBlockEncoding;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.ByteArrayBlockEncoding;
import io.prestosql.spi.block.DictionaryBlockEncoding;
import io.prestosql.spi.block.Int128ArrayBlockEncoding;
import io.prestosql.spi.block.IntArrayBlockEncoding;
import io.prestosql.spi.block.LazyBlockEncoding;
import io.prestosql.spi.block.LongArrayBlockEncoding;
import io.prestosql.spi.block.MapBlockEncoding;
import io.prestosql.spi.block.RowBlockEncoding;
import io.prestosql.spi.block.RunLengthBlockEncoding;
import io.prestosql.spi.block.ShortArrayBlockEncoding;
import io.prestosql.spi.block.SingleMapBlockEncoding;
import io.prestosql.spi.block.SingleRowBlockEncoding;
import io.prestosql.spi.block.VariableWidthBlockEncoding;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.BuiltInScalarFunctionImplementation;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.FunctionNamespaceManager;
import io.prestosql.spi.function.FunctionNamespaceManagerFactory;
import io.prestosql.spi.function.FunctionNamespaceTransactionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.ScalarFunctionImplementation;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.function.SqlFunction;
import io.prestosql.spi.function.SqlInvokedFunction;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.gen.CacheStatsMBean;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.type.InternalTypeManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.SystemSessionProperties.isListBuiltInFunctionsOnly;
import static io.prestosql.metadata.CastType.toOperatorType;
import static io.prestosql.metadata.FunctionResolver.constructFunctionNotFoundErrorMessage;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.connector.CatalogSchemaName.DEFAULT_NAMESPACE;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class FunctionAndTypeManager
        implements FunctionMetadataManager, TypeManager
{
    private final TransactionManager transactionManager;
    private final BuiltInTypeRegistry builtInTypeRegistry;
    private final BuiltInFunctionNamespaceManager builtInFunctionNamespaceManager;
    private final FunctionInvokerProvider functionInvokerProvider;
    private final Map<String, FunctionNamespaceManagerFactory> functionNamespaceManagerFactories = new ConcurrentHashMap<>();
    private final HandleResolver handleResolver;
    private final Map<String, FunctionNamespaceManager<? extends SqlFunction>> functionNamespaceManagers = new ConcurrentHashMap<>();
    private final FunctionResolver functionResolver;
    private final LoadingCache<FunctionResolutionCacheKey, FunctionHandle> functionCache;
    private final CacheStatsMBean cacheStatsMBean;
    private final ConcurrentMap<String, BlockEncoding> blockEncodings = new ConcurrentHashMap<>();
    private final Kryo kryo;

    @Inject
    public FunctionAndTypeManager(
            TransactionManager transactionManager,
            FeaturesConfig featuresConfig,
            HandleResolver handleResolver,
            Set<Type> types,
            Kryo kryo)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.builtInFunctionNamespaceManager = new BuiltInFunctionNamespaceManager(featuresConfig, this);
        this.functionNamespaceManagers.put(DEFAULT_NAMESPACE.getCatalogName(), builtInFunctionNamespaceManager);
        this.functionInvokerProvider = new FunctionInvokerProvider(this);
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.builtInTypeRegistry = new BuiltInTypeRegistry(types, featuresConfig, this);
        // TODO: Provide a more encapsulated way for TransactionManager to register FunctionNamespaceManager
        transactionManager.registerFunctionNamespaceManager(DEFAULT_NAMESPACE.getCatalogName(), builtInFunctionNamespaceManager);
        this.functionCache = CacheBuilder.newBuilder()
                .recordStats()
                .maximumSize(1000)
                .build(CacheLoader.from(key -> resolveBuiltInFunction(key.functionName, fromTypeSignatures(key.parameterTypes))));
        this.cacheStatsMBean = new CacheStatsMBean(functionCache);
        this.functionResolver = new FunctionResolver(this);
        this.kryo = requireNonNull(kryo, "Kryo Object cannot be null");

        // add the built-in BlockEncodings
        addBlockEncoding(new VariableWidthBlockEncoding());
        addBlockEncoding(new ByteArrayBlockEncoding());
        addBlockEncoding(new ShortArrayBlockEncoding());
        addBlockEncoding(new IntArrayBlockEncoding());
        addBlockEncoding(new LongArrayBlockEncoding());
        addBlockEncoding(new Int128ArrayBlockEncoding());
        addBlockEncoding(new DictionaryBlockEncoding());
        addBlockEncoding(new ArrayBlockEncoding());
        addBlockEncoding(new MapBlockEncoding(new InternalTypeManager(this)));
        addBlockEncoding(new SingleMapBlockEncoding(new InternalTypeManager(this)));
        addBlockEncoding(new RowBlockEncoding());
        addBlockEncoding(new SingleRowBlockEncoding());
        addBlockEncoding(new RunLengthBlockEncoding());
        addBlockEncoding(new LazyBlockEncoding());
    }

    public static FunctionAndTypeManager createTestFunctionAndTypeManager()
    {
        return new FunctionAndTypeManager(createTestTransactionManager(), new FeaturesConfig(), new HandleResolver(), ImmutableSet.of(), new Kryo());
    }

    public BlockEncoding getBlockEncoding(String encodingName)
    {
        BlockEncoding blockEncoding = blockEncodings.get(encodingName);
        checkArgument(blockEncoding != null, "Unknown block encoding: %s", encodingName);
        return blockEncoding;
    }

    public BlockEncodingSerde getBlockEncodingSerde()
    {
        return new InternalBlockEncodingSerde(this);
    }

    public BlockEncodingSerde getBlockKryoEncodingSerde()
    {
        return new KryoBlockEncodingSerde(this, kryo);
    }

    public void addBlockEncoding(BlockEncoding blockEncoding)
    {
        requireNonNull(blockEncoding, "blockEncoding is null");
        BlockEncoding existingEntry = blockEncodings.putIfAbsent(blockEncoding.getName(), blockEncoding);
        checkArgument(existingEntry == null, "Encoding already registered: %s", blockEncoding.getName());
    }

    @Managed
    @Nested
    public CacheStatsMBean getFunctionResolutionCacheStats()
    {
        return cacheStatsMBean;
    }

    public void loadFunctionNamespaceManager(
            HetuMetaStoreManager hetuMetaStoreManager,
            String functionNamespaceManagerName,
            String catalogName,
            Map<String, String> properties)
    {
        requireNonNull(hetuMetaStoreManager, "hetuMetaStoreManager is nll");
        FunctionNamespaceManagerContextInstance functionNamespaceManagerContextInstance = new FunctionNamespaceManagerContextInstance(hetuMetaStoreManager.getHetuMetastore());
        requireNonNull(functionNamespaceManagerName, "functionNamespaceManagerName is null");
        FunctionNamespaceManagerFactory factory = functionNamespaceManagerFactories.get(functionNamespaceManagerName);
        checkState(factory != null, "No factory for function namespace manager %s", functionNamespaceManagerName);
        FunctionNamespaceManager<?> functionNamespaceManager = factory.create(catalogName, properties, functionNamespaceManagerContextInstance);

        transactionManager.registerFunctionNamespaceManager(catalogName, functionNamespaceManager);
        if (functionNamespaceManagers.putIfAbsent(catalogName, functionNamespaceManager) != null) {
            throw new IllegalArgumentException(format("Function namespace manager is already registered for catalog [%s]", catalogName));
        }
    }

    @VisibleForTesting
    public void addFunctionNamespace(String catalogName, FunctionNamespaceManager functionNamespaceManager)
    {
        transactionManager.registerFunctionNamespaceManager(catalogName, functionNamespaceManager);
        if (functionNamespaceManagers.putIfAbsent(catalogName, functionNamespaceManager) != null) {
            throw new IllegalArgumentException(format("Function namespace manager is already registered for catalog [%s]", catalogName));
        }
    }

    public FunctionInvokerProvider getFunctionInvokerProvider()
    {
        return functionInvokerProvider;
    }

    public void addFunctionNamespaceFactory(FunctionNamespaceManagerFactory factory)
    {
        if (functionNamespaceManagerFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Resource group configuration manager '%s' is already registered", factory.getName()));
        }
        handleResolver.addFunctionNamespace(factory.getName(), factory.getHandleResolver());
    }

    public void registerBuiltInFunctions(List<? extends SqlFunction> functions)
    {
        builtInFunctionNamespaceManager.registerBuiltInFunctions(functions);
    }

    // this method will filter out all the function which is hidden
    public List<SqlFunction> listFunctions(Optional<Session> session)
    {
        boolean isListBuiltInFunctionsOnly = false;
        if (session.isPresent()) {
            isListBuiltInFunctionsOnly = isListBuiltInFunctionsOnly(session.get());
        }
        Collection<FunctionNamespaceManager<?>> managers = isListBuiltInFunctionsOnly ?
                ImmutableSet.of(builtInFunctionNamespaceManager) :
                functionNamespaceManagers.values();
        return managers.stream()
                .flatMap(manager -> manager.listFunctions().stream())
                .filter(function -> !function.isHidden())
                .collect(toImmutableList());
    }

    // this method will not filter out all the function which visibility is HIDDEN
    public List<SqlFunction> listFunctionsWithoutFilterOut(Optional<Session> session)
    {
        boolean isListBuiltInFunctionsOnly = false;
        if (session.isPresent()) {
            isListBuiltInFunctionsOnly = isListBuiltInFunctionsOnly(session.get());
        }
        Collection<FunctionNamespaceManager<?>> managers = isListBuiltInFunctionsOnly ?
                ImmutableSet.of(builtInFunctionNamespaceManager) :
                functionNamespaceManagers.values();
        return managers.stream()
                .flatMap(manager -> manager.listFunctions().stream())
                .collect(toImmutableList());
    }

    public Signature getBuiltInSignature(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        return ((BuiltInFunctionHandle) functionHandle).getSignature();
    }

    public Signature resolveBuiltInFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        QualifiedObjectName qualifiedObjectName = FunctionAndTypeManager.qualifyObjectName(name);
        FunctionHandle functionHandle = resolveFunction(Optional.empty(), qualifiedObjectName, parameterTypes);

        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        return ((BuiltInFunctionHandle) functionHandle).getSignature();
    }

    public Collection<? extends SqlFunction> getFunctions(Optional<TransactionId> transactionId, QualifiedObjectName functionName)
    {
        Optional<FunctionNamespaceManager<? extends SqlFunction>> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getCatalogSchemaName());
        if (!functionNamespaceManager.isPresent()) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Function not found: %s", functionName));
        }

        if (transactionId.isPresent()) {
            Optional<FunctionNamespaceTransactionHandle> transactionHandle = transactionId.map(
                    id -> transactionManager.getFunctionNamespaceTransaction(id, functionName.getCatalogName()));
            return functionNamespaceManager.get().getFunctions(transactionHandle, functionName);
        }
        else {
            return functionNamespaceManager.get().listFunctions().stream().filter(sqlFunction -> sqlFunction.getSignature().getName().equals(functionName)).collect(toImmutableList());
        }
    }

    @Override
    public void createFunction(SqlInvokedFunction function, boolean replace)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(function.getSignature().getName().getCatalogSchemaName());
        if (!functionNamespaceManager.isPresent()) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Cannot create function in function namespace: %s", function.getFunctionId().getFunctionName().getCatalogSchemaName()));
        }
        functionNamespaceManager.get().createFunction(function, replace);
    }

    public void addType(Type type)
    {
        builtInTypeRegistry.addType(type);
    }

    public void addParametricType(ParametricType parametricType)
    {
        builtInTypeRegistry.addParametricType(parametricType);
    }

    public static QualifiedObjectName qualifyObjectName(QualifiedName name)
    {
        if (!name.getPrefix().isPresent()) {
            return QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, name.getSuffix());
        }
        if (name.getOriginalParts().size() != 3) {
            throw new PrestoException(FUNCTION_NOT_FOUND, format("Non-builtin functions must be referenced by 'catalog.schema.function_name', found: %s", name));
        }
        return QualifiedObjectName.valueOf(new CatalogSchemaName(name.getOriginalParts().get(0).getValue(), name.getOriginalParts().get(1).getValue()), name.getOriginalParts().get(2).getValue());
    }

    /**
     * Resolves a function using implicit type coercions. We enforce explicit naming for dynamic function namespaces.
     * All unqualified function names will only be resolved against the built-in static function namespace. While it is
     * possible to define an ordering (through SQL path or other means) and convention (best match / first match), in
     * reality when complicated namespaces are involved such implicit resolution might hide errors and cause confusion.
     *
     * @throws PrestoException if there are no matches or multiple matches
     */
    public FunctionHandle resolveFunction(Optional<TransactionId> transactionId, QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        if (functionName.getCatalogSchemaName().equals(DEFAULT_NAMESPACE) && parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency)) {
            return lookupCachedFunction(functionName, parameterTypes);
        }
        return resolveFunctionInternal(transactionId, functionName, parameterTypes);
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        return builtInTypeRegistry.getType(signature);
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        return builtInTypeRegistry.getParameterizedType(baseTypeName, typeParameters);
    }

    @Override
    public List<Type> getTypes()
    {
        return builtInTypeRegistry.getTypes();
    }

    @Override
    public Collection<ParametricType> getParametricTypes()
    {
        return builtInTypeRegistry.getParametricTypes();
    }

    @Override
    public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
    {
        return builtInTypeRegistry.getCommonSuperType(firstType, secondType);
    }

    @Override
    public boolean canCoerce(Type actualType, Type expectedType)
    {
        return builtInTypeRegistry.canCoerce(actualType, expectedType);
    }

    @Override
    public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
    {
        return builtInTypeRegistry.isTypeOnlyCoercion(actualType, expectedType);
    }

    @Override
    public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
    {
        return builtInTypeRegistry.coerceTypeBase(sourceType, resultTypeBase);
    }

    @Override
    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getFunctionNamespace());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for '%s'", functionHandle.getFunctionNamespace());
        return functionNamespaceManager.get().getFunctionMetadata(functionHandle);
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getFunctionNamespace());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for '%s'", functionHandle.getFunctionNamespace());
        return functionNamespaceManager.get().getScalarFunctionImplementation(functionHandle);
    }

    public CompletableFuture<Block> executeFunction(FunctionHandle functionHandle, Page inputPage, List<Integer> channels, Session session)
    {
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getFunctionNamespace());
        checkState(functionNamespaceManager.isPresent(), format("FunctionHandle %s should have a serving function namespace", functionHandle));
        Optional<String> catalog = session.getCatalog();
        ConnectorSession connectorSession;
        if (catalog.isPresent()) {
            connectorSession = session.toConnectorSession(new CatalogName(catalog.get()));
        }
        else {
            connectorSession = session.toConnectorSession();
        }
        return functionNamespaceManager.get().executeRemoteFunction(functionHandle, inputPage, channels, this, connectorSession);
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(FunctionHandle functionHandle)
    {
        return builtInFunctionNamespaceManager.getWindowFunctionImplementation(functionHandle);
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(Signature signature)
    {
        return getAggregateFunctionImplementation(lookupFunction(signature.getName().getObjectName(), TypeSignatureProvider.fromTypeSignatures(signature.getArgumentTypes())));
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(FunctionHandle functionHandle)
    {
        return builtInFunctionNamespaceManager.getAggregateFunctionImplementation(functionHandle);
    }

    public BuiltInScalarFunctionImplementation getBuiltInScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        return (BuiltInScalarFunctionImplementation) builtInFunctionNamespaceManager.getScalarFunctionImplementation(functionHandle);
    }

    @VisibleForTesting
    public List<SqlFunction> listOperators()
    {
        Set<QualifiedObjectName> operatorNames = Arrays.asList(OperatorType.values()).stream()
                .map(OperatorType::getFunctionName)
                .collect(toImmutableSet());

        return builtInFunctionNamespaceManager.listFunctions().stream()
                .filter(function -> operatorNames.contains(function.getSignature().getName()))
                .collect(toImmutableList());
    }

    @Override
    public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
    {
        FunctionHandle functionHandle = resolveOperatorFunctionHandle(operatorType, fromTypes(argumentTypes));
        return getBuiltInScalarFunctionImplementation(functionHandle).getMethodHandle();
    }

    public FunctionHandle resolveOperatorFunctionHandle(OperatorType operatorType, List<TypeSignatureProvider> argumentTypes)
    {
        try {
            return resolveFunction(Optional.empty(), operatorType.getFunctionName(), argumentTypes);
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() == FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                throw new OperatorNotFoundException(
                        operatorType,
                        argumentTypes.stream()
                                .map(TypeSignatureProvider::getTypeSignature)
                                .collect(toImmutableList()));
            }
            else {
                throw e;
            }
        }
    }

    /**
     * Lookup up a function with name and fully bound types. This can only be used for builtin functions. {@link #resolveFunction(Optional, QualifiedObjectName, List)}
     * should be used for dynamically registered functions.
     *
     * @throws PrestoException if function could not be found
     */
    public FunctionHandle lookupFunction(String name, List<TypeSignatureProvider> parameterTypes)
    {
        QualifiedObjectName functionName = qualifyObjectName(QualifiedName.of(name));
        if (parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency)) {
            return lookupCachedFunction(functionName, parameterTypes);
        }
        Collection<? extends SqlFunction> candidates = builtInFunctionNamespaceManager.getFunctions(Optional.empty(), functionName);
        return functionResolver.lookupFunction(builtInFunctionNamespaceManager, Optional.empty(), functionName, parameterTypes, candidates);
    }

    public FunctionHandle lookupCast(CastType castType, TypeSignature fromType, TypeSignature toType)
    {
        Signature signature = new Signature(castType.getCastName(), SCALAR, emptyList(), emptyList(), toType, singletonList(fromType), false);

        try {
            builtInFunctionNamespaceManager.getScalarFunctionImplementation(signature);
        }
        catch (PrestoException e) {
            if (castType.isOperatorType() && e.getErrorCode().getCode() == FUNCTION_IMPLEMENTATION_MISSING.toErrorCode().getCode()) {
                throw new OperatorNotFoundException(toOperatorType(castType), ImmutableList.of(fromType), toType);
            }
            throw e;
        }
        return builtInFunctionNamespaceManager.getFunctionHandle(Optional.empty(), signature);
    }

    private FunctionHandle resolveFunctionInternal(Optional<TransactionId> transactionId, QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        FunctionNamespaceManager<?> functionNamespaceManager = getServingFunctionNamespaceManager(functionName.getCatalogSchemaName()).orElse(null);
        if (functionNamespaceManager == null) {
            throw new PrestoException(FUNCTION_NOT_FOUND, constructFunctionNotFoundErrorMessage(functionName, parameterTypes, ImmutableList.of()));
        }

        Optional<FunctionNamespaceTransactionHandle> transactionHandle = transactionId
                .map(id -> transactionManager.getFunctionNamespaceTransaction(id, functionName.getCatalogSchemaName().getCatalogName()));
        Collection<? extends SqlFunction> candidates = functionNamespaceManager.getFunctions(transactionHandle, functionName);

        return functionResolver.resolveFunction(functionNamespaceManager, transactionHandle, functionName, parameterTypes, candidates);
    }

    private FunctionHandle resolveBuiltInFunction(QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        checkArgument(functionName.getCatalogSchemaName().equals(DEFAULT_NAMESPACE), "Expect built-in functions");
        checkArgument(parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency), "Expect parameter types not to have dependency");
        return resolveFunctionInternal(Optional.empty(), functionName, parameterTypes);
    }

    private FunctionHandle lookupCachedFunction(QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        try {
            return functionCache.getUnchecked(new FunctionResolutionCacheKey(functionName, parameterTypes));
        }
        catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof PrestoException) {
                throw (PrestoException) e.getCause();
            }
            throw e;
        }
    }

    private Optional<FunctionNamespaceManager<? extends SqlFunction>> getServingFunctionNamespaceManager(CatalogSchemaName functionNamespace)
    {
        return Optional.ofNullable(functionNamespaceManagers.get(functionNamespace.getCatalogName()));
    }

    private static class FunctionResolutionCacheKey
    {
        private final QualifiedObjectName functionName;
        private final List<TypeSignature> parameterTypes;

        private FunctionResolutionCacheKey(QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes)
        {
            checkArgument(parameterTypes.stream().noneMatch(TypeSignatureProvider::hasDependency), "Only type signatures without dependency can be cached");
            this.functionName = requireNonNull(functionName, "functionName is null");
            this.parameterTypes = requireNonNull(parameterTypes, "parameterTypes is null").stream()
                    .map(TypeSignatureProvider::getTypeSignature)
                    .collect(toImmutableList());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(functionName, parameterTypes);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FunctionResolutionCacheKey other = (FunctionResolutionCacheKey) obj;
            return Objects.equals(this.functionName, other.functionName) &&
                    Objects.equals(this.parameterTypes, other.parameterTypes);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("functionName", functionName)
                    .add("parameterTypes", parameterTypes)
                    .toString();
        }
    }
}
