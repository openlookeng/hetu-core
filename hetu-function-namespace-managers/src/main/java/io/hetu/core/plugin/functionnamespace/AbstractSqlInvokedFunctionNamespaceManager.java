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
package io.hetu.core.plugin.functionnamespace;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionImplementationType;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.FunctionNamespaceManager;
import io.prestosql.spi.function.FunctionNamespaceManagerContext;
import io.prestosql.spi.function.FunctionNamespaceTransactionHandle;
import io.prestosql.spi.function.JDBCScalarFunctionImplementation;
import io.prestosql.spi.function.Parameter;
import io.prestosql.spi.function.ScalarFunctionImplementation;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.function.SqlFunction;
import io.prestosql.spi.function.SqlFunctionHandle;
import io.prestosql.spi.function.SqlFunctionId;
import io.prestosql.spi.function.SqlInvokedFunction;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class AbstractSqlInvokedFunctionNamespaceManager
        implements FunctionNamespaceManager<SqlInvokedFunction>
{
    private final ConcurrentMap<FunctionNamespaceTransactionHandle, FunctionCollection> transactions = new ConcurrentHashMap<>();

    private final String catalogName;
    private final LoadingCache<QualifiedObjectName, Collection<SqlInvokedFunction>> functions;
    private final LoadingCache<SqlFunctionHandle, FunctionMetadata> metadataByHandle;
    private final LoadingCache<SqlFunctionHandle, ScalarFunctionImplementation> implementationByHandle;
    private Optional<FunctionNamespaceManagerContext> functionNamespaceManagerContextOp = Optional.empty();
    SqlInvokedFunctionNamespaceManagerConfig sqlInvokedFunctionNamespaceManagerConfig;

    public AbstractSqlInvokedFunctionNamespaceManager(String catalogName, SqlInvokedFunctionNamespaceManagerConfig config)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        this.sqlInvokedFunctionNamespaceManagerConfig = config;
        this.functions = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<QualifiedObjectName, Collection<SqlInvokedFunction>>()
                {
                    @Override
                    @ParametersAreNonnullByDefault
                    public Collection<SqlInvokedFunction> load(QualifiedObjectName functionName)
                    {
                        Collection<SqlInvokedFunction> functions = fetchFunctionsDirect(functionName);
                        for (SqlInvokedFunction function : functions) {
                            metadataByHandle.put(function.getRequiredFunctionHandle(), sqlInvokedFunctionToMetadata(function));
                        }
                        return functions;
                    }
                });

        this.metadataByHandle = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionInstanceCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<SqlFunctionHandle, FunctionMetadata>()
                {
                    @Override
                    @ParametersAreNonnullByDefault
                    public FunctionMetadata load(SqlFunctionHandle functionHandle)
                    {
                        return fetchFunctionMetadataDirect(functionHandle);
                    }
                });
        this.implementationByHandle = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionInstanceCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<SqlFunctionHandle, ScalarFunctionImplementation>()
                {
                    @Override
                    public ScalarFunctionImplementation load(SqlFunctionHandle functionHandle)
                    {
                        return fetchFunctionImplementationDirect(functionHandle);
                    }
                });
    }

    protected abstract List<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName functionName);

    protected abstract Optional<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName functionName, List<TypeSignature> typeSignatureList);

    protected abstract FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle);

    protected abstract ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle);

    public final void setFunctionNamespaceManagerContext(FunctionNamespaceManagerContext functionNamespaceManagerContext)
    {
        if (functionNamespaceManagerContext != null) {
            functionNamespaceManagerContextOp = Optional.of(functionNamespaceManagerContext);
        }
    }

    @Override
    public final FunctionNamespaceTransactionHandle beginTransaction()
    {
        UuidFunctionNamespaceTransactionHandle transactionHandle = UuidFunctionNamespaceTransactionHandle.create();
        transactions.put(transactionHandle, new FunctionCollection());
        return transactionHandle;
    }

    @Override
    public final void commit(FunctionNamespaceTransactionHandle transactionHandle)
    {
        // Transactional commit is not supported yet.
        transactions.remove(transactionHandle);
    }

    @Override
    public final void abort(FunctionNamespaceTransactionHandle transactionHandle)
    {
        // Transactional rollback is not supported yet.
        transactions.remove(transactionHandle);
    }

    @Override
    public final List<SqlInvokedFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName)
    {
        checkCatalog(functionName);
        if (transactionHandle.isPresent()) {
            return transactions.get(transactionHandle.get()).loadAndGetFunctionsTransactional(functionName);
        }
        return fetchFunctionsDirect(functionName);
    }

    @Override
    public final FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        checkCatalog(signature.getName());
        // This is the only assumption in this class that we're dealing with sql-invoked regular function.
        SqlFunctionId functionId = new SqlFunctionId(signature.getName(), signature.getArgumentTypes());
        if (transactionHandle.isPresent()) {
            return transactions.get(transactionHandle.get()).getFunctionHandle(functionId);
        }
        FunctionCollection collection = new FunctionCollection();
        collection.loadAndGetFunctionsTransactional(signature.getName());
        return collection.getFunctionHandle(functionId);
    }

    @Override
    public final FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        checkCatalog(functionHandle);
        checkArgument(functionHandle instanceof SqlFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());
        return metadataByHandle.getUnchecked((SqlFunctionHandle) functionHandle);
    }

    @Override
    public final ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        checkCatalog(functionHandle);
        checkArgument(functionHandle instanceof SqlFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());
        return implementationByHandle.getUnchecked((SqlFunctionHandle) functionHandle);
    }

    @Override
    public CompletableFuture<Block> executeRemoteFunction(FunctionHandle functionHandle, Page input, List<Integer> channels, TypeManager typeManager, ConnectorSession connectorSession)
    {
        throw new UnsupportedOperationException("For now we do not support function execution.");
    }

    protected String getCatalogName()
    {
        return catalogName;
    }

    protected void checkCatalog(SqlFunction function)
    {
        checkCatalog(function.getSignature().getName());
    }

    protected void checkCatalog(QualifiedObjectName functionName)
    {
        checkCatalog(functionName.getCatalogSchemaName());
    }

    protected void checkCatalog(FunctionHandle functionHandle)
    {
        checkCatalog(functionHandle.getFunctionNamespace());
    }

    protected void checkCatalog(CatalogSchemaName functionNamespace)
    {
        checkArgument(
                catalogName.equals(functionNamespace.getCatalogName()),
                "Catalog [%s] is not served by this FunctionNamespaceManager, expected: %s",
                functionNamespace.getCatalogName(),
                catalogName);
    }

    protected void refreshFunctionsCache(QualifiedObjectName functionName)
    {
        functions.refresh(functionName);
    }

    protected void checkFunctionLanguageSupported(SqlInvokedFunction function)
    {
        if (!this.sqlInvokedFunctionNamespaceManagerConfig.getSupportedFunctionLanguages().contains(function.getRoutineCharacteristics().getLanguage().getLanguage())) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Catalog %s does not support functions implemented in language %s", catalogName, function.getRoutineCharacteristics().getLanguage()));
        }
    }

    protected FunctionMetadata sqlInvokedFunctionToMetadata(SqlInvokedFunction function)
    {
        return new FunctionMetadata(
                function.getSignature().getName(),
                function.getSignature().getArgumentTypes(),
                function.getParameters().stream()
                        .map(Parameter::getName)
                        .collect(toImmutableList()),
                function.getSignature().getReturnType(),
                SCALAR,
                function.getRoutineCharacteristics().getLanguage(),
                getFunctionImplementationType(function),
                function.isDeterministic(),
                function.isCalledOnNullInput());
    }

    protected FunctionImplementationType getFunctionImplementationType(SqlInvokedFunction function)
    {
        return FunctionImplementationType.valueOf(function.getRoutineCharacteristics().getLanguage().getLanguage());
    }

    protected ScalarFunctionImplementation sqlInvokedFunctionToImplementation(SqlInvokedFunction function)
    {
        FunctionImplementationType implementationType = getFunctionImplementationType(function);
        switch (implementationType) {
            case JDBC:
                checkArgument(function.getFunctionHandle().isPresent(), "Need functionHandle to get function implementation");
                return new JDBCScalarFunctionImplementation(function.getFunctionHandle().get(), function.getFunctionProperties().get(""));
            case BUILTIN:
                throw new IllegalStateException(
                        format("SqlInvokedFunction %s has BUILTIN implementation type but %s cannot manage BUILTIN functions", function.getSignature().getName(), this.getClass()));
            default:
                throw new IllegalStateException(format("Unknown function implementation type: %s", implementationType));
        }
    }

    private Collection<SqlInvokedFunction> fetchFunctions(QualifiedObjectName functionName)
    {
        return functions.getUnchecked(functionName);
    }

    private class FunctionCollection
    {
        @GuardedBy("this")
        private final Map<QualifiedObjectName, Collection<SqlInvokedFunction>> functions = new ConcurrentHashMap<>();

        @GuardedBy("this")
        private final Map<SqlFunctionId, SqlFunctionHandle> functionHandles = new ConcurrentHashMap<>();

        public synchronized List<SqlInvokedFunction> loadAndGetFunctionsTransactional(QualifiedObjectName functionName)
        {
            Collection<SqlInvokedFunction> functions = this.functions.computeIfAbsent(functionName, AbstractSqlInvokedFunctionNamespaceManager.this::fetchFunctions);
            functionHandles.putAll(functions.stream().collect(toImmutableMap(SqlInvokedFunction::getFunctionId, SqlInvokedFunction::getRequiredFunctionHandle)));
            return new ArrayList<>(functions);
        }

        public synchronized FunctionHandle getFunctionHandle(SqlFunctionId functionId)
        {
            return functionHandles.get(functionId);
        }
    }
}
