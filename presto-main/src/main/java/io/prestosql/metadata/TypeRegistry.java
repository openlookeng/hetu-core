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

import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.prestosql.cache.NonEvictableCache;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.CharParametricType;
import io.prestosql.spi.type.DecimalParametricType;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeNotFoundException;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.spi.type.TypeParameter;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarcharParametricType;
import io.prestosql.sql.analyzer.FeaturesConfig;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.prestosql.cache.CacheUtils.uncheckedCacheGet;
import static io.prestosql.cache.SafeCaches.buildNonEvictableCache;
import static io.prestosql.spi.function.IcebergInvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.prestosql.spi.function.IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.IcebergInvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.prestosql.spi.function.IcebergInvocationConvention.simpleConvention;
import static io.prestosql.spi.function.OperatorType.COMPARISON_UNORDERED_FIRST;
import static io.prestosql.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.XX_HASH_64;
import static io.prestosql.spi.type.ArrayParametricType.ARRAY;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.LikePatternType.LIKE_PATTERN;
import static io.prestosql.spi.type.MapParametricType.MAP;
import static io.prestosql.spi.type.P4HyperLogLogType.P4_HYPER_LOG_LOG;
import static io.prestosql.spi.type.QuantileDigestParametricType.QDIGEST;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.RowParametricType.ROW;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.UnknownType.UNKNOWN;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.type.CodePointsType.CODE_POINTS;
import static io.prestosql.type.ColorType.COLOR;
import static io.prestosql.type.FunctionParametricType.FUNCTION;
import static io.prestosql.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.prestosql.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.prestosql.type.IpAddressType.IPADDRESS;
import static io.prestosql.type.JoniRegexpType.JONI_REGEXP;
import static io.prestosql.type.JsonPathType.JSON_PATH;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.type.UuidType.UUID;
import static io.prestosql.type.setdigest.SetDigestType.SET_DIGEST;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class TypeRegistry
{
    private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ParametricType> parametricTypes = new ConcurrentHashMap<>();

    private final NonEvictableCache<TypeSignature, Type> parametricTypeCache;
    private final TypeManager typeManager;
    private final TypeOperators typeOperators;

    @Inject
    public TypeRegistry(TypeOperators typeOperators, FeaturesConfig featuresConfig)
    {
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        requireNonNull(featuresConfig, "featuresConfig is null");

        // Manually register UNKNOWN type without a verifyTypeClass call since it is a special type that cannot be used by functions
        this.types.put(UNKNOWN.getTypeSignature(), UNKNOWN);

        // always add the built-in types; Trino will not function without these
        addType(BOOLEAN);
        addType(BIGINT);
        addType(INTEGER);
        addType("int", INTEGER);
        addType(SMALLINT);
        addType(TINYINT);
        addType(DOUBLE);
        addType(REAL);
        addType(VARBINARY);
        addType(DATE);
        addType(INTERVAL_YEAR_MONTH);
        addType(INTERVAL_DAY_TIME);
        addType(HYPER_LOG_LOG);
        addType(SET_DIGEST);
        addType(P4_HYPER_LOG_LOG);
        addType(JONI_REGEXP);
        addType(LIKE_PATTERN);
        addType(JSON_PATH);
        addType(COLOR);
        addType(JSON);
        addType(CODE_POINTS);
        addType(IPADDRESS);
        addType(UUID);
        addParametricType(VarcharParametricType.VARCHAR);
        addParametricType(CharParametricType.CHAR);
        addParametricType(DecimalParametricType.DECIMAL);
        addParametricType(ROW);
        addParametricType(ARRAY);
        addParametricType(MAP);
        addParametricType(FUNCTION);
        addParametricType(QDIGEST);

        parametricTypeCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));

        typeManager = new InternalTypeManager(this, typeOperators);
    }

    public Type getType(TypeSignature signature)
    {
        Type type = types.get(signature);
        if (type == null) {
            try {
                return uncheckedCacheGet(parametricTypeCache, signature, () -> instantiateParametricType(signature));
            }
            catch (UncheckedExecutionException e) {
                throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        }
        return type;
    }

    private Type instantiateParametricType(TypeSignature signature)
    {
        List<TypeParameter> parameters = new ArrayList<>();

        for (TypeSignatureParameter parameter : signature.getParameters()) {
            TypeParameter typeParameter = TypeParameter.of(parameter, typeManager);
            parameters.add(typeParameter);
        }

        ParametricType parametricType = parametricTypes.get(signature.getBase().toLowerCase(Locale.ENGLISH));
        if (parametricType == null) {
            throw new TypeNotFoundException(signature);
        }

        Type instantiatedType;
        try {
            instantiatedType = parametricType.createType(typeManager, parameters);
        }
        catch (IllegalArgumentException e) {
            throw new TypeNotFoundException(signature, e);
        }

        // TODO: reimplement this check? Currently "varchar(Integer.MAX_VALUE)" fails with "varchar"
        return instantiatedType;
    }

    public Collection<Type> getTypes()
    {
        return ImmutableList.copyOf(types.values());
    }

    public Collection<ParametricType> getParametricTypes()
    {
        return ImmutableList.copyOf(parametricTypes.values());
    }

    public void addType(Type type)
    {
        requireNonNull(type, "type is null");
        Type existingType = types.putIfAbsent(type.getTypeSignature(), type);
    }

    public void addType(String alias, Type type)
    {
        requireNonNull(alias, "alias is null");
        requireNonNull(type, "type is null");

        Type existingType = types.putIfAbsent(new TypeSignature(alias), type);
    }

    public void addParametricType(ParametricType parametricType)
    {
        String name = parametricType.getName().toLowerCase(Locale.ENGLISH);
        checkArgument(!parametricTypes.containsKey(name), "Parametric type already registered: %s", name);
        parametricTypes.putIfAbsent(name, parametricType);
    }

    public TypeOperators getTypeOperators()
    {
        return typeOperators;
    }

    public void verifyTypes()
    {
        Set<Type> missingOperatorDeclaration = new HashSet<>();
        Multimap<Type, OperatorType> missingOperators = HashMultimap.create();
        for (Type type : ImmutableList.copyOf(types.values())) {
            if (type.getTypeOperatorDeclaration(typeOperators) == null) {
                missingOperatorDeclaration.add(type);
                continue;
            }
            if (type.isComparable()) {
                if (!hasEqualMethod(type)) {
                    missingOperators.put(type, EQUAL);
                }
                if (!hasHashCodeMethod(type)) {
                    missingOperators.put(type, HASH_CODE);
                }
                if (!hasXxHash64Method(type)) {
                    missingOperators.put(type, XX_HASH_64);
                }
                if (!hasDistinctFromMethod(type)) {
                    missingOperators.put(type, IS_DISTINCT_FROM);
                }
                if (!hasIndeterminateMethod(type)) {
                    missingOperators.put(type, INDETERMINATE);
                }
            }
            if (type.isOrderable()) {
                if (!hasComparisonUnorderedLastMethod(type)) {
                    missingOperators.put(type, COMPARISON_UNORDERED_LAST);
                }
                if (!hasComparisonUnorderedFirstMethod(type)) {
                    missingOperators.put(type, COMPARISON_UNORDERED_FIRST);
                }
                if (!hasLessThanMethod(type)) {
                    missingOperators.put(type, LESS_THAN);
                }
                if (!hasLessThanOrEqualMethod(type)) {
                    missingOperators.put(type, LESS_THAN_OR_EQUAL);
                }
            }
        }
        // TODO: verify the parametric types too
        if (!missingOperators.isEmpty()) {
            List<String> messages = new ArrayList<>();
            for (Type type : missingOperatorDeclaration) {
                messages.add(String.format("%s types operators is null", type));
            }
            for (Type type : missingOperators.keySet()) {
                messages.add(String.format("%s missing for %s", missingOperators.get(type), type));
            }
            throw new IllegalStateException(Joiner.on(", ").join(messages));
        }
    }

    private boolean hasEqualMethod(Type type)
    {
        try {
            typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasHashCodeMethod(Type type)
    {
        try {
            typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasXxHash64Method(Type type)
    {
        try {
            typeOperators.getXxHash64Operator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasDistinctFromMethod(Type type)
    {
        try {
            typeOperators.getDistinctFromOperator(type, simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE, BOXED_NULLABLE));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasIndeterminateMethod(Type type)
    {
        try {
            typeOperators.getIndeterminateOperator(type, simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasComparisonUnorderedLastMethod(Type type)
    {
        try {
            typeOperators.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
    }

    private boolean hasComparisonUnorderedFirstMethod(Type type)
    {
        try {
            typeOperators.getComparisonUnorderedFirstOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
    }

    private boolean hasLessThanMethod(Type type)
    {
        try {
            typeOperators.getLessThanOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
    }

    private boolean hasLessThanOrEqualMethod(Type type)
    {
        try {
            typeOperators.getLessThanOrEqualOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
    }

    private static final class InternalTypeManager
            implements TypeManager
    {
        private final TypeRegistry typeRegistry;
        private final TypeOperators typeOperators;

        @Inject
        public InternalTypeManager(TypeRegistry typeRegistry, TypeOperators typeOperators)
        {
            this.typeRegistry = requireNonNull(typeRegistry, "typeRegistry is null");
            this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        }

        @Override
        public Type getType(TypeSignature signature)
        {
            return typeRegistry.getType(signature);
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            return null;
        }

        @Override
        public List<Type> getTypes()
        {
            return null;
        }

        @Override
        public Collection<ParametricType> getParametricTypes()
        {
            return null;
        }

        @Override
        public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
        {
            return Optional.empty();
        }

        @Override
        public boolean canCoerce(Type actualType, Type expectedType)
        {
            return false;
        }

        @Override
        public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
        {
            return false;
        }

        @Override
        public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
        {
            return Optional.empty();
        }

        @Override
        public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
        {
            return null;
        }

        @Override
        public Type fromSqlType(String type)
        {
            return null;
        }

        @Override
        public Type getType(TypeId id)
        {
            return null;
        }

        @Override
        public TypeOperators getTypeOperators()
        {
            return typeOperators;
        }
    }
}
