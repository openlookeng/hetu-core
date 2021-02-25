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
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slices;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.ExpressionDomainTranslator;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.StringLiteral;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.orc.TupleDomainFilter.IS_NOT_NULL;
import static io.prestosql.orc.TupleDomainFilter.IS_NULL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.ExpressionUtils.or;
import static io.prestosql.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.type.ColorType.COLOR;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTupleDomainFilterUtils
{
    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();

    private static final Symbol C_BIGINT = new Symbol("c_bigint");
    private static final Symbol C_DOUBLE = new Symbol("c_double");
    private static final Symbol C_VARCHAR = new Symbol("c_varchar");
    private static final Symbol C_BOOLEAN = new Symbol("c_boolean");
    private static final Symbol C_BIGINT_1 = new Symbol("c_bigint_1");
    private static final Symbol C_DOUBLE_1 = new Symbol("c_double_1");
    private static final Symbol C_VARCHAR_1 = new Symbol("c_varchar_1");
    private static final Symbol C_TIMESTAMP = new Symbol("c_timestamp");
    private static final Symbol C_DATE = new Symbol("c_date");
    private static final Symbol C_COLOR = new Symbol("c_color");
    private static final Symbol C_HYPER_LOG_LOG = new Symbol("c_hyper_log_log");
    private static final Symbol C_VARBINARY = new Symbol("c_varbinary");
    private static final Symbol C_DECIMAL_26_5 = new Symbol("c_decimal_26_5");
    private static final Symbol C_DECIMAL_23_4 = new Symbol("c_decimal_23_4");
    private static final Symbol C_INTEGER = new Symbol("c_integer");
    private static final Symbol C_CHAR = new Symbol("c_char");
    private static final Symbol C_DECIMAL_21_3 = new Symbol("c_decimal_21_3");
    private static final Symbol C_DECIMAL_12_2 = new Symbol("c_decimal_12_2");
    private static final Symbol C_DECIMAL_6_1 = new Symbol("c_decimal_6_1");
    private static final Symbol C_DECIMAL_3_0 = new Symbol("c_decimal_3_0");
    private static final Symbol C_DECIMAL_2_0 = new Symbol("c_decimal_2_0");
    private static final Symbol C_SMALLINT = new Symbol("c_smallint");
    private static final Symbol C_TINYINT = new Symbol("c_tinyint");
    private static final Symbol C_REAL = new Symbol("c_real");

    private static final TypeProvider TYPES = TypeProvider.copyOf(ImmutableMap.<Symbol, Type>builder()
            .put(C_BIGINT, BIGINT)
            .put(C_DOUBLE, DOUBLE)
            .put(C_VARCHAR, VARCHAR)
            .put(C_BOOLEAN, BOOLEAN)
            .put(C_BIGINT_1, BIGINT)
            .put(C_DOUBLE_1, DOUBLE)
            .put(C_VARCHAR_1, VARCHAR)
            .put(C_TIMESTAMP, TIMESTAMP)
            .put(C_DATE, DATE)
            .put(C_COLOR, COLOR) // Equatable, but not orderable
            .put(C_HYPER_LOG_LOG, HYPER_LOG_LOG) // Not Equatable or orderable
            .put(C_VARBINARY, VARBINARY)
            .put(C_DECIMAL_26_5, createDecimalType(26, 5))
            .put(C_DECIMAL_23_4, createDecimalType(23, 4))
            .put(C_INTEGER, INTEGER)
            .put(C_CHAR, createCharType(10))
            .put(C_DECIMAL_21_3, createDecimalType(21, 3))
            .put(C_DECIMAL_12_2, createDecimalType(12, 2))
            .put(C_DECIMAL_6_1, createDecimalType(6, 1))
            .put(C_DECIMAL_3_0, createDecimalType(3, 0))
            .put(C_DECIMAL_2_0, createDecimalType(2, 0))
            .put(C_SMALLINT, SMALLINT)
            .put(C_TINYINT, TINYINT)
            .put(C_REAL, REAL)
            .build());

    private Metadata metadata;
    private LiteralEncoder literalEncoder;

    @BeforeClass
    public void setup()
    {
        metadata = createTestMetadataManager();
        literalEncoder = new LiteralEncoder(metadata);
    }

    @Test
    public void testBoolean()
    {
        assertEquals(toFilter(equal(C_BOOLEAN, TRUE_LITERAL)), TupleDomainFilter.BooleanValue.of(true, false));
        assertEquals(toFilter(equal(C_BOOLEAN, FALSE_LITERAL)), TupleDomainFilter.BooleanValue.of(false, false));

        assertEquals(toFilter(not(equal(C_BOOLEAN, TRUE_LITERAL))), TupleDomainFilter.BooleanValue.of(false, false));
        assertEquals(toFilter(not(equal(C_BOOLEAN, FALSE_LITERAL))), TupleDomainFilter.BooleanValue.of(true, false));

        assertEquals(toFilter(isDistinctFrom(C_BOOLEAN, TRUE_LITERAL)), TupleDomainFilter.BooleanValue.of(false, true));
        assertEquals(toFilter(isDistinctFrom(C_BOOLEAN, FALSE_LITERAL)), TupleDomainFilter.BooleanValue.of(true, true));
    }

    @Test
    public void testBigint()
    {
        assertEquals(toFilter(equal(C_BIGINT, bigintLiteral(2L))), TupleDomainFilter.BigintRange.of(2L, 2L, false));
        assertEquals(toFilter(not(equal(C_BIGINT, bigintLiteral(2L)))),
                TupleDomainFilter.BigintMultiRange.of(ImmutableList.of(
                        TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, 1L, false),
                        TupleDomainFilter.BigintRange.of(3L, Long.MAX_VALUE, false)), false));

        assertEquals(toFilter(lessThan(C_BIGINT, bigintLiteral(2L))), TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, 1L, false));
        assertEquals(toFilter(lessThanOrEqual(C_BIGINT, bigintLiteral(2L))), TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, 2L, false));
        assertEquals(toFilter(not(lessThan(C_BIGINT, bigintLiteral(2L)))), TupleDomainFilter.BigintRange.of(2L, Long.MAX_VALUE, false));
        assertEquals(toFilter(not(lessThanOrEqual(C_BIGINT, bigintLiteral(2L)))), TupleDomainFilter.BigintRange.of(3L, Long.MAX_VALUE, false));

        assertEquals(toFilter(greaterThan(C_BIGINT, bigintLiteral(2L))), TupleDomainFilter.BigintRange.of(3L, Long.MAX_VALUE, false));
        assertEquals(toFilter(greaterThanOrEqual(C_BIGINT, bigintLiteral(2L))), TupleDomainFilter.BigintRange.of(2L, Long.MAX_VALUE, false));
        assertEquals(toFilter(not(greaterThan(C_BIGINT, bigintLiteral(2L)))), TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, 2L, false));
        assertEquals(toFilter(not(greaterThanOrEqual(C_BIGINT, bigintLiteral(2L)))), TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, 1L, false));

        assertEquals(toFilter(in(C_BIGINT, ImmutableList.of(1L, 10L, 100L))), TupleDomainFilter.BigintValues.of(new long[] {1, 10, 100}, false));
        assertEquals(toFilter(not(in(C_BIGINT, ImmutableList.of(1L, 10L, 100L)))), TupleDomainFilter.BigintMultiRange.of(ImmutableList.of(
                TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, 0L, false),
                TupleDomainFilter.BigintRange.of(2L, 9L, false),
                TupleDomainFilter.BigintRange.of(11L, 99L, false),
                TupleDomainFilter.BigintRange.of(101, Long.MAX_VALUE, false)), false));

        assertEquals(toFilter(between(C_BIGINT, bigintLiteral(1L), bigintLiteral(10L))), TupleDomainFilter.BigintRange.of(1L, 10L, false));
        assertEquals(toFilter(not(between(C_BIGINT, bigintLiteral(1L), bigintLiteral(10L)))), TupleDomainFilter.BigintMultiRange.of(ImmutableList.of(
                TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, 0L, false),
                TupleDomainFilter.BigintRange.of(11L, Long.MAX_VALUE, false)), false));

        assertEquals(toFilter(isDistinctFrom(C_BIGINT, bigintLiteral(1L))), TupleDomainFilter.BigintMultiRange.of(ImmutableList.of(
                TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, 0L, false),
                TupleDomainFilter.BigintRange.of(2L, Long.MAX_VALUE, false)), true));
        assertEquals(toFilter(not(isDistinctFrom(C_BIGINT, bigintLiteral(1L)))), TupleDomainFilter.BigintRange.of(1, 1, false));

        assertEquals(toFilter(isNull(C_BIGINT)), IS_NULL);
        assertEquals(toFilter(not(isNull(C_BIGINT))), IS_NOT_NULL);
        assertEquals(toFilter(isNotNull(C_BIGINT)), IS_NOT_NULL);
        assertEquals(toFilter(not(isNotNull(C_BIGINT))), IS_NULL);

        assertEquals(toFilter(or(equal(C_BIGINT, bigintLiteral(2L)), isNull(C_BIGINT))), TupleDomainFilter.BigintRange.of(2, 2, true));
        assertEquals(toFilter(or(not(equal(C_BIGINT, bigintLiteral(2L))), isNull(C_BIGINT))),
                TupleDomainFilter.BigintMultiRange.of(ImmutableList.of(
                        TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, 1L, false),
                        TupleDomainFilter.BigintRange.of(3L, Long.MAX_VALUE, false)), true));
        assertEquals(toFilter(or(in(C_BIGINT, ImmutableList.of(1L, 10L, 100L)), isNull(C_BIGINT))), TupleDomainFilter.BigintValues.of(new long[] {1, 10, 100}, true));
        assertEquals(toFilter(or(not(in(C_BIGINT, ImmutableList.of(1L, 10L, 100L))), isNull(C_BIGINT))), TupleDomainFilter.BigintMultiRange.of(ImmutableList.of(
                TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, 0L, false),
                TupleDomainFilter.BigintRange.of(2L, 9L, false),
                TupleDomainFilter.BigintRange.of(11L, 99L, false),
                TupleDomainFilter.BigintRange.of(101, Long.MAX_VALUE, false)), true));
    }

    @Test
    public void testDecimal()
    {
        Long decimal = shortDecimal("-99.33");
        Expression decimalLiteral = toExpression(decimal, TYPES.get(C_DECIMAL_6_1));
        assertEquals(toFilter(equal(C_DECIMAL_6_1, decimalLiteral)), TupleDomainFilter.BigintRange.of(decimal, decimal, false));
    }

    @Test
    public void testVarchar()
    {
        assertEquals(toFilter(equal(C_VARCHAR, stringLiteral("abc", VARCHAR))), TupleDomainFilter.BytesRange.of(toBytes("abc"), false, toBytes("abc"), false, false));
        assertEquals(toFilter(not(equal(C_VARCHAR, stringLiteral("abc", VARCHAR)))), TupleDomainFilter.MultiRange.of(ImmutableList.of(
                TupleDomainFilter.BytesRange.of(null, true, toBytes("abc"), true, false),
                TupleDomainFilter.BytesRange.of(toBytes("abc"), true, null, true, false)), false));

        assertEquals(toFilter(lessThan(C_VARCHAR, stringLiteral("abc", VARCHAR))), TupleDomainFilter.BytesRange.of(null, true, toBytes("abc"), true, false));
        assertEquals(toFilter(lessThanOrEqual(C_VARCHAR, stringLiteral("abc", VARCHAR))), TupleDomainFilter.BytesRange.of(null, true, toBytes("abc"), false, false));

        assertEquals(toFilter(between(C_VARCHAR, stringLiteral("apple", VARCHAR), stringLiteral("banana", VARCHAR))), TupleDomainFilter.BytesRange.of(toBytes("apple"), false, toBytes("banana"), false, false));
        assertEquals(toFilter(or(isNull(C_VARCHAR), equal(C_VARCHAR, stringLiteral("abc", VARCHAR)))), TupleDomainFilter.BytesRange.of(toBytes("abc"), false, toBytes("abc"), false, true));
    }

    @Test
    public void testDate()
    {
        long days = TimeUnit.MILLISECONDS.toDays(DateTime.parse("2019-06-01").getMillis());
        assertEquals(toFilter(equal(C_DATE, dateLiteral("2019-06-01"))), TupleDomainFilter.BigintRange.of(days, days, false));
        assertEquals(toFilter(not(equal(C_DATE, dateLiteral("2019-06-01")))),
                TupleDomainFilter.BigintMultiRange.of(ImmutableList.of(
                        TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, days - 1, false),
                        TupleDomainFilter.BigintRange.of(days + 1, Long.MAX_VALUE, false)), false));

        assertEquals(toFilter(lessThan(C_DATE, dateLiteral("2019-06-01"))), TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, days - 1, false));
        assertEquals(toFilter(lessThanOrEqual(C_DATE, dateLiteral("2019-06-01"))), TupleDomainFilter.BigintRange.of(Long.MIN_VALUE, days, false));
        assertEquals(toFilter(not(lessThan(C_DATE, dateLiteral("2019-06-01")))), TupleDomainFilter.BigintRange.of(days, Long.MAX_VALUE, false));
        assertEquals(toFilter(not(lessThanOrEqual(C_DATE, dateLiteral("2019-06-01")))), TupleDomainFilter.BigintRange.of(days + 1, Long.MAX_VALUE, false));
    }

    private static byte[] toBytes(String value)
    {
        return Slices.utf8Slice(value).getBytes();
    }

    private TupleDomainFilter toFilter(Expression expression)
    {
        Optional<Map<Symbol, Domain>> domains = fromPredicate(expression).getTupleDomain().getDomains();
        assertTrue(domains.isPresent());
        Domain domain = Iterables.getOnlyElement(domains.get().values());
        return TupleDomainFilterUtils.toFilter(domain);
    }

    private ExpressionDomainTranslator.ExtractionResult fromPredicate(Expression originalPredicate)
    {
        return ExpressionDomainTranslator.fromPredicate(metadata, TEST_SESSION, originalPredicate, TYPES);
    }

    private static ComparisonExpression equal(Symbol symbol, Expression expression)
    {
        return equal(SymbolUtils.toSymbolReference(symbol), expression);
    }

    private static ComparisonExpression notEqual(Symbol symbol, Expression expression)
    {
        return notEqual(SymbolUtils.toSymbolReference(symbol), expression);
    }

    private static ComparisonExpression greaterThan(Symbol symbol, Expression expression)
    {
        return greaterThan(SymbolUtils.toSymbolReference(symbol), expression);
    }

    private static ComparisonExpression greaterThanOrEqual(Symbol symbol, Expression expression)
    {
        return greaterThanOrEqual(SymbolUtils.toSymbolReference(symbol), expression);
    }

    private static ComparisonExpression lessThan(Symbol symbol, Expression expression)
    {
        return lessThan(SymbolUtils.toSymbolReference(symbol), expression);
    }

    private static ComparisonExpression lessThanOrEqual(Symbol symbol, Expression expression)
    {
        return lessThanOrEqual(SymbolUtils.toSymbolReference(symbol), expression);
    }

    private static ComparisonExpression isDistinctFrom(Symbol symbol, Expression expression)
    {
        return isDistinctFrom(SymbolUtils.toSymbolReference(symbol), expression);
    }

    private static Expression isNotNull(Symbol symbol)
    {
        return isNotNull(SymbolUtils.toSymbolReference(symbol));
    }

    private static IsNullPredicate isNull(Symbol symbol)
    {
        return new IsNullPredicate(SymbolUtils.toSymbolReference(symbol));
    }

    private InPredicate in(Symbol symbol, List<?> values)
    {
        return in(SymbolUtils.toSymbolReference(symbol), TYPES.get(symbol), values);
    }

    private static BetweenPredicate between(Symbol symbol, Expression min, Expression max)
    {
        return new BetweenPredicate(SymbolUtils.toSymbolReference(symbol), min, max);
    }

    private static Expression isNotNull(Expression expression)
    {
        return new NotExpression(new IsNullPredicate(expression));
    }

    private InPredicate in(Expression expression, Type expressisonType, List<?> values)
    {
        List<Type> types = nCopies(values.size(), expressisonType);
        List<Expression> expressions = literalEncoder.toExpressions(values, types);
        return new InPredicate(expression, new InListExpression(expressions));
    }

    private static ComparisonExpression equal(Expression left, Expression right)
    {
        return comparison(EQUAL, left, right);
    }

    private static ComparisonExpression notEqual(Expression left, Expression right)
    {
        return comparison(NOT_EQUAL, left, right);
    }

    private static ComparisonExpression greaterThan(Expression left, Expression right)
    {
        return comparison(GREATER_THAN, left, right);
    }

    private static ComparisonExpression greaterThanOrEqual(Expression left, Expression right)
    {
        return comparison(GREATER_THAN_OR_EQUAL, left, right);
    }

    private static ComparisonExpression lessThan(Expression left, Expression expression)
    {
        return comparison(LESS_THAN, left, expression);
    }

    private static ComparisonExpression lessThanOrEqual(Expression left, Expression right)
    {
        return comparison(LESS_THAN_OR_EQUAL, left, right);
    }

    private static ComparisonExpression isDistinctFrom(Expression left, Expression right)
    {
        return comparison(IS_DISTINCT_FROM, left, right);
    }

    private static NotExpression not(Expression expression)
    {
        return new NotExpression(expression);
    }

    private static ComparisonExpression comparison(ComparisonExpression.Operator operator, Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(operator, expression1, expression2);
    }

    private static Literal bigintLiteral(long value)
    {
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return new GenericLiteral("BIGINT", Long.toString(value));
        }
        return new LongLiteral(Long.toString(value));
    }

    private static StringLiteral stringLiteral(String value)
    {
        return new StringLiteral(value);
    }

    private static Expression stringLiteral(String value, Type type)
    {
        return cast(stringLiteral(value), type);
    }

    private static Literal dateLiteral(String value)
    {
        return new GenericLiteral("DATE", value);
    }

    private static Expression cast(Expression expression, Type type)
    {
        return new Cast(expression, type.getTypeSignature().toString());
    }

    private static Long shortDecimal(String value)
    {
        return new BigDecimal(value).unscaledValue().longValueExact();
    }

    private Expression toExpression(Object object, Type type)
    {
        return literalEncoder.toExpression(object, type);
    }
}
