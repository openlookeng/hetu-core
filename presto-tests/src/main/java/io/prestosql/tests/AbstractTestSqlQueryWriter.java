/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.sql.SqlQueryWriter;
import io.prestosql.sql.builder.ExpressionFormatter;
import io.prestosql.sql.builder.SqlQueryBuilder;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ArrayConstructor;
import io.prestosql.sql.tree.AtTimeZone;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.CurrentPath;
import io.prestosql.sql.tree.CurrentTime;
import io.prestosql.sql.tree.CurrentUser;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FieldReference;
import io.prestosql.sql.tree.FrameBound;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.GroupingOperation;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.IntervalLiteral;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeLocation;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.NullIfExpression;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.Parameter;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SubqueryExpression;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;
import io.prestosql.sql.tree.TryExpression;
import io.prestosql.sql.tree.Window;
import io.prestosql.sql.tree.WindowFrame;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.tests.util.MockSqlQueryBuilder;
import io.prestosql.tests.util.PrePushDownPlanGenerator;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.regex.Pattern;

import static io.airlift.testing.Assertions.assertNotEquals;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.sql.QueryUtil.query;
import static io.prestosql.sql.QueryUtil.row;
import static io.prestosql.sql.QueryUtil.selectList;
import static io.prestosql.sql.QueryUtil.simpleQuery;
import static io.prestosql.sql.QueryUtil.table;
import static io.prestosql.sql.QueryUtil.values;
import static io.prestosql.sql.tree.ArithmeticUnaryExpression.negative;
import static io.prestosql.sql.tree.ArithmeticUnaryExpression.positive;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.prestosql.sql.tree.SortItem.NullOrdering.UNDEFINED;
import static io.prestosql.sql.tree.SortItem.Ordering.DESCENDING;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public abstract class AbstractTestSqlQueryWriter
{
    private static final Logger LOGGER = Logger.get(AbstractTestSqlQueryWriter.class);
    public static final String CONNECTOR_NAME = "tpch";
    public static final String SCHEMA_NAME = "tiny";
    private final SqlQueryWriter queryWriter;
    private LocalQueryRunner mockQueryRunner;
    private final String connectorName;
    private final String schemaName;

    protected AbstractTestSqlQueryWriter(SqlQueryWriter queryWriter)
    {
        this(queryWriter, CONNECTOR_NAME, SCHEMA_NAME);
    }

    protected AbstractTestSqlQueryWriter(SqlQueryWriter queryWriter, String connectorName, String schemaName)
    {
        this.queryWriter = queryWriter;
        this.connectorName = connectorName;
        this.schemaName = schemaName;
    }

    @BeforeClass
    public void setup()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(connectorName)
                .setSchema(schemaName)
                .setSystemProperty("task_concurrency", "1");

        this.mockQueryRunner = new PrePushDownPlanGenerator(sessionBuilder.build());
        getConnectorFactory().ifPresent(factory -> this.mockQueryRunner.createCatalog(this.connectorName, factory, ImmutableMap.of()));
    }

    @AfterClass
    public void clean()
    {
        // Do nothing
    }

    protected Optional<ConnectorFactory> getConnectorFactory()
    {
        return Optional.of(new TpchConnectorFactory(1));
    }

    @Test
    public void testQualifiedName()
    {
        LOGGER.info("Testing qualified name equals and hasCode implementation");
        io.prestosql.spi.sql.expression.QualifiedName x = new io.prestosql.spi.sql.expression.QualifiedName(list("a", "b", "c"));
        io.prestosql.spi.sql.expression.QualifiedName y = new io.prestosql.spi.sql.expression.QualifiedName(list("a", "b", "c"));
        io.prestosql.spi.sql.expression.QualifiedName z = new io.prestosql.spi.sql.expression.QualifiedName(list("a", "b"));
        assertEquals(x, y);
        assertEquals(x.hashCode(), y.hashCode());
        assertNotEquals(x, z);
    }

    @Test
    public void testIdentifierExpression()
    {
        LOGGER.info("Testing identifier expression");
        assertExpression(identifier("customer"), "customer");
        assertExpression(identifier("tpch.tiny.customer"), "\"tpch.tiny.customer\"");
        assertExpression(identifier("stats"), "stats");
        assertExpression(identifier("nfd"), "nfd");
        assertExpression(identifier("nfc"), "nfc");
        assertExpression(identifier("nfkd"), "nfkd");
        assertExpression(identifier("nfkc"), "nfkc");
    }

    @Test
    public void testSymbolReferenceExpression()
    {
        LOGGER.info("Testing symbol reference expression");
        assertExpression(new SymbolReference("customer"), "customer");
        assertExpression(new SymbolReference("tpch.tiny.customer"), "tpch.tiny.customer");
    }

    @Test
    public void testFieldReferenceExpression()
    {
        LOGGER.info("Testing field reference expression");
        assertExpression(new FieldReference(1), ":input(1)");
    }

    @Test
    public void testAllColumnsExpression()
    {
        LOGGER.info("Testing all columns expression");
        assertExpression(new AllColumns(), "*");
        assertExpression(new AllColumns(QualifiedName.of(ImmutableList.of(new Identifier("tpch"), new Identifier("tiny"), new Identifier("customer")))), "tpch.tiny.customer.*");
    }

    @Test
    public void testAtTimeZoneExpression()
    {
        LOGGER.info("Testing at timezone expression");
        assertExpression(new AtTimeZone(stringLiteral("2012-10-31 01:00 UTC"), stringLiteral("Asia/Shanghai")), "'2012-10-31 01:00 UTC' AT TIME ZONE 'Asia/Shanghai'");
    }

    @Test
    public void testBinaryLiteralExpression()
    {
        LOGGER.info("Testing binary literal expressions");
        assertExpression(new BinaryLiteral(""), "X''");
        assertExpression(new BinaryLiteral("abcdef1234567890ABCDEF"), "X'ABCDEF1234567890ABCDEF'");
    }

    @Test
    public void testDoubleLiteralExpression()
    {
        LOGGER.info("Testing Presto common literal expressions");
        assertExpression(doubleLiteral("123E7"), "1.23E9");
        assertExpression(doubleLiteral("123.456E7"), "1.23456E9");
        assertExpression(doubleLiteral(".4E42"), "4E41");
        assertExpression(doubleLiteral(".4E-42"), "4E-43");
    }

    @Test
    public void testDecimalLiteralExpression()
    {
        LOGGER.info("Testing Presto decimal literal expressions");
        assertExpression(new DecimalLiteral("12.34"), "DECIMAL '12.34'");
        assertExpression(new DecimalLiteral("12."), "DECIMAL '12.'");
        assertExpression(new DecimalLiteral("12"), "DECIMAL '12'");
        assertExpression(new DecimalLiteral(".34"), "DECIMAL '.34'");
        assertExpression(new DecimalLiteral("+12.34"), "DECIMAL '+12.34'");
        assertExpression(new DecimalLiteral("+12"), "DECIMAL '+12'");
        assertExpression(new DecimalLiteral("-12.34"), "DECIMAL '-12.34'");
        assertExpression(new DecimalLiteral("-12"), "DECIMAL '-12'");
        assertExpression(new DecimalLiteral("+.34"), "DECIMAL '+.34'");
        assertExpression(new DecimalLiteral("-.34"), "DECIMAL '-.34'");
    }

    @Test
    public void testUnicodeStringLiteralExpression()
    {
        LOGGER.info("Testing unicode string literal expressions");
        assertExpression(stringLiteral(""), "''");
        assertExpression(stringLiteral("hello\u6D4B\u8BD5\uDBFF\uDFFFworld\u7F16\u7801"), "U&'hello\\6D4B\\8BD5\\+10FFFFworld\\7F16\\7801'");
        assertExpression(stringLiteral("\u6D4B\u8BD5ABC\u6D4B\u8BD5"), "U&'\\6D4B\\8BD5ABC\\6D4B\\8BD5'");
        assertExpression(stringLiteral("\u6D4B\u8BD5ABC\u6D4B\u8BD5"), "U&'\\6D4B\\8BD5ABC\\6D4B\\8BD5'");
        assertExpression(stringLiteral("\u6D4B\u8BD5ABC\\"), "U&'\\6D4B\\8BD5ABC\\\\'");
        assertExpression(stringLiteral("\u6D4B\u8BD5ABC#\u8BD5"), "U&'\\6D4B\\8BD5ABC#\\8BD5'");
        assertExpression(stringLiteral("\u6D4B\u8BD5\'A\'B\'C#\'\'\u8BD5"), "U&'\\6D4B\\8BD5''A''B''C#''''\\8BD5'");
        assertExpression(stringLiteral("hello\u6D4B\u8BD5\uDBFF\uDFFFworld\u7F16\u7801"), "U&'hello\\6D4B\\8BD5\\+10FFFFworld\\7F16\\7801'");
        assertExpression(stringLiteral("\u6D4B\u8BD5ABC\u6D4B\u8BD5"), "U&'\\6D4B\\8BD5ABC\\6D4B\\8BD5'");
        assertExpression(stringLiteral("hello\\6d4B\\8BD5\\+10FFFFworld\\7F16\\7801"), "'hello\\6d4B\\8BD5\\+10FFFFworld\\7F16\\7801'");
    }

    @Test
    public void testIntervalLiteralExpression()
    {
        LOGGER.info("Testing interval literal expressions");
        assertExpression(new IntervalLiteral("123", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.YEAR), "INTERVAL '123' YEAR");
        assertExpression(new IntervalLiteral("123-3", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.YEAR, Optional.of(IntervalLiteral.IntervalField.MONTH)), "INTERVAL '123-3' YEAR TO MONTH");
        assertExpression(new IntervalLiteral("123", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.MONTH), "INTERVAL '123' MONTH");
        assertExpression(new IntervalLiteral("123", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.DAY), "INTERVAL '123' DAY");
        assertExpression(new IntervalLiteral("123 23:58:53.456", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.DAY, Optional.of(IntervalLiteral.IntervalField.SECOND)), "INTERVAL '123 23:58:53.456' DAY TO SECOND");
        assertExpression(new IntervalLiteral("123", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.HOUR), "INTERVAL '123' HOUR");
        assertExpression(new IntervalLiteral("23:59", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.HOUR, Optional.of(IntervalLiteral.IntervalField.MINUTE)), "INTERVAL '23:59' HOUR TO MINUTE");
        assertExpression(new IntervalLiteral("123", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.MINUTE), "INTERVAL '123' MINUTE");
        assertExpression(new IntervalLiteral("123", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.SECOND), "INTERVAL '123' SECOND");
    }

    @Test
    public void testMiscellaneousLiteralExpression()
    {
        LOGGER.info("Testing Presto miscellaneous literal expressions");
        assertExpression(new TimeLiteral("abc"), "TIME" + " 'abc'");
        assertExpression(new TimeLiteral("03:04:05"), "TIME '03:04:05'");
        assertExpression(new TimestampLiteral("abc"), "TIMESTAMP" + " 'abc'");
        assertExpression(new IntervalLiteral("33", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.DAY, Optional.empty()), "INTERVAL '33' DAY");
        assertExpression(new IntervalLiteral("33", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.DAY, Optional.of(IntervalLiteral.IntervalField.SECOND)), "INTERVAL '33' DAY TO SECOND");
        assertExpression(new CharLiteral("abc"), "CHAR 'abc'");
    }

    @Test
    public void testGenericLiteralExpression()
    {
        LOGGER.info("Testing Presto generic literal expressions");
        assertExpression(new GenericLiteral("VARCHAR", "abc"), "VARCHAR 'abc'");
        assertExpression(new GenericLiteral("BIGINT", "abc"), "BIGINT 'abc'");
        assertExpression(new GenericLiteral("DOUBLE", "abc"), "DOUBLE 'abc'");
        assertExpression(new GenericLiteral("BOOLEAN", "abc"), "BOOLEAN 'abc'");
        assertExpression(new GenericLiteral("DATE", "abc"), "DATE 'abc'");
        assertExpression(new GenericLiteral("foo", "abc"), "foo 'abc'");
    }

    @Test
    public void testCastExpression()
    {
        LOGGER.info("Testing cast expressions");
        assertCast("ARRAY(foo(42,55))");
        assertCast("varchar");
        assertCast("bigint");
        assertCast("BIGINT");
        assertCast("double");
        assertCast("DOUBLE");
        assertCast("boolean");
        assertCast("date");
        assertCast("time");
        assertCast("timestamp");
        assertCast("time with time zone");
        assertCast("timestamp with time zone");
        assertCast("ARRAY(BIGINT)");
        assertCast("array(array(bigint))");
        assertCast("array(array(bigint))");

        assertCast("ARRAY(ARRAY(ARRAY(boolean)))");
        assertCast("ARRAY(ARRAY(ARRAY(boolean)))");
        assertCast("ARRAY(ARRAY(ARRAY(boolean)))");

        assertCast("map(BIGINT,array(VARCHAR))");
        assertCast("map(BIGINT,array(VARCHAR))");

        assertCast("varchar(42)");
        assertCast("ARRAY(varchar(42))");
        assertCast("ARRAY(varchar(42))");

        assertCast("ROW(m DOUBLE)");
        assertCast("ROW(m DOUBLE)");
        assertCast("ROW(x BIGINT,y DOUBLE)");
        assertCast("ROW(x bigint,y double)");
        assertCast("ROW(x BIGINT,y DOUBLE,z ROW(m array(bigint),n map(double,timestamp)))");
        assertCast("ARRAY(ROW(x BIGINT,y TIMESTAMP))");

        assertCast("INTERVAL YEAR TO MONTH");
    }

    @Test
    public void testArithmeticUnaryExpression()
    {
        LOGGER.info("Testing unary expressions");
        assertExpression(longLiteral("9"), "9");
        assertExpression(positive(longLiteral("9")), "+9");
        assertExpression(positive(positive(longLiteral("9"))), "++9");
        assertExpression(positive(positive(positive(longLiteral("9")))), "+++9");
        assertExpression(negative(longLiteral("9")), "-9");
        assertExpression(negative(positive(longLiteral("9"))), "-+9");
        assertExpression(positive(negative(positive(longLiteral("9")))), "+-+9");
        assertExpression(negative(positive(negative(positive(longLiteral("9"))))), "-+-+9");
        assertExpression(positive(negative(positive(negative(positive(longLiteral("9")))))), "+-+-+9");
        assertExpression(negative(negative(negative(longLiteral("9")))), "- - -9");
        assertExpression(negative(negative(negative(longLiteral("9")))), "- - -9");
    }

    @Test
    public void testPredicateExpression()
    {
        LOGGER.info("Testing predicate expressions");
        List<Expression> literals = list(longLiteral("10"), longLiteral("20"), longLiteral("30"));
        assertExpression(new InPredicate(new SymbolReference("age"), array(literals)), "(age IN ARRAY[10,20,30])");
        assertExpression(new InListExpression(literals), "(10, 20, 30)");
        assertExpression(new IsNullPredicate(new SymbolReference("age")), "(age IS NULL)");
        assertExpression(new IsNotNullPredicate(new SymbolReference("age")), "(age IS NOT NULL)");
        assertExpression(new BetweenPredicate(longLiteral("1"), longLiteral("2"), longLiteral("3")), "(1 BETWEEN 2 AND 3)");
        assertExpression(new NotExpression(new BetweenPredicate(longLiteral("1"), longLiteral("2"), longLiteral("3"))), "(NOT (1 BETWEEN 2 AND 3))");
        assertExpression(new ExistsPredicate(new SubqueryExpression(simpleQuery(selectList(new LongLiteral("1"))))), "(EXISTS (SELECT 1\n" +
                "\n" +
                "))");
    }

    @Test
    public void testIfExpression()
    {
        LOGGER.info("Testing if and nullif expressions");
        assertExpression(new IfExpression(new BooleanLiteral("true"), longLiteral("1"), longLiteral("0")), "IF(true, 1, 0)");
        assertExpression(new IfExpression(new BooleanLiteral("true"), longLiteral("3"), new NullLiteral()), "IF(true, 3, null)");
        assertExpression(new IfExpression(new BooleanLiteral("false"), new NullLiteral(), longLiteral("4")), "IF(false, null, 4)");
        assertExpression(new IfExpression(new BooleanLiteral("false"), new NullLiteral(), new NullLiteral()), "IF(false, null, null)");
        assertExpression(new IfExpression(new BooleanLiteral("true"), longLiteral("3"), null), "IF(true, 3)");
        assertExpression(new NullIfExpression(longLiteral("42"), longLiteral("87")), "NULLIF(42, 87)");
        assertExpression(new NullIfExpression(longLiteral("42"), new NullLiteral()), "NULLIF(42, null)");
        assertExpression(new NullIfExpression(new NullLiteral(), new NullLiteral()), "NULLIF(null, null)");
    }

    @Test
    public void testArrayExpression()
    {
        LOGGER.info("Testing array expressions");
        assertExpression(array(list()), "ARRAY[]");
        assertExpression(array(list(longLiteral("1"), longLiteral("2"))), "ARRAY[1,2]");
        assertExpression(array(list(doubleLiteral("1.0"), doubleLiteral("2.5"))), "ARRAY[1E0,2.5E0]");
        assertExpression(array(list(stringLiteral("hi"))), "ARRAY['hi']");
        assertExpression(array(list(stringLiteral("hi"), stringLiteral("hello"))), "ARRAY['hi','hello']");
        assertExpression(new SubscriptExpression(
                array(list(longLiteral("1"), longLiteral("2"))),
                longLiteral("1")), "ARRAY[1,2][1]");
    }

    @Test
    public void testCoalesceExpression()
    {
        LOGGER.info("Testing coalesce expressions");
        assertExpression(new CoalesceExpression(new LongLiteral("13"), new LongLiteral("42")), "COALESCE(13, 42)");
        assertExpression(new CoalesceExpression(new LongLiteral("6"), new LongLiteral("7"), new LongLiteral("8")), "COALESCE(6, 7, 8)");
        assertExpression(new CoalesceExpression(new LongLiteral("13"), new NullLiteral()), "COALESCE(13, null)");
        assertExpression(new CoalesceExpression(new NullLiteral(), new LongLiteral("13")), "COALESCE(null, 13)");
        assertExpression(new CoalesceExpression(new NullLiteral(), new NullLiteral()), "COALESCE(null, null)");
    }

    @Test
    public void testFunctionCallAndTryExpression()
    {
        LOGGER.info("Testing function call and try expressions");
        List<Expression> literals = list(longLiteral("10"), longLiteral("20"), longLiteral("30"));
        FunctionCall functionCall = new FunctionCall(Optional.empty(),
                QualifiedName.of("test"),
                Optional.empty(),
                Optional.of(new InPredicate(new SymbolReference("age"), array(literals))),
                Optional.empty(),
                true, literals);
        TryExpression tryExpression = new TryExpression(functionCall);
        assertExpression(functionCall, "test(DISTINCT 10, 20, 30) FILTER (WHERE (age IN ARRAY[10,20,30]))");
        assertExpression(tryExpression, "TRY(test(DISTINCT 10, 20, 30) FILTER (WHERE (age IN ARRAY[10,20,30])))");
        assertExpression(new FunctionCall(QualifiedName.of("strpos"), list(stringLiteral("b"), stringLiteral("a"))), "strpos('b', 'a')");
    }

    @Test
    public void testLambdaExpression()
    {
        LOGGER.info("Testing lambda expressions");
        assertExpression(new LambdaExpression(
                list(),
                identifier("x")), "() -> x");
        assertExpression(new LambdaExpression(
                list(new LambdaArgumentDeclaration(identifier("x"))),
                new FunctionCall(QualifiedName.of("sin"), list(identifier("x")))), "(x) -> sin(x)");
        assertExpression(new LambdaExpression(
                list(new LambdaArgumentDeclaration(identifier("x")), new LambdaArgumentDeclaration(identifier("y"))),
                new FunctionCall(
                        QualifiedName.of("mod"),
                        list(identifier("x"), identifier("y")))), "(x, y) -> mod(x, y)");
    }

    @Test
    public void testQuantifiedComparisonExpression()
    {
        LOGGER.info("Testing comparison expressions");
        assertExpression(new QuantifiedComparisonExpression(
                        LESS_THAN,
                        QuantifiedComparisonExpression.Quantifier.ANY,
                        identifier("col1"),
                        new SubqueryExpression(simpleQuery(selectList(new SingleColumn(identifier("col2"))), table(QualifiedName.of("table1"))))),
                "(col1 < ANY (SELECT col2\n" +
                        "FROM\n" +
                        "  table1\n" +
                        "))");
        assertExpression(new QuantifiedComparisonExpression(
                        ComparisonExpression.Operator.EQUAL,
                        QuantifiedComparisonExpression.Quantifier.ALL,
                        identifier("col1"),
                        new SubqueryExpression(query(values(row(longLiteral("1")), row(longLiteral("2")))))),
                "(col1 = ALL ( VALUES \n" +
                        "  ROW (1)\n" +
                        ", ROW (2)\n" +
                        "))");
        assertExpression(new QuantifiedComparisonExpression(
                        ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                        QuantifiedComparisonExpression.Quantifier.SOME,
                        identifier("col1"),
                        new SubqueryExpression(simpleQuery(selectList(longLiteral("10"))))),
                "(col1 >= SOME (SELECT 10\n" +
                        "\n" +
                        "))");
    }

    @Test
    public void testAggregationWithOrderByExpression()
    {
        LOGGER.info("Testing aggregation with ORDER BY expressions");
        FunctionCall functionCall = new FunctionCall(Optional.empty(),
                QualifiedName.of("array_agg"),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new OrderBy(list(new SortItem(identifier("x"), DESCENDING, UNDEFINED)))),
                false, list(identifier("x")));
        assertExpression(functionCall, "array_agg(x  ORDER BY x DESC NULLS LAST)");
    }

    @Test
    public void testParameterExpression()
    {
        LOGGER.info("Testing parameter expressions");
        Optional<List<Expression>> params = Optional.of(list(new SymbolReference("tpch.tiny.customer"), longLiteral("1")));
        assertExpression(new Parameter(0), "tpch.tiny.customer", params);
        assertExpression(new Parameter(1), "1", params);
        assertExpression(new Parameter(2), "?");
    }

    @Test
    public void testPartitionExpression()
    {
        LOGGER.info("Testing partition expressions");
        assertExpression(new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.CURRENT_ROW),
                Optional.of(new FrameBound(FrameBound.Type.CURRENT_ROW))), "ROWS BETWEEN CURRENT ROW AND CURRENT ROW");

        assertExpression(new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.UNBOUNDED_FOLLOWING),
                Optional.of(new FrameBound(FrameBound.Type.CURRENT_ROW))), "ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW");

        assertExpression(new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.UNBOUNDED_PRECEDING),
                Optional.of(new FrameBound(FrameBound.Type.CURRENT_ROW))), "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    @Test
    public void testPrecedenceAndAssociativityExpression()
    {
        LOGGER.info("Testing precedence and associativity expressions");
        assertExpression(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR,
                new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND,
                        longLiteral("1"), longLiteral("2")), longLiteral("3")), "((1 AND 2) OR 3)");

        assertExpression(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR,
                longLiteral("1"), new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND,
                longLiteral("2"), longLiteral("3"))), "(1 OR (2 AND 3))");

        assertExpression(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND,
                new NotExpression(longLiteral("1")), longLiteral("2")), "((NOT 1) AND 2)");

        assertExpression(new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR,
                new NotExpression(longLiteral("1")), longLiteral("2")), "((NOT 1) OR 2)");

        assertExpression(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD,
                negative(longLiteral("1")), longLiteral("2")), "(-1 + 2)");

        assertExpression(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.SUBTRACT,
                new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.SUBTRACT,
                        longLiteral("1"), longLiteral("2")), longLiteral("3")), "((1 - 2) - 3)");

        assertExpression(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.DIVIDE,
                new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.DIVIDE,
                        longLiteral("1"), longLiteral("2")), longLiteral("3")), "((1 / 2) / 3)");

        assertExpression(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD,
                longLiteral("1"), new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY,
                longLiteral("2"), longLiteral("3"))), "(1 + (2 * 3))");
    }

    @Test
    public void testMiscellaneousExpression()
    {
        LOGGER.info("Testing Presto miscellaneous expressions");
        assertExpression(new GroupingOperation(Optional.empty(), ImmutableList.of(QualifiedName.of("a"), QualifiedName.of("b"))), "GROUPING (a, b)");
        assertExpression(new DereferenceExpression(new SymbolReference("b"), identifier("x")), "b.x");
        assertExpression(new Window(ImmutableList.of(new SymbolReference("a")), Optional.empty(), Optional.empty()), "(PARTITION BY a)");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testCurrentTimestampExpression()
    {
        LOGGER.info("Testing Presto current timestamp expression");
        assertExpression(new CurrentTime(CurrentTime.Function.TIME, 2), "current_time(2)");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testCurrentPathExpression()
    {
        LOGGER.info("Testing Presto current path expression");
        assertExpression(new CurrentPath(new NodeLocation(0, 0)), "CURRENT_PATH");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testCurrentUserExpression()
    {
        LOGGER.info("Testing Presto current user expression");
        assertExpression(new CurrentUser(new NodeLocation(0, 0)), "CURRENT_USER");
    }

    @Test
    public void testWindowFunction()
    {
    }

    @Test
    public void testGroupByWithComplexGroupingOperations()
    {
    }

    @Test
    public void testCurrentUserStatement()
    {
        LOGGER.info("Testing current user in a statement");
        @Language("SQL") String query = "SELECT current_user, name FROM customer";
        assertStatement(query, "SELECT", "user", "FROM", "customer");
    }

    @Test
    public void testIntermediateFunctions()
    {
        LOGGER.info("Testing Presto current time in a statement");
        // current_time is converted to $literal$time with time zone
        @Language("SQL") String query = "SELECT current_time FROM customer";
        assertStatement(query);

        // interval '29' day is converted to $literal$interval day to second
        // timestamp is converted to $literal$timestamp
        query = "SELECT * FROM orders WHERE orderdate - interval '29' day > timestamp '2012-10-31 01:00 UTC'";
        assertStatement(query);
    }

    @Test
    public void testSelectStatement()
    {
        LOGGER.info("Testing select statement");
        @Language("SQL") String query = "SELECT (totalprice + 2) AS new_price FROM orders";
        assertStatement(query, "SELECT", "totalprice", "+", "2E0", "FROM", "orders");
    }

    @Test
    public void testExtractStatement()
    {
        LOGGER.info("Testing extract statement");
        @Language("SQL") String query = "SELECT extract(YEAR FROM orderdate) AS year FROM orders LIMIT 10";
        assertStatement(query, "SELECT", "year", "orderdate", "FROM", "orders", "LIMIT 10");
    }

    @Test
    public void testLambdaStatement()
    {
        LOGGER.info("Testing lambda in a statement");
        @Language("SQL") String query = "SELECT filter(split(comment, ' '), x -> length(x) > 2) FROM customer LIMIT 10";
        assertStatement(query, "SELECT", "filter", "split", "comment", "' '", "expr", "->", "length", ">", "2", "LIMIT 10");
    }

    @Test
    public void testConditionalQueryStatement()
    {
        LOGGER.info("Testing conditional statement");
        @Language("SQL") String query = "SELECT regionkey, name, CASE WHEN regionkey = 0 THEN 'Africa' ELSE '' END FROM region";
        assertStatement(query, "SELECT", "CASE", "WHEN", "regionkey", "THEN", "Africa", "END", "FROM", "region");

        query = "SELECT regionkey, name, CASE regionkey WHEN 0 THEN 'Africa' ELSE '' END FROM region";
        assertStatement(query, "SELECT", "CASE", "regionkey", "WHEN", "THEN", "Africa", "END", "FROM", "region");
    }

    @Test
    public void testPartitionStatement()
    {
        LOGGER.info("Testing partition statement");
        // Window is not supported
        @Language("SQL") String query = "SELECT regionkey, name, rank() OVER (PARTITION BY name ORDER BY name DESC) AS rnk FROM region ORDER BY name";
        assertStatement(query, "SELECT", "FROM", "rank", "(", ")", "OVER", "partition by name  order by name desc nulls last", "AS rank", "region", "ORDER BY name ASC NULLS LAST");
    }

    @Test
    public void testGroupingSetsStatement()
    {
        LOGGER.info("Testing grouping sets statement");
        // DistinctLimitNode not supported
        @Language("SQL") String query = "SELECT orderkey, orderstatus, orderpriority FROM orders GROUP BY grouping sets (orderkey, (orderstatus, orderpriority)) LIMIT 10";
        assertStatement(query);
    }

    @Test
    public void testJoinStatements()
    {
        LOGGER.info("Testing join statements");
        @Language("SQL") String query = "SELECT c.name FROM customer c LEFT JOIN orders o ON c.custkey=o.custkey";
        assertStatement(query, "SELECT", "FROM", "customer", "LEFT JOIN", "orders", "ON", "table0.custkey = table1.custkey_0");

        query = "SELECT c.name FROM customer c RIGHT JOIN orders o ON c.custkey=o.custkey";
        assertStatement(query, "SELECT", "FROM", "customer", "RIGHT JOIN", "orders", "ON", "table0.custkey = table1.custkey_0");

        query = "SELECT c.name FROM customer c JOIN orders o ON c.custkey=o.custkey";
        assertStatement(query, "SELECT", "FROM", "customer", "INNER JOIN", "orders", "ON", "table0.custkey = table1.custkey_0");

        query = "SELECT c.name FROM customer c FULL JOIN orders o ON c.custkey=o.custkey";
        assertStatement(query, "SELECT", "FROM", "customer", "FULL JOIN", "orders", "ON", "table0.custkey = table1.custkey_0");

        query = "SELECT c.name FROM customer c JOIN orders o USING (custkey)";
        assertStatement(query, "SELECT", "FROM", "customer", "INNER JOIN", "orders", "ON", "table0.custkey = table1.custkey_0");

        query = "SELECT c.name FROM customer c CROSS JOIN orders LIMIT 10";
        assertStatement(query, "SELECT", "FROM", "customer", "CROSS JOIN", "orders", "LIMIT 10");

        query = "SELECT c.name FROM customer c LEFT JOIN orders o ON c.custkey=o.custkey WHERE o.totalprice > 10";
        assertStatement(query, "SELECT", "FROM", "customer", "INNER JOIN", "orders", "WHERE", ">", "1E1");

        query = "SELECT c.name FROM customer c RIGHT JOIN orders o ON c.custkey=o.custkey WHERE o.totalprice > 10 AND o.orderstatus='F'";
        assertStatement(query, "SELECT", "FROM", "customer", "RIGHT JOIN", "orders", "WHERE", ">", "1E1", "AND", "'f'");

        query = "SELECT c.name FROM customer c RIGHT JOIN orders o ON c.custkey=o.custkey WHERE o.totalprice > 10 AND o.orderstatus='F' ORDER BY cast(c.name AS VARCHAR) LIMIT 10";
        assertStatement(query, "SELECT", "FROM", "customer", "RIGHT JOIN", "orders", "WHERE", ">", "1E1", "AND", "'f'", "ORDER BY", "LIMIT 10");

        query = "SELECT * " +
                "FROM (SELECT max(totalprice) AS price, o.orderkey AS orderkey " +
                "FROM customer c JOIN orders o ON c.custkey=o.custkey GROUP BY orderpriority, orderkey LIMIT 10) t1 LEFT JOIN lineitem l ON t1.orderkey=l.orderkey";
        assertStatement(query, "SELECT", "FROM", "customer", "INNER JOIN", "orders", "GROUP BY", "LIMIT 10", "LEFT JOIN", "lineitem");

        query = "SELECT c.name FROM customer c RIGHT JOIN orders o ON c.custkey=o.custkey WHERE o.totalprice > 10 AND o.orderstatus='F'";
        assertStatement(query, "SELECT", "FROM", "customer", "RIGHT JOIN", "orders", "WHERE", ">", "1E1", "AND", "'f'");

        query = "SELECT t1.custkey1, t2.custkey, t2.name FROM " +
                "   (SELECT c.custkey AS custkey1, o.custkey AS custkey2 FROM " +
                "       customer c INNER JOIN orders o ON c.custkey = o.custkey) t1 " +
                "           LEFT JOIN customer t2 ON t1.custkey1=t2.custkey LIMIT 10";
        assertStatement(query, "SELECT", "FROM", "customer", "INNER JOIN", "orders", "LEFT JOIN", "customer");

        query = "SELECT * FROM orders o LEFT JOIN lineitem l USING (orderkey) LEFT JOIN " +
                "   customer c using (custkey) LEFT JOIN supplier s USING (nationkey) LEFT JOIN " +
                "       partsupp ps ON ps.suppkey=s.suppkey JOIN part pt ON pt.partkey=ps.partkey LIMIT 20";
        assertStatement(query, "SELECT", "FROM", "orders", "LEFT JOIN", "lineitem", "LEFT JOIN", "customer", "LEFT JOIN", "supplier", "LEFT JOIN", "partsupp", "INNER JOIN", "part", "LIMIT 20");

        query = "SELECT c_count, count(*) AS custdist FROM " +
                "   (SELECT c.custkey, count(o.orderkey) FROM " +
                "       customer c LEFT OUTER JOIN orders o ON c.custkey = o.custkey AND o.comment NOT LIKE '%[WORD1]%[WORD2]%' GROUP BY c.custkey) AS c_orders(c_custkey, c_count) " +
                "           GROUP BY c_count ORDER BY custdist DESC, c_count DESC";
        assertStatement(query, "SELECT", "FROM", "customer", "LEFT JOIN", "orders", "NOT", "LIKE", "%", "WORD1", "%", "WORD2", "GROUP BY", "ORDER BY", "DESC");
    }

    @Test
    public void testAggregationStatements()
    {
        LOGGER.info("Testing aggregation statements");
        @Language("SQL") String query = "SELECT * FROM " +
                "   (SELECT max(totalprice) AS price, o.orderkey AS orderkey FROM " +
                "       customer c JOIN orders o ON c.custkey=o.custkey GROUP BY orderpriority, orderkey) t1 " +
                "   LEFT JOIN lineitem l ON substr(cast(t1.orderkey AS VARCHAR), 0, 2)=cast(t1.orderkey AS VARCHAR) LIMIT 20";
        assertStatement(query, "SELECT", "FROM", "customer", "INNER JOIN", "orders", "GROUP BY", "LEFT JOIN", "lineitem", "LIMIT 20");

        query = "SELECT * FROM " +
                "   (SELECT max(totalprice) AS price, o.orderkey AS orderkey FROM " +
                "       customer c join orders o ON c.custkey=o.custkey GROUP BY orderpriority, orderkey HAVING orderkey>100) t1 " +
                "   LEFT JOIN lineitem l ON substr(cast(t1.orderkey  AS VARCHAR), 0, 2)=cast(t1.orderkey  AS VARCHAR) LIMIT 10";
        assertStatement(query, "SELECT", "FROM", "customer", "INNER JOIN", "orders", "WHERE", ">", "100", "GROUP BY", "LEFT JOIN", "lineitem", "LIMIT 10");
    }

    @Test
    public void testUnionStatement()
    {
        LOGGER.info("Testing union statements");
        @Language("SQL") String queryUnionDefault = "SELECT nationkey FROM nation UNION SELECT regionkey FROM nation";
        assertStatement(queryUnionDefault, "SELECT", "FROM", "nationkey", "nation", "UNION", "ALL", "GROUP BY");

        @Language("SQL") String queryUnionAll = "SELECT nationkey FROM nation UNION ALL SELECT regionkey FROM nation";
        assertStatement(queryUnionAll, "SELECT", "FROM", "nationkey", "nation", "UNION", "ALL");

        @Language("SQL") String queryUnionDistinct = "SELECT nationkey FROM nation UNION DISTINCT SELECT regionkey FROM nation";
        assertStatement(queryUnionDistinct, "SELECT", "FROM", "nationkey", "nation", "UNION", "ALL", "GROUP BY");
    }

    @Test
    public void testIntersectStatement()
    {
    }

    @Test
    public void testExceptStatement()
    {
    }

    @Test
    public void testTpchSql1()
    {
        LOGGER.info("Testing TPCH Sql 1");
        @Language("SQL") String query = "SELECT returnflag, linestatus, sum(quantity) AS sum_qty, sum(extendedprice) AS sum_base_price, sum(extendedprice * (1 - discount)) AS sum_disc_price, sum(extendedprice * (1 - discount) * (1 + tax)) AS sum_charge, avg(quantity) AS avg_qty, avg(extendedprice) AS avg_price, avg(discount) AS avg_disc, count(*) AS count_order FROM lineitem WHERE shipdate <= date '1998-09-16' GROUP BY returnflag, linestatus ORDER BY returnflag, linestatus";
        assertStatement(query, "sum", "sum", "sum", "sum", "avg", "avg", "avg", "count", "(", "*", ")", "WHERE", "\\<=", "date", "GROUP BY", "ORDER BY");
    }

    @Test
    public void testTpchSql3()
    {
        LOGGER.info("Testing TPCH Sql 3");
        @Language("SQL") String query = "SELECT l.orderkey, sum(l.extendedprice * (1 - l.discount)) AS revenue, o.orderdate, o.shippriority FROM customer c, orders o, lineitem l WHERE c.mktsegment = 'BUILDING' and c.custkey = o.custkey and l.orderkey = o.orderkey and o.orderdate < date '1995-03-22' and l.shipdate > date '1995-03-22' GROUP BY l.orderkey, o.orderdate, o.shippriority ORDER BY revenue desc, o.orderdate LIMIT 10";
        assertStatement(query, "sum", "FROM", "customer", "INNER JOIN", "orders", "INNER JOIN", "lineitem", "GROUP BY", "ORDER BY", "desc");
    }

    @Test
    public void testTpchSql4()
    {
        LOGGER.info("Testing TPCH Sql 4");
        @Language("SQL") String query = "SELECT o.orderpriority, count(*) AS order_count FROM orders o WHERE o.orderdate >= date '1996-05-01' and o.orderdate < date '1996-08-01' and exists ( SELECT * FROM lineitem l WHERE l.orderkey = o.orderkey and l.commitdate < l.receiptdate ) GROUP BY o.orderpriority ORDER BY o.orderpriority";
        assertStatement(query, "count", "FROM", "orders", "INNER JOIN", "lineitem", "GROUP BY", "ORDER BY", "asc");
    }

    @Test
    public void testTpchSql5()
    {
        LOGGER.info("Testing TPCH Sql 5");
        @Language("SQL") String query = "SELECT n.name, sum(l.extendedprice * (1 - l.discount)) AS revenue FROM customer c, orders o, lineitem l, supplier s, nation n, region r WHERE c.custkey = o.custkey and l.orderkey = o.orderkey and l.suppkey = s.suppkey and c.nationkey = s.nationkey and s.nationkey = n.nationkey and n.regionkey = r.regionkey and r.name = 'AFRICA' and o.orderdate >= date '1993-01-01' and o.orderdate < date '1994-01-01' GROUP BY n.name ORDER BY revenue desc";
        assertStatement(query, "sum", "FROM", "customer", "INNER JOIN", "orders", "INNER JOIN", "lineitem", "INNER JOIN", "supplier", "INNER JOIN", "nation", "INNER JOIN", "region", "GROUP BY", "ORDER BY", "desc");
    }

    @Test
    public void testTpchSql6()
    {
        LOGGER.info("Testing TPCH Sql 6");
        @Language("SQL") String query = "SELECT sum(extendedprice * discount) AS revenue FROM lineitem WHERE shipdate >= date '1993-01-01' and shipdate < date '1994-01-01' and discount between 0.06 - 0.01 and 0.06 + 0.01 and quantity < 25";
        assertStatement(query, "sum", "FROM", "lineitem", "WHERE", "date", "date", "between");
    }

    protected static LongLiteral longLiteral(String val)
    {
        return new LongLiteral(val);
    }

    protected static DoubleLiteral doubleLiteral(String val)
    {
        return new DoubleLiteral(val);
    }

    protected static StringLiteral stringLiteral(String val)
    {
        return new StringLiteral(val);
    }

    protected static ArrayConstructor array(List<Expression> expressions)
    {
        return new ArrayConstructor(expressions);
    }

    protected static <T> List<T> list(T... values)
    {
        return Arrays.asList(values);
    }

    protected void assertExpression(Node expression, String expected)
    {
        assertExpression(expression, expected, Optional.empty());
    }

    protected void assertExpression(Node expression, String expected, Optional<List<Expression>> params)
    {
        String actual = ExpressionFormatter.formatExpression(this.queryWriter, expression, params);
        assertEquals(actual, expected, "failed to rewrite expression");
    }

    protected void assertCast(String type)
    {
        type = type.toLowerCase(Locale.ENGLISH);
        assertExpression(new Cast(new NullLiteral(), type), "CAST(null AS " + type + ")");
    }

    protected void assertStatement(@Language("SQL") String query, String... keywords)
    {
        LOGGER.info("Testing " + query);

        // Build the logical plan
        mockQueryRunner.inTransaction(transaction -> {
            Plan plan = mockQueryRunner.createPlan(transaction, query, WarningCollector.NOOP);
            OutputNode outputNode = (OutputNode) plan.getRoot();

            // Build the sub-query
            MockSqlQueryBuilder sqlQueryBuilder = new MockSqlQueryBuilder(mockQueryRunner.getMetadata(), transaction);
            Optional<SqlQueryBuilder.Result> result = sqlQueryBuilder.build(outputNode.getSource());

            int noOfVisits = sqlQueryBuilder.getVisits();

            if (result.isPresent()) {
                if (keywords.length == 0) {
                    fail("Query is rewritten to " + result.get().getQuery() + " but expected not to rewrite");
                }

                // Validate cache
                sqlQueryBuilder.build(outputNode.getSource().getSources().get(0));

                assertEquals(sqlQueryBuilder.getVisits(), noOfVisits, "SqlQueryBuilder does not cache the result");

                String sql = result.get().getQuery();

                LOGGER.info("Rewritten to: " + sql);

                if (!pattern(keywords).matcher(sql.toLowerCase(Locale.ENGLISH)).find()) {
                    fail("Rewritten query does not match the keywords in order: " + Arrays.toString(keywords));
                }

                compare(query, sql);
            }
            else {
                if (keywords.length != 0) {
                    fail("Failed to rewrite the query " + query);
                }
            }
            return null;
        });
    }

    protected void compare(@Language("SQL") String original, @Language("SQL") String rewritten)
    {
    }

    private static Pattern pattern(String... keywords)
    {
        StringJoiner joiner = new StringJoiner(".*");
        for (String keyword : keywords) {
            switch (keyword) {
                case "*":
                    joiner.add("\\*");
                    break;
                case "+":
                    joiner.add("\\+");
                    break;
                case ".":
                    joiner.add("\\.");
                    break;
                case "(":
                    joiner.add("\\(");
                    break;
                case ")":
                    joiner.add("\\)");
                    break;
                default:
                    joiner.add(keyword.toLowerCase(Locale.ENGLISH));
            }
        }
        return Pattern.compile(joiner.toString());
    }
}
