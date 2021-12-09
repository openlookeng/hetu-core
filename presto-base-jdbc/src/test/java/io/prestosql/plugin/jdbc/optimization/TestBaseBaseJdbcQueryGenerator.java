/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.plugin.jdbc.optimization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule.FULL_PUSHDOWN;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.relational.Expressions.call;
import static org.testng.Assert.assertEquals;

public class TestBaseBaseJdbcQueryGenerator
        extends TestBaseJdbcPushDownBase
{
    private static final SessionHolder defaultSessionHolder = new SessionHolder();
    private static final JdbcTableHandle jdbcTable = testTable;

    private void testJQL(
            Function<PlanBuilder, PlanNode> planBuilderConsumer,
            String expectedJQL)
    {
        PlanNode planNode = planBuilderConsumer.apply(createPlanBuilder());
        testJQL(planNode, expectedJQL);
    }

    private void testJQL(
            PlanNode planNode,
            String expectedJQL)
    {
        TesterParameter testerParameter = TesterParameter.getTesterParameter();
        JdbcPushDownParameter pushDownParameter = new JdbcPushDownParameter("'", false, FULL_PUSHDOWN, testerParameter.getFunctionResolution());
        JdbcQueryGeneratorResult jdbcQueryGeneratorResult = (new BaseJdbcQueryGenerator(pushDownParameter, new BaseJdbcRowExpressionConverter(testerParameter.getMetadata().getFunctionAndTypeManager(), testerParameter.getFunctionResolution(), testerParameter.getRowExpressionService(), testerParameter.getDeterminismEvaluator()), new BaseJdbcSqlStatementWriter(pushDownParameter))).generate(planNode, new TestTypeManager()).get();
        String generatedJQL = jdbcQueryGeneratorResult.getGeneratedSql().getSql();
        assertEquals(generatedJQL, expectedJQL);
    }

    private PlanNode buildPlan(Function<PlanBuilder, PlanNode> consumer)
    {
        PlanBuilder planBuilder = createPlanBuilder();
        return consumer.apply(planBuilder);
    }

    @Test
    public void testSimpleSelectStar()
    {
        testJQL(planBuilder -> tableScan(planBuilder, jdbcTable, regionId, city, fare, amount),
                "SELECT regionid, city, fare, amount FROM 'table'");
        testJQL(planBuilder -> limit(planBuilder, 10L, tableScan(planBuilder, jdbcTable, regionId, city, fare, amount)),
                "SELECT regionid, city, fare, amount FROM 'table' LIMIT 10");
    }

    @Test
    public void testSimpleOperatorFilter()
    {
        testJQL(planBuilder -> filter(
                planBuilder,
                tableScan(planBuilder, jdbcTable, regionId, city, fare, amount),
                getRowExpression("amount = 20", defaultSessionHolder)),
                "SELECT regionid, city, fare, amount FROM (SELECT regionid, city, fare, amount FROM 'table') hetu_table_1 WHERE (amount = 20)");
    }

    @Test
    public void testFilterWithLike()
    {
        testJQL(planBuilder -> filter(
                planBuilder,
                tableScan(planBuilder, jdbcTable, regionId, city, fare, amount),
                getRowExpression("city like 'city'", defaultSessionHolder)),
                "SELECT regionid, city, fare, amount FROM (SELECT regionid, city, fare, amount FROM 'table') hetu_table_1 WHERE (city LIKE 'city')");

        testJQL(planBuilder -> filter(
                planBuilder,
                tableScan(planBuilder, jdbcTable, regionId, city, fare, amount),
                getRowExpression("city not like 'city'", defaultSessionHolder)),
                "SELECT regionid, city, fare, amount FROM (SELECT regionid, city, fare, amount FROM 'table') hetu_table_1 WHERE (NOT (city LIKE 'city'))");
    }

    @Test
    public void testFilterWithIsNull()
    {
        testJQL(planBuilder -> filter(
                planBuilder,
                tableScan(planBuilder, jdbcTable, regionId, city, fare, amount),
                getRowExpression("amount is null", defaultSessionHolder)),
                "SELECT regionid, city, fare, amount FROM (SELECT regionid, city, fare, amount FROM 'table') hetu_table_1 WHERE (amount IS NULL)");

        testJQL(planBuilder -> filter(
                planBuilder,
                tableScan(planBuilder, jdbcTable, regionId, city, fare, amount),
                getRowExpression("amount is not null", defaultSessionHolder)),
                "SELECT regionid, city, fare, amount FROM (SELECT regionid, city, fare, amount FROM 'table') hetu_table_1 WHERE (NOT (amount IS NULL))");
    }

    @Test
    public void testFilterWithIn()
    {
        testJQL(planBuilder -> filter(
                planBuilder,
                tableScan(planBuilder, jdbcTable, regionId, city, fare, amount),
                getRowExpression("amount in (100, 200)", defaultSessionHolder)),
                "SELECT regionid, city, fare, amount FROM (SELECT regionid, city, fare, amount FROM 'table') hetu_table_1 WHERE (amount IN (100, 200))");

        testJQL(planBuilder -> filter(
                planBuilder,
                tableScan(planBuilder, jdbcTable, regionId, city, fare, amount),
                getRowExpression("amount not in (100, 200)", defaultSessionHolder)),
                "SELECT regionid, city, fare, amount FROM (SELECT regionid, city, fare, amount FROM 'table') hetu_table_1 WHERE (NOT (amount IN (100, 200)))");
    }

    @Test
    public void testSimpleSelectWithFilterLimit()
    {
        testJQL(planBuilder -> limit(
                planBuilder,
                30L,
                project(
                        planBuilder,
                        filter(
                                planBuilder,
                                tableScan(planBuilder, jdbcTable, regionId, city, fare, amount),
                                getRowExpression("amount > 20", defaultSessionHolder)),
                        ImmutableList.of("city", "fare"))),
                "SELECT city, fare FROM (SELECT regionid, city, fare, amount FROM (SELECT regionid, city, fare, amount FROM 'table') hetu_table_1 WHERE (amount > 20)) hetu_table_2 LIMIT 30");
    }

    @Test
    public void testCountStar()
    {
        BiConsumer<PlanBuilder, PlanBuilder.AggregationBuilder> aggregationFunctionBuilder = ((planBuilder, aggregationBuilder) ->
                aggregationBuilder.addAggregation(planBuilder.symbol("agg"), getRowExpression("count(*)", defaultSessionHolder)));
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, jdbcTable, regionId, city, fare, amount));
        testJQL(planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(justScan).globalGrouping())),
                "SELECT CAST(count(*) AS bigint) AS agg FROM (SELECT regionid, city, fare, amount FROM 'table') hetu_table_1");
    }

    @Test
    public void testDistinctSelection()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, jdbcTable, regionId, city, fare, amount));
        testJQL(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(symbol("regionid"))),
                "SELECT regionid FROM (SELECT regionid, city, fare, amount FROM 'table') hetu_table_1 GROUP BY regionid");
    }

    @Test
    public void testSimpleJoin()
    {
        testJQL(planBuilder -> planBuilder.join(
                JoinNode.Type.INNER,
                tableScan(planBuilder, testLeftTable, leftId, leftValue),
                tableScan(planBuilder, testRightTable, rightId, rightValue),
                new JoinNode.EquiJoinClause(symbol("leftid"),
                        symbol("rightid"))),
                "SELECT leftid, leftvalue, rightid, rightvalue FROM ((SELECT leftid, leftvalue FROM 'left_table') hetu_left_1 INNER JOIN (SELECT rightid, rightvalue FROM 'right_table') hetu_right_1 ON leftid = rightid)");
        testJQL(planBuilder -> planBuilder.join(
                JoinNode.Type.LEFT,
                tableScan(planBuilder, testLeftTable, leftId, leftValue),
                tableScan(planBuilder, testRightTable, rightId, rightValue),
                new JoinNode.EquiJoinClause(symbol("leftid"), symbol("rightid"))),
                "SELECT leftid, leftvalue, rightid, rightvalue FROM ((SELECT leftid, leftvalue FROM 'left_table') hetu_left_1 LEFT JOIN (SELECT rightid, rightvalue FROM 'right_table') hetu_right_1 ON leftid = rightid)");
        testJQL(planBuilder -> planBuilder.join(
                JoinNode.Type.RIGHT,
                tableScan(planBuilder, testLeftTable, leftId, leftValue),
                tableScan(planBuilder, testRightTable, rightId, rightValue),
                new JoinNode.EquiJoinClause(symbol("leftid"), symbol("rightid"))),
                "SELECT leftid, leftvalue, rightid, rightvalue FROM ((SELECT leftid, leftvalue FROM 'left_table') hetu_left_1 RIGHT JOIN (SELECT rightid, rightvalue FROM 'right_table') hetu_right_1 ON leftid = rightid)");
        testJQL(planBuilder -> planBuilder.join(
                JoinNode.Type.FULL,
                tableScan(planBuilder, testLeftTable, leftId, leftValue),
                tableScan(planBuilder, testRightTable, rightId, rightValue),
                new JoinNode.EquiJoinClause(symbol("leftid"), symbol("rightid"))),
                "SELECT leftid, leftvalue, rightid, rightvalue FROM ((SELECT leftid, leftvalue FROM 'left_table') hetu_left_1 FULL JOIN (SELECT rightid, rightvalue FROM 'right_table') hetu_right_1 ON leftid = rightid)");
    }

    @Test
    public void testMultiLayeredJoin()
    {
        testJQL(planBuilder -> planBuilder.join(
                JoinNode.Type.INNER,
                planBuilder.join(
                        JoinNode.Type.INNER,
                        tableScan(planBuilder, testLeftTable, leftId, leftValue),
                        tableScan(planBuilder, testRightTable, rightId, rightValue),
                        new JoinNode.EquiJoinClause(symbol("leftid"), symbol("rightid"))),
                tableScan(planBuilder, jdbcTable, regionId), new JoinNode.EquiJoinClause(symbol("leftid"), symbol("regoinid"))),
                "SELECT leftid, leftvalue, rightid, rightvalue, regionid FROM ((SELECT leftid, leftvalue, rightid, rightvalue FROM ((SELECT leftid, leftvalue FROM 'left_table') hetu_left_1 INNER JOIN (SELECT rightid, rightvalue FROM 'right_table') hetu_right_1 ON leftid = rightid)) hetu_left_2 INNER JOIN (SELECT regionid FROM 'table') hetu_right_2 ON leftid = regoinid)");
    }

    @Test
    public void testJoinWithFilter()
    {
        testJQL(planBuilder -> planBuilder.join(
                JoinNode.Type.INNER,
                tableScan(planBuilder, testLeftTable, leftId, leftValue),
                tableScan(planBuilder, testRightTable, rightId, rightValue),
                getRowExpression("leftvalue > rightvalue", defaultSessionHolder),
                new JoinNode.EquiJoinClause(symbol("leftid"), symbol("rightid"))),
                "SELECT leftid, leftvalue, rightid, rightvalue FROM ((SELECT leftid, leftvalue FROM 'left_table') hetu_left_1 INNER JOIN (SELECT rightid, rightvalue FROM 'right_table') hetu_right_1 ON leftid = rightid AND (leftvalue > rightvalue))");
    }

    @Test
    public void testTopN()
    {
        testJQL(planBuilder -> planBuilder.topN(
                10L,
                ImmutableList.of(symbol("regionid")),
                tableScan(planBuilder, jdbcTable, regionId, city, fare, amount)),
                "SELECT regionid, city, fare, amount FROM (SELECT regionid, city, fare, amount FROM 'table') hetu_table_1 ORDER BY regionid ASC NULLS FIRST LIMIT 10");
    }

    @Test
    public void testUnion()
    {
        PlanNode leftScanNode = buildPlan(planBuilder -> tableScan(planBuilder, testLeftTable, leftId, leftValue));
        PlanNode rightScanNode = buildPlan(planBuilder -> tableScan(planBuilder, testRightTable, rightId, rightValue));
        testJQL(planBuilder -> planBuilder.union(
                ImmutableListMultimap.<Symbol, Symbol>builder()
                        .put(symbol("id"), symbol("leftid"))
                        .put(symbol("value"), symbol("leftvalue"))
                        .put(symbol("id"), symbol("rightid"))
                        .put(symbol("value"), symbol("rightvalue"))
                        .build(),
                ImmutableList.of(leftScanNode, rightScanNode)
        ), "SELECT id, value FROM ((SELECT leftid AS id, leftvalue AS value FROM (SELECT leftid, leftvalue FROM 'left_table') hetu_table_1) UNION ALL (SELECT rightid AS id, rightvalue AS value FROM (SELECT rightid, rightvalue FROM 'right_table') hetu_table_2)) hetu_table_3");
    }

    @Test
    public void testWindowFunctionWithOrderBy()
    {
        PlanNode scanNode = buildPlan(planBuilder -> tableScan(planBuilder, jdbcTable, regionId, city, fare, amount));
        testJQL(planBuilder -> planBuilder.window(
                new WindowNode.Specification(
                        ImmutableList.of(),
                        Optional.of(new OrderingScheme(
                                ImmutableList.of(symbol("fare")),
                                ImmutableMap.of(symbol("fare"), SortOrder.ASC_NULLS_FIRST)))),
                ImmutableMap.of(
                        symbol("amount_out"),
                        new WindowNode.Function(
                                call("min",
                                        metadata.getFunctionAndTypeManager().lookupFunction("min", fromTypes(BIGINT)),
                                        BIGINT,
                                        ImmutableList.of(new VariableReferenceExpression("amount", types.get(symbol("amount"))))),
                                ImmutableList.of(new VariableReferenceExpression("amount", types.get(symbol("amount")))),
                                new WindowNode.Frame(
                                        Types.WindowFrameType.RANGE,
                                        Types.FrameBoundType.UNBOUNDED_PRECEDING,
                                        Optional.empty(),
                                        Types.FrameBoundType.CURRENT_ROW,
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))),
                symbol("city"),
                scanNode),
                "SELECT regionid, city, fare, amount, min(amount) OVER ( ORDER BY fare ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS amount_out FROM (SELECT regionid, city, fare, amount FROM 'table') hetu_table_1");
    }

    @Test
    public void testWindowFunctionWithProjectAndRange()
    {
        PlanNode scanNode = buildPlan(planBuilder -> tableScan(planBuilder, jdbcTable, regionId, city, fare, amount, startValueColumn, endValueColumn));
        testJQL(planBuilder -> planBuilder.project(
                Assignments.builder()
                        .put(symbol("regionid"), variable("regionid"))
                        .put(symbol("city"), variable("city"))
                        .put(symbol("amount_out"), variable("amount_out", BIGINT))
                        .build(),
                planBuilder.window(
                        new WindowNode.Specification(
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableMap.of(
                                symbol("amount_out"),
                                new WindowNode.Function(
                                        call("min",
                                                metadata.getFunctionAndTypeManager().lookupFunction("min", fromTypes(BIGINT)),
                                                BIGINT,
                                                ImmutableList.of(new VariableReferenceExpression("amount", types.get(symbol("amount"))))),
                                        ImmutableList.of(new VariableReferenceExpression("amount", types.get(symbol("amount")))),
                                        new WindowNode.Frame(
                                                Types.WindowFrameType.RANGE,
                                                Types.FrameBoundType.PRECEDING,
                                                Optional.of(startValue),
                                                Types.FrameBoundType.FOLLOWING,
                                                Optional.of(endValue),
                                                Optional.of(startValue.getName()),
                                                Optional.of(endValue.getName())))),
                        symbol("city"),
                        scanNode)),
                "SELECT regionid, city, amount_out FROM (SELECT regionid, city, fare, amount, startvalue, endvalue, min(amount) OVER (RANGE BETWEEN startValue PRECEDING AND endValue FOLLOWING) AS amount_out FROM (SELECT regionid, city, fare, amount, startValue AS startvalue, endValue AS endvalue FROM 'table') hetu_table_1) hetu_table_2");
        testJQL(planBuilder -> planBuilder.project(
                Assignments.builder()
                        .put(symbol("regionid"), variable("regionid"))
                        .put(symbol("city"), variable("city"))
                        .put(symbol("amount_out"), variable("amount_out", BIGINT))
                        .build(),
                planBuilder.window(
                        new WindowNode.Specification(
                                ImmutableList.of(),
                                Optional.of(new OrderingScheme(
                                        ImmutableList.of(symbol("fare")),
                                        ImmutableMap.of(symbol("fare"), SortOrder.ASC_NULLS_FIRST)))),
                        ImmutableMap.of(
                                symbol("amount_out"),
                                new WindowNode.Function(
                                        call("min",
                                                metadata.getFunctionAndTypeManager().lookupFunction("min", fromTypes(BIGINT)),
                                                BIGINT,
                                                ImmutableList.of(new VariableReferenceExpression("amount", types.get(symbol("amount"))))),
                                        ImmutableList.of(new VariableReferenceExpression("amount", types.get(symbol("amount")))),
                                        new WindowNode.Frame(
                                                Types.WindowFrameType.ROWS,
                                                Types.FrameBoundType.PRECEDING,
                                                Optional.of(startValue),
                                                Types.FrameBoundType.UNBOUNDED_FOLLOWING,
                                                Optional.empty(),
                                                Optional.of(startValue.getName()),
                                                Optional.empty()))),
                        symbol("city"),
                        scanNode)),
                "SELECT regionid, city, amount_out FROM (SELECT regionid, city, fare, amount, startvalue, endvalue, min(amount) OVER ( ORDER BY fare ASC NULLS FIRST ROWS BETWEEN startValue PRECEDING AND UNBOUNDED FOLLOWING) AS amount_out FROM (SELECT regionid, city, fare, amount, startValue AS startvalue, endValue AS endvalue FROM 'table') hetu_table_1) hetu_table_2");
    }
}
