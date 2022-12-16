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

package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode.DistributionType;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.CTE_REUSE_ENABLED;
import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.SystemSessionProperties.SPILL_ENABLED;
import static io.prestosql.SystemSessionProperties.TASK_CONCURRENCY;
import static io.prestosql.SystemSessionProperties.USE_EXACT_PARTITIONING;
import static io.prestosql.spi.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.spi.plan.JoinNode.DistributionType.REPLICATED;
import static io.prestosql.spi.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyNot;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestAddExchangesPlans
        extends BasePlanTest
{
    public TestAddExchangesPlans()
    {
        super();
    }

    protected LocalQueryRunner createQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setSpillerSpillPaths("/tmp/test_spill_path");
        LocalQueryRunner queryRunner = new LocalQueryRunner(session, featuresConfig);
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testRepartitionForUnionWithAnyTableScans()
    {
        assertDistributedPlan("SELECT nationkey FROM nation UNION select regionkey from region",
                anyTree(
                        aggregation(ImmutableMap.of(),
                                anyTree(
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation")))),
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                tableScan("region"))))))));
        assertDistributedPlan("SELECT nationkey FROM nation UNION select 1",
                anyTree(
                        aggregation(ImmutableMap.of(),
                                anyTree(
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation")))),
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                values())))))));
    }

    @Test
    public void testRepartitionForUnionAllBeforeHashJoin()
    {
        assertDistributedPlan("SELECT * FROM (SELECT nationkey FROM nation UNION ALL select nationkey from nation) n join region r on n.nationkey = r.regionkey",
                noJoinReordering(),
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("nationkey", "regionkey")),
                                anyTree(
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))),
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        tableScan("nation")))),
                                anyTree(
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));

        assertDistributedPlan("SELECT * FROM (SELECT nationkey FROM nation UNION ALL select 1) n join region r on n.nationkey = r.regionkey",
                noJoinReordering(),
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("nationkey", "regionkey")),
                                anyTree(
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))),
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        values()))),
                                anyTree(
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));
    }

    @Test
    public void testNonSpillableBroadcastJoinAboveTableScan()
    {
        assertDistributedPlan(
                "SELECT * FROM nation n join region r on n.nationkey = r.regionkey",
                spillEnabledWithJoinDistributionType(BROADCAST),
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("nationkey", "regionkey")), Optional.empty(), Optional.of(REPLICATED), Optional.of(false),
                                anyNot(ExchangeNode.class,
                                        node(FilterNode.class, tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))),
                                anyTree(
                                        exchange(REMOTE, REPLICATE,
                                                anyTree(
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));

        assertDistributedPlan(
                "SELECT * FROM nation n join region r on n.nationkey = r.regionkey",
                spillEnabledWithJoinDistributionType(PARTITIONED),
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("nationkey", "regionkey")), Optional.empty(), Optional.of(DistributionType.PARTITIONED), Optional.empty(),
                                exchange(REMOTE, REPARTITION,
                                        anyTree(
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))),
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));
    }

    private Session spillEnabledWithJoinDistributionType(JoinDistributionType joinDistributionType)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, joinDistributionType.toString())
                .setSystemProperty(SPILL_ENABLED, "true")
                .setSystemProperty(TASK_CONCURRENCY, "16")
                .build();
    }

    private Session noJoinReordering()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .build();
    }

    private Session cteEnabledSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(CTE_REUSE_ENABLED, "true")
                .build();
    }

    @Test
    public void testAggregateIsExactlyPartitioned()
    {
        assertDistributedPlan(
                "SELECT\n" +
                        "    AVG(1)\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        orderkey,\n" +
                        "        orderstatus,\n" +
                        "        COUNT(*)\n" +
                        "    FROM orders\n" +
                        "    WHERE\n" +
                        "        orderdate > CAST('2042-01-01' AS DATE)\n" +
                        "    GROUP BY\n" +
                        "        orderkey,\n" +
                        "        orderstatus\n" +
                        ")\n" +
                        "GROUP BY\n" +
                        "    orderkey",
                useExactPartitioning(),
                anyTree(
                        exchange(REMOTE, REPARTITION,
                                anyTree(
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ordertatus", "orderstatus",
                                                                "orderkey", "orderkey",
                                                                "orderdate", "orderdate"))))))));
    }

    private Session useExactPartitioning()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                .setSystemProperty(USE_EXACT_PARTITIONING, "true")
                .build();
    }

    @Test
    public void testRowNumberIsExactlyPartitioned()
    {
        assertDistributedPlan(
                "SELECT\n" +
                        "    *\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        a,\n" +
                        "        ROW_NUMBER() OVER (\n" +
                        "            PARTITION BY\n" +
                        "                a\n" +
                        "        ) rn\n" +
                        "    FROM (\n" +
                        "        VALUES\n" +
                        "            (1)\n" +
                        "    ) t (a)\n" +
                        ") t",
                useExactPartitioning(),
                anyTree(
                        exchange(REMOTE, REPARTITION,
                                anyTree(
                                        values("a")))));
    }

    @Test
    public void testTopNRowNumberIsExactlyPartitioned()
    {
        assertDistributedPlan(
                "SELECT\n" +
                        "    a,\n" +
                        "    ROW_NUMBER() OVER (\n" +
                        "        PARTITION BY\n" +
                        "            a\n" +
                        "        ORDER BY\n" +
                        "            a\n" +
                        "    ) rn\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        a,\n" +
                        "        b,\n" +
                        "        COUNT(*)\n" +
                        "    FROM (\n" +
                        "        VALUES\n" +
                        "            (1, 2)\n" +
                        "    ) t (a, b)\n" +
                        "    GROUP BY\n" +
                        "        a,\n" +
                        "        b\n" +
                        ")\n" +
                        "LIMIT\n" +
                        "    2",
                useExactPartitioning(),
                anyTree(
                        exchange(REMOTE, REPARTITION,
                                anyTree(
                                        values("a", "b")))));
    }

    @Test
    public void testJoinIsExactlyPartitioned()
    {
        assertDistributedPlan(
                "SELECT\n" +
                        "    orders.orderkey,\n" +
                        "    orders.orderstatus\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        orderkey,\n" +
                        "        ARBITRARY(orderstatus) AS orderstatus,\n" +
                        "        COUNT(*)\n" +
                        "    FROM orders\n" +
                        "    GROUP BY\n" +
                        "        orderkey\n" +
                        ") t,\n" +
                        "orders\n" +
                        "WHERE\n" +
                        "    orders.orderkey = t.orderkey\n" +
                        "    AND orders.orderstatus = t.orderstatus",
                useExactPartitioning(),
                anyTree(
                        exchange(REMOTE, REPARTITION,
                                anyTree(
                                        aggregation(
                                                singleGroupingSet("orderkey"),
                                                ImmutableMap.of(Optional.of("arbitrary"), PlanMatchPattern.functionCall("arbitrary", false, ImmutableList.of(anySymbol()))),
                                                ImmutableList.of("orderkey"),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                tableScan("orders", ImmutableMap.of(
                                                        "orderkey", "orderkey",
                                                        "orderstatus", "orderstatus"))))),
                        exchange(LOCAL, GATHER,
                                exchange(REMOTE, REPARTITION,
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of(
                                                        "orderkey1", "orderkey",
                                                        "orderstatus3", "orderstatus")))))));
    }

    @Test
    public void testMarkDistinctIsExactlyPartitioned()
    {
        assertDistributedPlan(
                "    SELECT\n" +
                        "        orderkey,\n" +
                        "        orderstatus,\n" +
                        "        COUNT(DISTINCT orderdate),\n" +
                        "        COUNT(DISTINCT clerk)\n" +
                        "    FROM orders\n" +
                        "    WHERE\n" +
                        "        orderdate > CAST('2042-01-01' AS DATE)\n" +
                        "    GROUP BY\n" +
                        "        orderkey,\n" +
                        "        orderstatus\n",
                useExactPartitioning(),
                anyTree(
                        exchange(REMOTE, REPARTITION,
                                anyTree(
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        exchange(REMOTE, REPARTITION,
                                                                anyTree(
                                                                        tableScan("orders", ImmutableMap.of(
                                                                                "orderstatus", "orderstatus",
                                                                                "orderkey", "orderkey",
                                                                                "clerk", "clerk",
                                                                                "orderdate", "orderdate"))))))))));
    }

    // Negative test for use-exact-partitioning
    @Test
    public void testJoinNotExactlyPartitioned()
    {
        assertDistributedPlan(
                "SELECT\n" +
                        "    orders.orderkey,\n" +
                        "    orders.orderstatus\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        orderkey,\n" +
                        "        ARBITRARY(orderstatus) AS orderstatus,\n" +
                        "        COUNT(*)\n" +
                        "    FROM orders\n" +
                        "    GROUP BY\n" +
                        "        orderkey\n" +
                        ") t,\n" +
                        "orders\n" +
                        "WHERE\n" +
                        "    orders.orderkey = t.orderkey\n" +
                        "    AND orders.orderstatus = t.orderstatus",
                noJoinReordering(),
                anyTree(
                        project(
                                anyTree(
                                        tableScan("orders"))),
                        exchange(LOCAL, GATHER,
                                exchange(REMOTE, REPARTITION,
                                        anyTree(
                                                tableScan("orders"))))));
    }
}
