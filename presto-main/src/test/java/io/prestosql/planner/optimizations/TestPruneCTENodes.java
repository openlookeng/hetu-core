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
package io.prestosql.planner.optimizations;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.SystemSessionProperties.CTE_REUSE_ENABLED;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyNot;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.output;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestPruneCTENodes
        extends BasePlanTest
{
    public TestPruneCTENodes()
    {
        super(TestPruneCTENodes::createQueryRunner);
    }

    private static LocalQueryRunner createQueryRunner()
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
    public void testCrossJoinWithCTE()
    {
        List<PlanOptimizer> allOptimizers = getQueryRunner().getPlanOptimizers(false);

        assertPlan("WITH ss AS (SELECT * FROM orders) \n" +
                        "SELECT * FROM ss ss1, ss ss2",
                cteEnabledSession(),
                anyTree(node(JoinNode.class,
                        tableScan("orders"),
                        anyNot(CTEScanNode.class, exchange(
                                tableScan("orders"))))),
                allOptimizers);
    }

    @Test
    public void testSingleCTEUsage()
    {
        List<PlanOptimizer> allOptimizers = getQueryRunner().getPlanOptimizers(false);

        assertPlan("WITH ss AS (SELECT * FROM orders) \n" +
                        "SELECT * FROM ss",
                cteEnabledSession(),
                output(anyNot(CTEScanNode.class, tableScan("orders"))),
                allOptimizers);
    }

    private Session cteEnabledSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(CTE_REUSE_ENABLED, "true")
                .build();
    }
}
