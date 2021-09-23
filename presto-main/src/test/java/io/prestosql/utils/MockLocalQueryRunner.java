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
package io.prestosql.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.sql.planner.LogicalPlanner;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.RuleStatsRecorder;
import io.prestosql.sql.planner.iterative.IterativeOptimizer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.testing.LocalQueryRunner;

import java.util.Map;
import java.util.Set;

import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class MockLocalQueryRunner
        extends LocalQueryRunner
{
    public MockLocalQueryRunner(Map<String, String> sessionProperties)
    {
        super(createSession(sessionProperties));
    }

    public void init()
    {
        createCatalog(getDefaultSession().getCatalog().get(), new TpchConnectorFactory(1), ImmutableMap.of());
    }

    private static Session createSession(Map<String, String> sessionProperties)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder().setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        sessionProperties.entrySet()
                .forEach(entry -> sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue()));
        return sessionBuilder.build();
    }

    public Plan createPlan(String sql, PlanOptimizer optimizer)
    {
        IterativeOptimizer iterativeOptimizer = createIterativeOptimizer(
                ImmutableSet.of(new RemoveRedundantIdentityProjections()));
        return this.inTransaction(transactionSession -> this.createPlan(transactionSession, sql,
            ImmutableList.of(iterativeOptimizer, optimizer), LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED,
            WarningCollector.NOOP));
    }

    public Plan createPlan(String sql, Rule<?> rule)
    {
        IterativeOptimizer optimizer = createIterativeOptimizer(ImmutableSet.of(rule));
        return this.createPlan(sql, optimizer);
    }

    private IterativeOptimizer createIterativeOptimizer(Set<Rule<?>> rules)
    {
        return new IterativeOptimizer(new RuleStatsRecorder(), getStatsCalculator(), getCostCalculator(), rules);
    }
}
