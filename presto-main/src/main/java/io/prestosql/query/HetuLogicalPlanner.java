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
package io.prestosql.query;

import io.prestosql.Session;
import io.prestosql.cost.CachingCostProvider;
import io.prestosql.cost.CachingStatsProvider;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.CostProvider;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.cost.StatsProvider;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.planner.LogicalPlanner;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.optimizations.BeginTableWrite;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.sanity.PlanSanityChecker;
import io.prestosql.utils.OptimizerUtils;

import java.util.List;
import java.util.Optional;

import static io.prestosql.SystemSessionProperties.isSkipAttachingStatsWithPlan;
import static io.prestosql.spi.plan.PlanNode.SkipOptRuleLevel.APPLY_ALL_RULES;
import static io.prestosql.sql.planner.sanity.PlanSanityChecker.DISTRIBUTED_PLAN_SANITY_CHECKER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HetuLogicalPlanner
        extends LogicalPlanner
{
    private final PlanNodeIdAllocator idAllocator;

    private final Session session;

    private final List<PlanOptimizer> planOptimizers;

    private final PlanSanityChecker planSanityChecker;

    private final Metadata metadata;

    private final TypeAnalyzer typeAnalyzer;

    private final StatsCalculator statsCalculator;

    private final CostCalculator costCalculator;

    private final WarningCollector warningCollector;

    public HetuLogicalPlanner(Session session, List<PlanOptimizer> planOptimizers, PlanNodeIdAllocator idAllocator,
            Metadata metadata, TypeAnalyzer typeAnalyzer, StatsCalculator statsCalculator, CostCalculator costCalculator,
            WarningCollector warningCollector)
    {
        super(session, planOptimizers, idAllocator, metadata, typeAnalyzer, statsCalculator, costCalculator,
                warningCollector);
        this.session = session;
        this.planOptimizers = planOptimizers;
        this.planSanityChecker = DISTRIBUTED_PLAN_SANITY_CHECKER;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.typeAnalyzer = typeAnalyzer;
        this.statsCalculator = statsCalculator;
        this.costCalculator = costCalculator;
        this.warningCollector = warningCollector;
    }

    @Override
    public Plan plan(Analysis analysis, boolean skipStatsWithPlan, Stage stage)
    {
        PlanNode root = planStatement(analysis, analysis.getStatement());
        PlanNode.SkipOptRuleLevel optimizationLevel = APPLY_ALL_RULES;

        planSanityChecker.validateIntermediatePlan(root, session, metadata, typeAnalyzer, planSymbolAllocator.getTypes(),
                warningCollector);

        if (stage.ordinal() >= Stage.OPTIMIZED.ordinal()) {
            for (PlanOptimizer optimizer : planOptimizers) {
                if (OptimizerUtils.isEnabledLegacy(optimizer, session, root)) {
                    // Hetu: BeginTableWrite writes catalog name, schema name, etc to the Hetu meata data
                    // We run it separately in the SqlQueryExecution
                    if (optimizer instanceof BeginTableWrite) {
                        continue;
                    }

                    if (OptimizerUtils.canApplyOptimizer(optimizer, optimizationLevel)) {
                        root = optimizer.optimize(root, session, planSymbolAllocator.getTypes(), planSymbolAllocator, idAllocator,
                                warningCollector);
                        requireNonNull(root, format("%s returned a null plan", optimizer.getClass().getName()));
                        optimizationLevel = optimizationLevel == APPLY_ALL_RULES ? root.getSkipOptRuleLevel() : optimizationLevel;
                    }
                }
            }
        }

        if (stage.ordinal() >= Stage.OPTIMIZED_AND_VALIDATED.ordinal()) {
            // make sure we produce a valid plan after optimizations run. This is mainly to catch programming errors
            planSanityChecker.validateFinalPlan(root, session, metadata, typeAnalyzer, planSymbolAllocator.getTypes(),
                    warningCollector);
        }

        TypeProvider types = planSymbolAllocator.getTypes();

        // Incase SKIP_ATTACHING_STATS_WITH_PLAN is enabled, in order to reduce call to get stats from metastore,
        // we calculate stats here only if need to show as part of EXPLAIN, otherwise not needed.
        if (skipStatsWithPlan && isSkipAttachingStatsWithPlan(session)) {
            return new Plan(root, types, StatsAndCosts.empty());
        }
        else {
            StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, types);
            CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session,
                    types);
            return new Plan(root, types, StatsAndCosts.create(root, statsProvider, costProvider));
        }
    }

    public void validateCachedPlan(PlanNode planNode, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, WarningCollector warningCollector)
    {
        planSanityChecker.validateFinalPlan(planNode, session, metadata, typeAnalyzer, planSymbolAllocator.getTypes(), warningCollector);
    }
}
