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
package io.prestosql.sql.planner.optimizations;

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;

import static io.prestosql.SystemSessionProperties.isSkipNonApplicableRulesEnabled;
import static io.prestosql.spi.plan.PlanNode.SkipOptRuleLevel.APPLY_ALL_LEGACY_AND_ROWEXPR;
import static io.prestosql.spi.plan.PlanNode.SkipOptRuleLevel.APPLY_ALL_LEGACY_AND_ROWEXPR_PUSH_PREDICATE;
import static io.prestosql.spi.plan.PlanNode.SkipOptRuleLevel.APPLY_ALL_RULES;
import static java.util.Objects.requireNonNull;

public class AdjustApplicableOptimizationRule
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator symbolAllocator,
                             PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (!isSkipNonApplicableRulesEnabled(session)) {
            return plan;
        }

        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        return SimplePlanRewriter.rewriteWith(new OptimizedPlanRewriter(), plan);
    }

    private static class OptimizedPlanRewriter
            extends SimplePlanRewriter<PlanNode>
    {
        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<PlanNode> context)
        {
            node.setSkipOptRuleLevel(APPLY_ALL_LEGACY_AND_ROWEXPR);
            return context.defaultRewrite(node, node);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<PlanNode> context)
        {
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<PlanNode> context)
        {
            if (context.get().getSkipOptRuleLevel() != APPLY_ALL_RULES) {
                context.get().setSkipOptRuleLevel(APPLY_ALL_LEGACY_AND_ROWEXPR_PUSH_PREDICATE);
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<PlanNode> context)
        {
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<PlanNode> context)
        {
            // if any other node also there, reset the flag
            context.get().setSkipOptRuleLevel(APPLY_ALL_RULES);
            return node;    // No need to traverse further
        }
    }
}
