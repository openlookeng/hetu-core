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
package io.prestosql.sql.planner.optimizations;

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.SystemSessionProperties.isCTEReuseEnabled;
import static io.prestosql.spi.plan.JoinNode.DistributionType.REPLICATED;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.planner.plan.ExchangeNode.gatheringExchange;
import static io.prestosql.sql.planner.plan.ExchangeNode.partitionedExchange;
import static java.util.Objects.requireNonNull;

public class AddExchangeAboveCTENode
        implements PlanOptimizer
{
    public AddExchangeAboveCTENode()
    {
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator,
            PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (isCTEReuseEnabled(session)) {
            OptimizedPlanRewriter optimizedPlanRewriter = new OptimizedPlanRewriter(idAllocator);
            PlanNode newNode = SimplePlanRewriter.rewriteWith(optimizedPlanRewriter, plan, new ExchangeProperties(ExchangeNode.Type.GATHER, new ArrayList<>()));
            return newNode;
        }
        else {
            return plan;
        }
    }

    private static class OptimizedPlanRewriter
            extends SimplePlanRewriter<ExchangeProperties>
    {
        private final PlanNodeIdAllocator idAllocator;

        private OptimizedPlanRewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<ExchangeProperties> context)
        {
            context.get().setType(node.getType());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<ExchangeProperties> context)
        {
            if (node.getDistributionType().get().equals(REPLICATED)) {
                context.get().setType(ExchangeNode.Type.REPLICATE);
            }
            List<Symbol> leftSymbols = node.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::getLeft)
                    .collect(Collectors.toList());
            context.get().setJoinCriteriaSymbols(leftSymbols);
            return visitPlan(node, context);
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<ExchangeProperties> context)
        {
            List<PlanNode> sources = node.getSources();
            List<PlanNode> rewrittenSources = new ArrayList<>();
            for (PlanNode source : sources) {
                ExchangeNode rewrittenNode;
                if (source instanceof CTEScanNode) {
                    switch (context.get().getType()) {
                        case GATHER:
                            rewrittenNode = gatheringExchange(
                                    idAllocator.getNextId(),
                                    REMOTE,
                                    source);
                            break;
                        default:
                            if (((CTEScanNode) source).getSource() instanceof AggregationNode) {
                                if (!((AggregationNode) ((CTEScanNode) source).getSource()).getGroupingKeys().isEmpty()
                                        && ((AggregationNode) ((CTEScanNode) source).getSource()).getGroupingKeys().containsAll(context.get().joinCriteriaSymbols)) {
                                    rewrittenNode = partitionedExchange(
                                            idAllocator.getNextId(),
                                            REMOTE,
                                            source,
                                            ((AggregationNode) ((CTEScanNode) source).getSource()).getGroupingKeys(),
                                            Optional.empty());
                                    break;
                                }
                            }
                            rewrittenNode = partitionedExchange(
                                    idAllocator.getNextId(),
                                    REMOTE,
                                    source,
                                    source.getOutputSymbols(),
                                    Optional.empty());
                    }
                    rewrittenSources.add(rewrittenNode);
                }
                else {
                    rewrittenSources.add(source);
                }
            }

            return context.defaultRewrite(node.replaceChildren(rewrittenSources), context.get());
        }
    }

    public static class ExchangeProperties
    {
        private ExchangeNode.Type type;

        private List<Symbol> joinCriteriaSymbols;

        public ExchangeProperties(ExchangeNode.Type type, List<Symbol> joinCriteriaSymbols)
        {
            this.type = type;
            this.joinCriteriaSymbols = joinCriteriaSymbols;
        }

        public ExchangeNode.Type getType()
        {
            return type;
        }

        public void setType(ExchangeNode.Type type)
        {
            this.type = type;
        }

        public void setJoinCriteriaSymbols(List<Symbol> joinCriteriaSymbols)
        {
            this.joinCriteriaSymbols = joinCriteriaSymbols;
        }
    }
}
