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
package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.PlanVisitor;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MemoryPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private static final Logger log = Logger.get(MemoryPlanOptimizer.class);
    private static final Set<Class<? extends PlanNode>> UNSUPPORTED_ROOT_NODE = ImmutableSet.of(GroupIdNode.class, MarkDistinctNode.class);

    private final TypeManager typeManager;
    private final StandardFunctionResolution functionResolution;
    private final LogicalRowExpressions logicalRowExpressions;
    private final RowExpressionService rowExpressionService;

    @Inject
    public MemoryPlanOptimizer(
            TypeManager typeManager,
            RowExpressionService rowExpressionService,
            DeterminismEvaluator determinismEvaluator,
            FunctionMetadataManager functionManager,
            StandardFunctionResolution functionResolution)
    {
        this.typeManager = typeManager;
        this.rowExpressionService = rowExpressionService;
        this.functionResolution = functionResolution;
        this.logicalRowExpressions = new LogicalRowExpressions(
                determinismEvaluator,
                functionResolution,
                functionManager);
    }

    @Override
    public PlanNode optimize(
            PlanNode maxSubPlan,
            ConnectorSession session,
            Map<String, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        // Some node cannot be push down root node.
        if (UNSUPPORTED_ROOT_NODE.contains(maxSubPlan.getClass())) {
            return maxSubPlan;
        }
        return maxSubPlan.accept(new Visitor(idAllocator, types, session, symbolAllocator), null);
    }

    private static PlanNode replaceChildren(PlanNode node, List<PlanNode> children)
    {
        List<PlanNode> childrenNodes = node.getSources();
        for (int i = 0; i < childrenNodes.size(); i++) {
            if (children.get(i) != childrenNodes.get(i)) {
                return node.replaceChildren(children);
            }
        }
        return node;
    }

    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final ConnectorSession session;
        private final Map<String, Type> types;
        private final SymbolAllocator symbolAllocator;
        private final IdentityHashMap<FilterNode, Void> filtersSplitUp = new IdentityHashMap<>();

        public Visitor(
                PlanNodeIdAllocator idAllocator,
                Map<String, Type> types,
                ConnectorSession session,
                SymbolAllocator symbolAllocator)
        {
            this.idAllocator = idAllocator;
            this.types = types;
            this.session = session;
            this.symbolAllocator = symbolAllocator;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            return node;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            if (filtersSplitUp.containsKey(node)) {
                return this.visitPlan(node, context);
            }
            filtersSplitUp.put(node, null);
            FilterNode nodeToRecurseInto = node;
            List<RowExpression> pushableExpressions = new ArrayList<>();
            List<RowExpression> nonPushableExpressions = new ArrayList<>();

            for (RowExpression conjunct : logicalRowExpressions.extractConjuncts(node.getPredicate())) {
                // TODO: determine what expressions can be pushed down
                if (conjunct instanceof CallExpression) {
                    pushableExpressions.add(conjunct);
                }
                else {
                    nonPushableExpressions.add(conjunct);
                }
            }
            if (!pushableExpressions.isEmpty()) {
                RowExpression pushableExpression = logicalRowExpressions.combineConjuncts(pushableExpressions);
                FilterNode pushableFilter = new FilterNode(idAllocator.getNextId(), node.getSource(), pushableExpression);

                RowExpression nonPushableExpression = logicalRowExpressions.combineConjuncts(pushableExpressions);
                Optional<FilterNode> nonPushableFilter = nonPushableExpressions.isEmpty() ? Optional.empty() :
                        Optional.of(new FilterNode(idAllocator.getNextId(), pushableFilter, nonPushableExpression));

                filtersSplitUp.put(pushableFilter, null);
                if (nonPushableFilter.isPresent()) {
                    FilterNode nonPushableFilterNode = nonPushableFilter.get();
                    filtersSplitUp.put(nonPushableFilterNode, null);
                    nodeToRecurseInto = nonPushableFilterNode;
                }
                else {
                    nodeToRecurseInto = pushableFilter;
                }
            }
            return this.visitFilter(nodeToRecurseInto, context);
        }
    }
}
