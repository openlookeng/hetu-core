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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.matching.Captures;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.IsNotNullPredicate;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;

public class TransformUncorrelatedInPredicateSubqueryToJoin
        extends TransformUncorrelatedInPredicateSubqueryToSemiJoin
{
    @Override
    public Result apply(ApplyNode applyNode, Captures captures, Context context)
    {
        if (applyNode.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }

        Expression expression = castToExpression(getOnlyElement(applyNode.getSubqueryAssignments().getExpressions()));
        InPredicate inPredicate;
        if (expression instanceof InPredicate) {
            inPredicate = (InPredicate) expression;
        }
        else {
            return Result.empty();
        }

        Symbol semiJoinSymbol = getOnlyElement(applyNode.getSubqueryAssignments().getSymbols());

        JoinNode.EquiJoinClause equiJoinClause = new JoinNode.EquiJoinClause(SymbolUtils.from(inPredicate.getValue()), SymbolUtils.from(inPredicate.getValueList()));
        List<Symbol> outputSymbols = new LinkedList<>(applyNode.getInput().getOutputSymbols());
        outputSymbols.add(SymbolUtils.from(inPredicate.getValueList()));

        AggregationNode distinctNode = new AggregationNode(
                context.getIdAllocator().getNextId(),
                applyNode.getSubquery(),
                ImmutableMap.of(),
                singleGroupingSet(applyNode.getSubquery().getOutputSymbols()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());

        JoinNode joinNode = new JoinNode(context.getIdAllocator().getNextId(),
                JoinNode.Type.RIGHT,
                distinctNode,
                applyNode.getInput(),
                ImmutableList.of(equiJoinClause),
                outputSymbols,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Collections.emptyMap());

        Map<Symbol, RowExpression> assignments = new HashMap<>();
        assignments.put(semiJoinSymbol, castToRowExpression(new IsNotNullPredicate(inPredicate.getValueList())));
        for (Symbol symbol : applyNode.getInput().getOutputSymbols()) {
            assignments.put(symbol, castToRowExpression(toSymbolReference(symbol)));
        }
        ProjectNode projectNode = new ProjectNode(context.getIdAllocator().getNextId(),
                joinNode,
                new Assignments(assignments));

        return Result.ofPlanNode(projectNode);
    }
}
