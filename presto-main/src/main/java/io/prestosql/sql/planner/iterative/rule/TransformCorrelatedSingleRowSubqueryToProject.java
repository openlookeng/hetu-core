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
package io.prestosql.sql.planner.iterative.rule;

import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AssignmentUtils;
import io.prestosql.sql.planner.plan.LateralJoinNode;

import java.util.List;

import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.sql.planner.plan.Patterns.LateralJoin.filter;
import static io.prestosql.sql.planner.plan.Patterns.lateralJoin;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;

/**
 * This optimizer can rewrite correlated single row subquery to projection in a way described here:
 * From:
 * <pre>
 * - Lateral(with correlation list: [A, C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (subquery)
 *     - Project (A + C)
 *       - single row VALUES()
 * </pre>
 * to:
 * <pre>
 *   - Project(A, B, C, A + C)
 *       - (input) plan which produces symbols: [A, B, C]
 * </pre>
 */

public class TransformCorrelatedSingleRowSubqueryToProject
        implements Rule<LateralJoinNode>
{
    private static final Pattern<LateralJoinNode> PATTERN = lateralJoin()
            .with(filter().equalTo(TRUE_LITERAL));

    @Override
    public Pattern<LateralJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LateralJoinNode parent, Captures captures, Context context)
    {
        List<ValuesNode> values = searchFrom(parent.getSubquery(), context.getLookup())
                .recurseOnlyWhen(ProjectNode.class::isInstance)
                .where(ValuesNode.class::isInstance)
                .findAll();

        if (values.size() != 1 || !isSingleRowValuesWithNoColumns(values.get(0))) {
            return Result.empty();
        }

        List<ProjectNode> subqueryProjections = searchFrom(parent.getSubquery(), context.getLookup())
                .where(node -> node instanceof ProjectNode && !node.getOutputSymbols().equals(parent.getCorrelation()))
                .findAll();

        if (subqueryProjections.size() == 0) {
            return Result.ofPlanNode(parent.getInput());
        }
        if (subqueryProjections.size() == 1) {
            Assignments assignments = Assignments.builder()
                    .putAll(AssignmentUtils.identityAsSymbolReferences(parent.getInput().getOutputSymbols()))
                    .putAll(subqueryProjections.get(0).getAssignments())
                    .build();
            return Result.ofPlanNode(projectNode(parent.getInput(), assignments, context));
        }

        return Result.empty();
    }

    private ProjectNode projectNode(PlanNode source, Assignments assignments, Context context)
    {
        return new ProjectNode(context.getIdAllocator().getNextId(), source, assignments);
    }

    private static boolean isSingleRowValuesWithNoColumns(ValuesNode values)
    {
        return values.getRows().size() == 1 && values.getRows().get(0).size() == 0;
    }
}
