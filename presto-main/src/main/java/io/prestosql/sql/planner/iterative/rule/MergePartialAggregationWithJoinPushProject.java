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

import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeProvider;

import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;

public class MergePartialAggregationWithJoinPushProject
        extends MergePartialAggregationWithJoin
{
    private static final Capture<ProjectNode> PROJECT_NODE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(MergePartialAggregationWithJoin::isSupportedAggregationNode)
            .with(source().matching(project().capturedAs(PROJECT_NODE)
                    .with(source().matching(join().capturedAs(JOIN_NODE)))));

    public MergePartialAggregationWithJoinPushProject(Metadata metadata)
    {
        super(metadata);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECT_NODE);
        JoinNode joinNode = captures.get(JOIN_NODE);
        if (!nonTrivialProjection(projectNode)) {
            return Result.empty();
        }
        if (joinNode.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }

        // Check if Project can be pushed down through join
        // Check if aggregations can be pushed down through join
        // First push Project through Join
        // Then call super.apply() or equivalent function to apply the rule
        Assignments assignments = projectNode.getAssignments();
        Assignments.Builder leftAssignments = Assignments.builder();
        Assignments.Builder rightAssignments = Assignments.builder();
        HashSet<Symbol> leftSymbolSet = new HashSet<>(joinNode.getLeft().getOutputSymbols());
        HashSet<Symbol> rightSymbolSet = new HashSet<>(joinNode.getRight().getOutputSymbols());
        for (Entry<Symbol, RowExpression> assignment : assignments.entrySet()) {
            List<Symbol> symbols = SymbolsExtractor.extractAll(assignment.getValue());
            if (leftSymbolSet.containsAll(symbols)) {
                leftAssignments.put(assignment.getKey(), assignment.getValue());
            }
            else if (rightSymbolSet.containsAll(symbols)) {
                rightAssignments.put(assignment.getKey(), assignment.getValue());
            }
            else {
                return Result.empty();
            }
        }
        TypeProvider typeProvider = context.getSymbolAllocator().getTypes();
        for (Entry<String, Symbol> df : joinNode.getDynamicFilters().entrySet()) {
            if (leftSymbolSet.contains(df.getValue())) {
                leftAssignments.put(df.getValue(), new VariableReferenceExpression(df.getValue().getName(), typeProvider.get(df.getValue())));
            }
            else if (rightSymbolSet.contains(df.getValue())) {
                rightAssignments.put(df.getValue(), new VariableReferenceExpression(df.getValue().getName(), typeProvider.get(df.getValue())));
            }
        }

        PlanNode leftNode = joinNode.getLeft();
        Assignments build = leftAssignments.build();
        if (build.size() > 0) {
            leftNode = new ProjectNode(context.getIdAllocator().getNextId(), joinNode.getLeft(), build);
        }

        PlanNode rightNode = joinNode.getRight();
        build = rightAssignments.build();
        if (build.size() > 0) {
            rightNode = new ProjectNode(context.getIdAllocator().getNextId(), joinNode.getRight(), build);
        }
        joinNode = new JoinNode(joinNode.getId(),
                joinNode.getType(),
                leftNode,
                rightNode,
                joinNode.getCriteria(),
                projectNode.getOutputSymbols(),
                joinNode.getFilter(),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType(),
                joinNode.isSpillable(),
                joinNode.getDynamicFilters());
        node = new AggregationNode(node.getId(),
                joinNode,
                node.getAggregations(),
                node.getGroupingSets(),
                node.getPreGroupedSymbols(),
                node.getStep(),
                node.getHashSymbol(),
                node.getGroupIdSymbol(),
                node.getAggregationType(),
                node.getFinalizeSymbol());
        return checkAndApplyRule(node, context, joinNode);
    }

    private static boolean nonTrivialProjection(ProjectNode project)
    {
        return !project.getAssignments()
                .getExpressions().stream()
                .allMatch(expression -> expression instanceof VariableReferenceExpression);
    }
}
