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

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.optimizations.SymbolMapper;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Optional;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.topN;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static io.prestosql.sql.relational.ProjectNodeUtils.isIdentity;

/**
 * Transforms:
 * <pre>
 * - TopN
 *    - Project (non-identity)
 *       - Source other than Filter(TableScan) or TableScan
 * </pre>
 * Into:
 * <pre>
 * - Project
 *    - TopN
 *       - Source
 * </pre>
 */
public final class PushTopNThroughProject
        implements Rule<TopNNode>
{
    private static final Capture<ProjectNode> PROJECT_CHILD = newCapture();

    private static final Pattern<TopNNode> PATTERN =
            topN()
                    .with(source().matching(
                            project()
                                    // do not push topN through identity projection which could be there for column pruning purposes
                                    .matching(projectNode -> !isIdentity(projectNode))
                                    .capturedAs(PROJECT_CHILD)
                                    // do not push topN between projection and table scan so that they can be merged into a PageProcessor
                                    .with(source().matching(node -> !(node instanceof TableScanNode)))));

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode parent, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECT_CHILD);

        // do not push topN between projection and filter(table scan) so that they can be merged into a PageProcessor
        PlanNode projectSource = context.getLookup().resolve(projectNode.getSource());
        if (projectSource instanceof FilterNode) {
            PlanNode filterSource = context.getLookup().resolve(((FilterNode) projectSource).getSource());
            if (filterSource instanceof TableScanNode) {
                return Result.empty();
            }
        }

        Optional<SymbolMapper> symbolMapper = symbolMapper(parent.getOrderingScheme().getOrderBy(), projectNode.getAssignments());
        if (!symbolMapper.isPresent()) {
            return Result.empty();
        }

        TopNNode mappedTopN = symbolMapper.get().map(parent, projectNode.getSource(), context.getIdAllocator().getNextId());
        return Result.ofPlanNode(projectNode.replaceChildren(ImmutableList.of(mappedTopN)));
    }

    private Optional<SymbolMapper> symbolMapper(List<Symbol> symbols, Assignments assignments)
    {
        SymbolMapper.Builder mapper = SymbolMapper.builder();
        for (Symbol symbol : symbols) {
            if (isExpression(assignments.get(symbol))) {
                Expression expression = castToExpression(assignments.get(symbol));
                if (!(expression instanceof SymbolReference)) {
                    return Optional.empty();
                }
                mapper.put(symbol, SymbolUtils.from(expression));
            }
            else {
                RowExpression expression = assignments.get(symbol);
                if (!(expression instanceof VariableReferenceExpression)) {
                    return Optional.empty();
                }
                mapper.put(symbol, new Symbol(((VariableReferenceExpression) expression).getName()));
            }
        }
        return Optional.of(mapper.build());
    }
}
