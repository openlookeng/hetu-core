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
import com.google.common.collect.ImmutableListMultimap;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.RowExpressionVariableInliner;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.optimizations.SetOperationNodeUtils.sourceSymbolMap;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.union;

public class PushProjectionThroughUnion
        implements Rule<ProjectNode>
{
    private static final Capture<UnionNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .matching(PushProjectionThroughUnion::nonTrivialProjection)
            .with(source().matching(union().capturedAs(CHILD)));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode parent, Captures captures, Context context)
    {
        UnionNode source = captures.get(CHILD);

        // OutputLayout of the resultant Union, will be same as the layout of the Project
        List<Symbol> outputLayout = parent.getOutputSymbols();

        // Mapping from the output symbol to ordered list of symbols from each of the sources
        ImmutableListMultimap.Builder<Symbol, Symbol> mappings = ImmutableListMultimap.builder();

        // sources for the resultant UnionNode
        ImmutableList.Builder<PlanNode> outputSources = ImmutableList.builder();

        for (int i = 0; i < source.getSources().size(); i++) {
            Map<Symbol, SymbolReference> outputToInput = sourceSymbolMap(source, i);   // Map: output of union -> input of this source to the union
            Assignments.Builder assignments = Assignments.builder(); // assignments for the new ProjectNode

            // mapping from current ProjectNode to new ProjectNode, used to identify the output layout
            Map<Symbol, Symbol> projectSymbolMapping = new HashMap<>();

            // Translate the assignments in the ProjectNode using symbols of the source of the UnionNode
            for (Map.Entry<Symbol, RowExpression> entry : parent.getAssignments().entrySet()) {
                RowExpression translatedExpression;
                Map<VariableReferenceExpression, VariableReferenceExpression> variable = new HashMap<>();
                Map<Symbol, Type> symbols = context.getSymbolAllocator().getSymbols();
                for (Symbol symbolMap : source.getSymbolMapping().keySet()) {
                    Symbol symboli = source.getSymbolMapping().get(symbolMap).get(i);
                    variable.put(new VariableReferenceExpression(symbolMap.getName(), symbols.get(symbolMap)),
                            new VariableReferenceExpression(symboli.getName(), symbols.get(symboli)));
                }
                translatedExpression = RowExpressionVariableInliner.inlineVariables(variable, entry.getValue());
                Symbol symbol = context.getSymbolAllocator().newSymbol(translatedExpression);
                assignments.put(symbol, translatedExpression);
                projectSymbolMapping.put(entry.getKey(), symbol);
            }
            outputSources.add(new ProjectNode(context.getIdAllocator().getNextId(), source.getSources().get(i), assignments.build()));
            outputLayout.forEach(symbol -> mappings.put(symbol, projectSymbolMapping.get(symbol)));
        }

        return Result.ofPlanNode(new UnionNode(parent.getId(), outputSources.build(), mappings.build(), ImmutableList.copyOf(mappings.build().keySet())));
    }

    private static boolean nonTrivialProjection(ProjectNode project)
    {
        return !project.getAssignments()
                .getExpressions().stream()
                .allMatch(expression -> expression instanceof VariableReferenceExpression);
    }
}
