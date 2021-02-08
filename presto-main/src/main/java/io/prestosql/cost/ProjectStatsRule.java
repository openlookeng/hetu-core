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
package io.prestosql.cost;

import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;

import java.util.Map;
import java.util.Optional;

import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Objects.requireNonNull;

public class ProjectStatsRule
        extends SimpleStatsRule<ProjectNode>
{
    private static final Pattern<ProjectNode> PATTERN = project();

    private final ScalarStatsCalculator scalarStatsCalculator;

    public ProjectStatsRule(ScalarStatsCalculator scalarStatsCalculator, StatsNormalizer normalizer)
    {
        super(normalizer);
        this.scalarStatsCalculator = requireNonNull(scalarStatsCalculator, "scalarStatsCalculator is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(ProjectNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        PlanNodeStatsEstimate.Builder calculatedStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(sourceStats.getOutputRowCount());
        Map<Integer, Symbol> layout = null;
        for (Map.Entry<Symbol, RowExpression> entry : node.getAssignments().entrySet()) {
            RowExpression expression = entry.getValue();
            if (isExpression(expression)) {
                calculatedStats.addSymbolStatistics(entry.getKey(), scalarStatsCalculator.calculate(castToExpression(expression), sourceStats, session, types));
            }
            else {
                if (layout == null) {
                    layout = SymbolUtils.toLayOut(node.getOutputSymbols());
                }
                calculatedStats.addSymbolStatistics(entry.getKey(), scalarStatsCalculator.calculate(expression, sourceStats, session, layout));
            }
        }
        return Optional.of(calculatedStats.build());
    }
}
