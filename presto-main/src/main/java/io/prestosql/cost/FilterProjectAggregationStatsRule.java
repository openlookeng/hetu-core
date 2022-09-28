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
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupReference;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;

import java.util.Optional;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.prestosql.SystemSessionProperties.isNonEstimatablePredicateApproximationEnabled;
import static io.prestosql.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.ProjectNodeUtils.isIdentity;
import static java.util.Objects.requireNonNull;

/**
 * AggregationStatsRule does not have sufficient information to provide accurate estimates on aggregated symbols.
 * This rule finds unestimated filters on top of aggregations whose output row count was estimated and returns
 * estimated row count of (UNKNOWN_FILTER_COEFFICIENT * aggregation output row count) for the filter.
 */
public class FilterProjectAggregationStatsRule
        extends SimpleStatsRule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FilterStatsCalculator filterStatsCalculator;

    public FilterProjectAggregationStatsRule(StatsNormalizer normalizer, FilterStatsCalculator filterStatsCalculator)
    {
        super(normalizer);
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator cannot be null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(FilterNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        if (!isNonEstimatablePredicateApproximationEnabled(session)) {
            return Optional.empty();
        }
        PlanNode nodeSource = resolveGroup(lookup, node.getSource());
        AggregationNode aggregationNode;
        if (nodeSource instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) nodeSource;
            if (!isIdentity(projectNode)) {
                return Optional.empty();
            }
            PlanNode projectNodeSource = resolveGroup(lookup, projectNode.getSource());
            if (!(projectNodeSource instanceof AggregationNode)) {
                return Optional.empty();
            }
            aggregationNode = (AggregationNode) projectNodeSource;
        }
        else if (nodeSource instanceof AggregationNode) {
            aggregationNode = (AggregationNode) nodeSource;
        }
        else {
            return Optional.empty();
        }

        return calculate(node, aggregationNode, sourceStats, session, types);
    }

    private Optional<PlanNodeStatsEstimate> calculate(FilterNode filterNode, AggregationNode aggregationNode, StatsProvider statsProvider, Session session, TypeProvider types)
    {
        // We assume here that due to predicate push-down all the filters left are on the aggregation result
        PlanNodeStatsEstimate filteredStats = filterStatsCalculator.filterStats(statsProvider.getStats(filterNode.getSource()), castToExpression(filterNode.getPredicate()), session, types);
        if (filteredStats.isOutputRowCountUnknown()) {
            PlanNodeStatsEstimate sourceStats = statsProvider.getStats(aggregationNode);
            if (sourceStats.isOutputRowCountUnknown()) {
                return Optional.of(filteredStats);
            }
            return Optional.of(sourceStats.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT));
        }
        return Optional.of(filteredStats);
    }

    private static PlanNode resolveGroup(Lookup lookup, PlanNode node)
    {
        if (node instanceof GroupReference) {
            return lookup.resolveGroup(node).collect(onlyElement());
        }
        return node;
    }
}
