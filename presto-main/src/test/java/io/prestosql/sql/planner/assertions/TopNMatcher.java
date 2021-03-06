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
package io.prestosql.sql.planner.assertions;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.TopNNode.Step;
import io.prestosql.sql.planner.assertions.PlanMatchPattern.Ordering;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.prestosql.sql.planner.assertions.MatchResult.match;
import static io.prestosql.sql.planner.assertions.Util.orderingSchemeMatches;
import static java.util.Objects.requireNonNull;

public class TopNMatcher
        implements Matcher
{
    private final long count;
    private final List<Ordering> orderBy;
    private final Step step;

    public TopNMatcher(long count, List<Ordering> orderBy, Step step)
    {
        this.count = count;
        this.orderBy = ImmutableList.copyOf(requireNonNull(orderBy, "orderBy is null"));
        this.step = requireNonNull(step, "step is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof TopNNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        TopNNode topNNode = (TopNNode) node;

        if (topNNode.getCount() != count) {
            return NO_MATCH;
        }

        if (!orderingSchemeMatches(orderBy, topNNode.getOrderingScheme(), symbolAliases)) {
            return NO_MATCH;
        }

        if (topNNode.getStep() != step) {
            return NO_MATCH;
        }

        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("count", count)
                .add("orderBy", orderBy)
                .add("step", step)
                .toString();
    }
}
