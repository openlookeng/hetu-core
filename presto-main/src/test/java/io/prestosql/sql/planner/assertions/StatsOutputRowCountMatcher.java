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

import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.PlanNode;

public class StatsOutputRowCountMatcher
        implements Matcher
{
    private final double expectedOutputRowCount;

    StatsOutputRowCountMatcher(double expectedOutputRowCount)
    {
        this.expectedOutputRowCount = expectedOutputRowCount;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return true;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        return new MatchResult(Double.compare(stats.getStats(node).getOutputRowCount(), expectedOutputRowCount) == 0);
    }

    @Override
    public String toString()
    {
        return "expectedOutputRowCount(" + expectedOutputRowCount + ")";
    }
}
