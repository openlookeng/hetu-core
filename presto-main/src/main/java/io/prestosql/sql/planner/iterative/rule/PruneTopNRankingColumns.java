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

import com.google.common.collect.Streams;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.plan.TopNRankingNumberNode;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.prestosql.sql.planner.plan.Patterns.topNRankingNumber;

public class PruneTopNRankingColumns
        extends ProjectOffPushDownRule<TopNRankingNumberNode>
{
    public PruneTopNRankingColumns()
    {
        super(topNRankingNumber());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, TopNRankingNumberNode topNRankingNode, Set<Symbol> referencedOutputs)
    {
        Set<Symbol> requiredInputs = Streams.concat(
                referencedOutputs.stream()
                        .filter(symbol -> !symbol.equals(topNRankingNode.getRowNumberSymbol())),
                topNRankingNode.getPartitionBy().stream(),
                topNRankingNode.getOrderingScheme().getOrderBy().stream(),
                topNRankingNode.getHashSymbol().isPresent() ? Stream.of(topNRankingNode.getHashSymbol().get()) : Stream.empty())
                .collect(toImmutableSet());

        return restrictChildOutputs(idAllocator, topNRankingNode, requiredInputs);
    }
}
