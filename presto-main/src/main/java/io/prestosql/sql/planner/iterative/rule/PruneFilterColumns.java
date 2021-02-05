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
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;

public class PruneFilterColumns
        extends ProjectOffPushDownRule<FilterNode>
{
    public PruneFilterColumns()
    {
        super(filter());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, FilterNode filterNode, Set<Symbol> referencedOutputs)
    {
        Set<Symbol> prunedFilterInputs = Streams.concat(
                referencedOutputs.stream(),
                SymbolsExtractor.extractUnique(castToExpression(filterNode.getPredicate())).stream())
                .collect(toImmutableSet());

        return restrictChildOutputs(idAllocator, filterNode, prunedFilterInputs);
    }
}
