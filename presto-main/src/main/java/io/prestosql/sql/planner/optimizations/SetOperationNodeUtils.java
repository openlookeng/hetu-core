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
package io.prestosql.sql.planner.optimizations;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.prestosql.spi.plan.SetOperationNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.Map;

import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;

public class SetOperationNodeUtils
{
    private SetOperationNodeUtils() {}

    /**
     * Returns the output to input symbol mapping for the given source channel
     */
    public static Map<Symbol, SymbolReference> sourceSymbolMap(SetOperationNode node, int sourceIndex)
    {
        ImmutableMap.Builder<Symbol, SymbolReference> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, Collection<Symbol>> entry : node.getSymbolMapping().asMap().entrySet()) {
            builder.put(entry.getKey(), toSymbolReference(Iterables.get(entry.getValue(), sourceIndex)));
        }

        return builder.build();
    }

    /**
     * Returns the input to output symbol mapping for the given source channel.
     * A single input symbol can map to multiple output symbols, thus requiring a Multimap.
     */
    public static Multimap<Symbol, SymbolReference> outputSymbolMap(SetOperationNode node, int sourceIndex)
    {
        return Multimaps.transformValues(FluentIterable.from(node.getOutputSymbols())
                .toMap(outputToSourceSymbolFunction(node, sourceIndex))
                .asMultimap()
                .inverse(), SymbolUtils::toSymbolReference);
    }

    private static Function<Symbol, Symbol> outputToSourceSymbolFunction(SetOperationNode node, final int sourceIndex)
    {
        return outputSymbol -> node.getSymbolMapping().get(outputSymbol).get(sourceIndex);
    }
}
