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
package io.prestosql.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.prestosql.operator.window.RankingFunction;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.WindowNode.Specification;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.concat;
import static java.util.Objects.requireNonNull;

@Immutable
public final class TopNRankingNumberNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final Specification specification;
    private final Symbol rowNumberSymbol;
    private final int maxRowCountPerPartition;
    private final boolean partial;
    private final Optional<Symbol> hashSymbol;
    private final Optional<RankingFunction> rankingFunction;

    @JsonCreator
    public TopNRankingNumberNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("specification") Specification specification,
            @JsonProperty("rowNumberSymbol") Symbol rowNumberSymbol,
            @JsonProperty("maxRowCountPerPartition") int maxRowCountPerPartition,
            @JsonProperty("partial") boolean partial,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("rankingFunction") Optional<RankingFunction> rankingFunction)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(specification, "specification is null");
        checkArgument(specification.getOrderingScheme().isPresent(), "specification orderingScheme is absent");
        requireNonNull(rowNumberSymbol, "rowNumberSymbol is null");
        checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition must be > 0");
        requireNonNull(hashSymbol, "hashSymbol is null");
        requireNonNull(rankingFunction, "rankingFunction is null");

        this.source = source;
        this.specification = specification;
        this.rowNumberSymbol = rowNumberSymbol;
        this.maxRowCountPerPartition = maxRowCountPerPartition;
        this.partial = partial;
        this.hashSymbol = hashSymbol;
        this.rankingFunction = rankingFunction;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        if (!partial) {
            return ImmutableList.copyOf(concat(source.getOutputSymbols(), ImmutableList.of(rowNumberSymbol)));
        }
        return ImmutableList.copyOf(source.getOutputSymbols());
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public Specification getSpecification()
    {
        return specification;
    }

    public List<Symbol> getPartitionBy()
    {
        return specification.getPartitionBy();
    }

    public OrderingScheme getOrderingScheme()
    {
        return specification.getOrderingScheme().get();
    }

    @JsonProperty
    public Symbol getRowNumberSymbol()
    {
        return rowNumberSymbol;
    }

    @JsonProperty
    public Optional<RankingFunction> getRankingFunction()
    {
        return this.rankingFunction;
    }

    @JsonProperty
    public int getMaxRowCountPerPartition()
    {
        return maxRowCountPerPartition;
    }

    @JsonProperty
    public boolean isPartial()
    {
        return partial;
    }

    @JsonProperty
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTopNRankingNumber(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new TopNRankingNumberNode(getId(), Iterables.getOnlyElement(newChildren), specification, rowNumberSymbol, maxRowCountPerPartition, partial, hashSymbol, rankingFunction);
    }
}
