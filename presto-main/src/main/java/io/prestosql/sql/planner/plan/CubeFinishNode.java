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
import io.prestosql.spi.cube.CubeUpdateMetadata;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.type.Type;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

@Immutable
public class CubeFinishNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final Symbol rowCountSymbol;
    private final CubeUpdateMetadata metadata;
    private final Map<Symbol, Type> predicateColumnsType;

    @JsonCreator
    public CubeFinishNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("rowCountSymbol") Symbol rowCountSymbol,
            @JsonProperty("metadata") CubeUpdateMetadata metadata,
            @JsonProperty("predicateColumnsType") Map<Symbol, Type> predicateColumnsType)
    {
        super(id);
        this.source = source;
        this.rowCountSymbol = rowCountSymbol;
        this.metadata = metadata;
        this.predicateColumnsType = predicateColumnsType;
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public Symbol getRowCountSymbol()
    {
        return rowCountSymbol;
    }

    @JsonProperty
    public CubeUpdateMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.of(rowCountSymbol);
    }

    @JsonProperty
    public Map<Symbol, Type> getPredicateColumnsType()
    {
        return this.predicateColumnsType;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitCubeFinish(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new CubeFinishNode(
                getId(),
                Iterables.getOnlyElement(newChildren),
                rowCountSymbol,
                metadata,
                predicateColumnsType);
    }
}
