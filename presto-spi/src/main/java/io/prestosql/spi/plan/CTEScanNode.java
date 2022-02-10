/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.spi.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.relation.RowExpression;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public class CTEScanNode
        extends PlanNode
{
    private PlanNode source;
    private List<Symbol> outputSymbols;
    private Optional<RowExpression> predicate;
    private Set<PlanNodeId> consumerPlans;
    private final String cteRefName;
    private final Integer commonCTERefNum;

    @JsonCreator
    public CTEScanNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("outputSymbols") List<Symbol> outputs,
            @JsonProperty("predicate") Optional<RowExpression> predicate,
            @JsonProperty("cteRefName") String cteRefName,
            @JsonProperty("consumerPlans") Set<PlanNodeId> planNodeIds,
            @JsonProperty("commonCTERefNum") Integer commonCTERefNum)
    {
        super(id);
        this.outputSymbols = new ArrayList<>(outputs);
        this.predicate = predicate;
        this.source = source;
        this.consumerPlans = planNodeIds;
        this.cteRefName = cteRefName;
        this.commonCTERefNum = commonCTERefNum;
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    public void setOutputSymbols(List<Symbol> outputSymbols)
    {
        this.outputSymbols = outputSymbols;
    }

    @JsonProperty
    public Optional<RowExpression> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public void setPredicate(Optional<RowExpression> predicate)
    {
        this.predicate = predicate;
    }

    @JsonProperty
    public Set<PlanNodeId> getConsumerPlans()
    {
        return consumerPlans;
    }

    @JsonProperty
    public String getCteRefName()
    {
        return cteRefName;
    }

    public void updateConsumerPlans(Set<PlanNodeId> planNodeId)
    {
        consumerPlans.addAll(planNodeId);
    }

    public void updateConsumerPlan(PlanNodeId planNodeId)
    {
        consumerPlans.add(planNodeId);
    }

    @JsonProperty
    public Integer getCommonCTERefNum()
    {
        return commonCTERefNum;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "expected newChildren to contain 1 node");
        return new CTEScanNode(getId(), newChildren.get(0), outputSymbols, predicate, cteRefName, consumerPlans, commonCTERefNum);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitCTEScan(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", source)
                .add("outputSymbols", outputSymbols)
                .add("predicate", predicate)
                .add("consumerPlans", consumerPlans)
                .toString();
    }

    public static boolean isNotCTEScanNode(PlanNode node)
    {
        return !(node instanceof CTEScanNode);
    }
}
