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

package io.prestosql.spi.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.prestosql.spi.relation.RowExpression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.spi.plan.JoinNode.DistributionType.PARTITIONED;
import static io.prestosql.spi.plan.JoinNode.DistributionType.REPLICATED;
import static io.prestosql.spi.plan.JoinNode.Type.RIGHT;
import static java.util.Objects.requireNonNull;

public class JoinOnAggregationNode
        extends PlanNode
{
    private final JoinNode.Type type;
    private final List<JoinNode.EquiJoinClause> criteria;
    private final Optional<RowExpression> filter;
    private final Optional<Symbol> leftHashSymbol;
    private final Optional<Symbol> rightHashSymbol;
    private final Optional<JoinNode.DistributionType> distributionType;
    private final Optional<Boolean> spillable;
    private final Map<String, Symbol> dynamicFilters;

    private final JoinInternalAggregation leftAggr;
    private final JoinInternalAggregation rightAggr;

    private final JoinInternalAggregation aggrOnAggrLeft;
    private final JoinInternalAggregation aggrOnAggrRight;

    private final List<Symbol> outputSymbols;

    @JsonCreator
    public JoinOnAggregationNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") JoinNode.Type type,
            @JsonProperty("criteria") List<JoinNode.EquiJoinClause> criteria,
            @JsonProperty("filter") Optional<RowExpression> filter,
            @JsonProperty("leftHashSymbol") Optional<Symbol> leftHashSymbol,
            @JsonProperty("rightHashSymbol") Optional<Symbol> rightHashSymbol,
            @JsonProperty("distributionType") Optional<JoinNode.DistributionType> distributionType,
            @JsonProperty("spillable") Optional<Boolean> spillable,
            @JsonProperty("dynamicFilters") Map<String, Symbol> dynamicFilters,
            @JsonProperty("leftAggr") JoinInternalAggregation leftAggr,
            @JsonProperty("rightAggr") JoinInternalAggregation rightAggr,
            @JsonProperty("aggrOnAggrLeft") JoinInternalAggregation aggrOnAggrLeft,
            @JsonProperty("aggrOnAggrRight") JoinInternalAggregation aggrOnAggrRight,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols)
    {
        super(id);
        requireNonNull(type, "type is null");
        requireNonNull(criteria, "criteria is null");
        requireNonNull(outputSymbols, "outputSymbols is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(leftHashSymbol, "leftHashSymbol is null");
        requireNonNull(rightHashSymbol, "rightHashSymbol is null");
        requireNonNull(distributionType, "distributionType is null");
        requireNonNull(spillable, "spillable is null");

        this.type = type;
        this.criteria = ImmutableList.copyOf(criteria);
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);
        this.filter = filter;
        this.leftHashSymbol = leftHashSymbol;
        this.rightHashSymbol = rightHashSymbol;
        this.distributionType = distributionType;
        this.spillable = spillable;
        this.dynamicFilters = ImmutableMap.copyOf(requireNonNull(dynamicFilters, "dynamicFilters is null"));

        checkArgument(!(criteria.isEmpty() && leftHashSymbol.isPresent()), "Left hash symbol is only valid in an equijoin");
        checkArgument(!(criteria.isEmpty() && rightHashSymbol.isPresent()), "Right hash symbol is only valid in an equijoin");

        if (distributionType.isPresent()) {
            // The implementation of full outer join only works if the data is hash partitioned.
            checkArgument(
                    !(distributionType.get() == REPLICATED && (type == RIGHT || type == JoinNode.Type.FULL)),
                    "%s join do not work with %s distribution type",
                    type,
                    distributionType.get());
            // It does not make sense to PARTITION when there is nothing to partition on
            checkArgument(
                    !(distributionType.get() == PARTITIONED && criteria.isEmpty() && type != RIGHT && type != JoinNode.Type.FULL),
                    "Equi criteria are empty, so %s join should not have %s distribution type",
                    type,
                    distributionType.get());
        }

        for (Symbol symbol : dynamicFilters.values()) {
            checkArgument(rightAggr.getSource().getOutputSymbols().contains(symbol), "Right join input doesn't contain symbol for dynamic filter: %s", symbol);
        }

        this.leftAggr = leftAggr;
        this.rightAggr = rightAggr;
        this.aggrOnAggrLeft = aggrOnAggrLeft;
        this.aggrOnAggrRight = aggrOnAggrRight;
    }

    @JsonProperty("type")
    public JoinNode.Type getType()
    {
        return type;
    }

    public PlanNode getLeft()
    {
        return leftAggr.getSource();
    }

    public PlanNode getRight()
    {
        return rightAggr.getSource();
    }

    @JsonProperty("criteria")
    public List<JoinNode.EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty("filter")
    public Optional<RowExpression> getFilter()
    {
        return filter;
    }

    public Set<Symbol> getRightOutputSymbols()
    {
        return ImmutableSet.copyOf(rightAggr.getSource().getOutputSymbols());
    }

    @JsonProperty("leftHashSymbol")
    public Optional<Symbol> getLeftHashSymbol()
    {
        return leftHashSymbol;
    }

    @JsonProperty("rightHashSymbol")
    public Optional<Symbol> getRightHashSymbol()
    {
        return rightHashSymbol;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(leftAggr.getSource(), rightAggr.getSource());
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty("distributionType")
    public Optional<JoinNode.DistributionType> getDistributionType()
    {
        return distributionType;
    }

    @JsonProperty("spillable")
    public Optional<Boolean> isSpillable()
    {
        return spillable;
    }

    @JsonProperty
    public Map<String, Symbol> getDynamicFilters()
    {
        return dynamicFilters;
    }

    @JsonProperty("leftAggr")
    public JoinInternalAggregation getLeftAggr()
    {
        return leftAggr;
    }

    @JsonProperty("rightAggr")
    public JoinInternalAggregation getRightAggr()
    {
        return rightAggr;
    }

    @JsonProperty("aggrOnAggrLeft")
    public JoinInternalAggregation getAggrOnLeft()
    {
        return aggrOnAggrLeft;
    }

    @JsonProperty("aggrOnAggrRight")
    public JoinInternalAggregation getAggrOnRight()
    {
        return aggrOnAggrRight;
    }

    public List<Symbol> getAllSymbols()
    {
        return Streams.concat(leftAggr.getAllSymbols().stream(), rightAggr.getAllSymbols().stream(),
                        aggrOnAggrLeft.getAllSymbols().stream(), aggrOnAggrRight.getAllSymbols().stream())
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        JoinInternalAggregation leftAggrLocal = (JoinInternalAggregation) this.leftAggr.replaceChildren(newChildren.subList(0, 1));
        JoinInternalAggregation rightAggrLocal = (JoinInternalAggregation) this.rightAggr.replaceChildren(newChildren.subList(1, 2));
        JoinInternalAggregation aggrOnAggrLeftLocal = (JoinInternalAggregation) this.aggrOnAggrLeft.replaceChildren(Collections.singletonList(leftAggrLocal));
        JoinInternalAggregation aggrOnAggrRightLocal = (JoinInternalAggregation) this.aggrOnAggrRight.replaceChildren(Collections.singletonList(rightAggrLocal));

        return new JoinOnAggregationNode(getId(),
                type,
                criteria,
                filter,
                leftHashSymbol,
                rightHashSymbol,
                distributionType,
                spillable,
                dynamicFilters,
                leftAggrLocal,
                rightAggrLocal,
                aggrOnAggrLeftLocal,
                aggrOnAggrRightLocal,
                outputSymbols);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitJoinOnAggregation(this, context);
    }

    public static class JoinInternalAggregation
            extends PlanNode
    {
        private final PlanNode source;
        private final Map<Symbol, AggregationNode.Aggregation> aggregations;
        private final AggregationNode.GroupingSetDescriptor groupingSets;
        private final List<Symbol> preGroupedSymbols;
        private final AggregationNode.Step step;
        private final Optional<Symbol> hashSymbol;
        private final Optional<Symbol> groupIdSymbol;
        private final List<Symbol> outputs;
        private AggregationNode.AggregationType aggregationType;
        private Optional<Symbol> finalizeSymbol;

        @JsonCreator
        public JoinInternalAggregation(
                @JsonProperty("id") PlanNodeId id,
                @JsonProperty("source") PlanNode source,
                @JsonProperty("aggregations") Map<Symbol, AggregationNode.Aggregation> aggregations,
                @JsonProperty("groupingSets") AggregationNode.GroupingSetDescriptor groupingSets,
                @JsonProperty("preGroupedSymbols") List<Symbol> preGroupedSymbols,
                @JsonProperty("step") AggregationNode.Step step,
                @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
                @JsonProperty("groupIdSymbol") Optional<Symbol> groupIdSymbol,
                @JsonProperty("aggregationType") AggregationNode.AggregationType aggregationType,
                @JsonProperty("finalizeSymbol") Optional<Symbol> finalizeSymbol)
        {
            super(id);
            this.source = source;
            this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));

            requireNonNull(groupingSets, "groupingSets is null");
            groupIdSymbol.ifPresent(symbol -> checkArgument(groupingSets.getGroupingKeys().contains(symbol), "Grouping columns does not contain groupId column"));
            this.groupingSets = groupingSets;

            this.groupIdSymbol = requireNonNull(groupIdSymbol);

            boolean noOrderBy = aggregations.values().stream()
                    .map(AggregationNode.Aggregation::getOrderingScheme)
                    .noneMatch(Optional::isPresent);
            checkArgument(noOrderBy || step == SINGLE, "ORDER BY does not support distributed aggregation");

            this.step = step;
            this.hashSymbol = hashSymbol;

            requireNonNull(preGroupedSymbols, "preGroupedSymbols is null");
            checkArgument(preGroupedSymbols.isEmpty() || groupingSets.getGroupingKeys().containsAll(preGroupedSymbols), "Pre-grouped symbols must be a subset of the grouping keys");
            this.preGroupedSymbols = ImmutableList.copyOf(preGroupedSymbols);

            ImmutableList.Builder<Symbol> outputs1 = ImmutableList.builder();
            outputs1.addAll(groupingSets.getGroupingKeys());
            hashSymbol.ifPresent(outputs1::add);
            outputs1.addAll(aggregations.keySet());

            AggregationNode.AggregationType tmpAggregationType = aggregationType;
            if (tmpAggregationType == AggregationNode.AggregationType.SORT_BASED) {
                if (step.equals(AggregationNode.Step.PARTIAL)) {
                    if (finalizeSymbol.isPresent()) {
                        List<Symbol> symbolList = new ArrayList<>(Arrays.asList(finalizeSymbol.get()));
                        outputs1.addAll(symbolList);
                    }
                }
                else if (step.equals(AggregationNode.Step.FINAL) && !finalizeSymbol.isPresent()) {
                    tmpAggregationType = AggregationNode.AggregationType.HASH;
                }
            }

            this.outputs = outputs1.build();
            this.aggregationType = tmpAggregationType;
            this.finalizeSymbol = finalizeSymbol;
        }

        @JsonProperty("source")
        public PlanNode getSource()
        {
            return source;
        }

        @Override
        public List<PlanNode> getSources()
        {
            return ImmutableList.of(source);
        }

        public List<Symbol> getOutputSymbols()
        {
            return outputs;
        }

        @Override
        public PlanNode replaceChildren(List<PlanNode> newChildren)
        {
            return new JoinInternalAggregation(getId(),
                    newChildren.get(0),
                    aggregations,
                    groupingSets,
                    preGroupedSymbols,
                    step,
                    hashSymbol,
                    groupIdSymbol,
                    aggregationType,
                    finalizeSymbol);
        }

        @JsonProperty("step")
        public AggregationNode.Step getStep()
        {
            return step;
        }

        @JsonProperty("hashSymbol")
        public Optional<Symbol> getHashSymbol()
        {
            return hashSymbol;
        }

        @JsonProperty("groupIdSymbol")
        public Optional<Symbol> getGroupIdSymbol()
        {
            return groupIdSymbol;
        }

        @JsonProperty("aggregations")
        public Map<Symbol, AggregationNode.Aggregation> getAggregations()
        {
            return aggregations;
        }

        @JsonProperty("preGroupedSymbols")
        public List<Symbol> getPreGroupedSymbols()
        {
            return preGroupedSymbols;
        }

        @JsonProperty("groupingSets")
        public AggregationNode.GroupingSetDescriptor getGroupingSets()
        {
            return groupingSets;
        }

        public List<Symbol> getGroupingKeys()
        {
            return groupingSets.getGroupingKeys();
        }

        /**
         * @return whether this node should produce default output in case of no input pages.
         * For example for query:
         * <p>
         * SELECT count(*) FROM nation WHERE nationkey < 0
         * <p>
         * A default output of "0" is expected to be produced by FINAL aggregation operator.
         */
        public boolean hasDefaultOutput()
        {
            return hasEmptyGroupingSet() && (step.isOutputPartial() || step.equals(SINGLE));
        }

        public boolean hasEmptyGroupingSet()
        {
            return !groupingSets.getGlobalGroupingSets().isEmpty();
        }

        public boolean hasNonEmptyGroupingSet()
        {
            return groupingSets.getGroupingSetCount() > groupingSets.getGlobalGroupingSets().size();
        }

        public int getGroupingSetCount()
        {
            return groupingSets.getGroupingSetCount();
        }

        public Set<Integer> getGlobalGroupingSets()
        {
            return groupingSets.getGlobalGroupingSets();
        }

        public boolean hasOrderings()
        {
            return aggregations.values().stream()
                    .map(AggregationNode.Aggregation::getOrderingScheme)
                    .anyMatch(Optional::isPresent);
        }

        @JsonProperty("aggregationType")
        public AggregationNode.AggregationType getAggregationType()
        {
            return aggregationType;
        }

        @JsonProperty("finalizeSymbol")
        public Optional<Symbol> getFinalizeSymbol()
        {
            return finalizeSymbol;
        }
    }
}
