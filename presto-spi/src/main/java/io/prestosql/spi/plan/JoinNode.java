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
import io.prestosql.spi.relation.RowExpression;

import javax.annotation.concurrent.Immutable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.plan.JoinNode.DistributionType.PARTITIONED;
import static io.prestosql.spi.plan.JoinNode.DistributionType.REPLICATED;
import static io.prestosql.spi.plan.JoinNode.Type.RIGHT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Immutable
public class JoinNode
        extends PlanNode
{
    private final Type type;
    private final PlanNode left;
    private final PlanNode right;
    private final List<EquiJoinClause> criteria;
    private final List<Symbol> outputSymbols;
    private final Optional<RowExpression> filter;
    private final Optional<Symbol> leftHashSymbol;
    private final Optional<Symbol> rightHashSymbol;
    private final Optional<DistributionType> distributionType;
    private final Optional<Boolean> spillable;
    private final Map<String, Symbol> dynamicFilters;

    @JsonCreator
    public JoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("criteria") List<EquiJoinClause> criteria,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols,
            @JsonProperty("filter") Optional<RowExpression> filter,
            @JsonProperty("leftHashSymbol") Optional<Symbol> leftHashSymbol,
            @JsonProperty("rightHashSymbol") Optional<Symbol> rightHashSymbol,
            @JsonProperty("distributionType") Optional<DistributionType> distributionType,
            @JsonProperty("spillable") Optional<Boolean> spillable,
            @JsonProperty("dynamicFilters") Map<String, Symbol> dynamicFilters)
    {
        super(id);
        requireNonNull(type, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(criteria, "criteria is null");
        requireNonNull(outputSymbols, "outputSymbols is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(leftHashSymbol, "leftHashSymbol is null");
        requireNonNull(rightHashSymbol, "rightHashSymbol is null");
        requireNonNull(distributionType, "distributionType is null");
        requireNonNull(spillable, "spillable is null");

        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = ImmutableList.copyOf(criteria);
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);
        this.filter = filter;
        this.leftHashSymbol = leftHashSymbol;
        this.rightHashSymbol = rightHashSymbol;
        this.distributionType = distributionType;
        this.spillable = spillable;
        this.dynamicFilters = ImmutableMap.copyOf(requireNonNull(dynamicFilters, "dynamicFilters is null"));

        Set<Symbol> inputSymbols = ImmutableSet.<Symbol>builder()
                .addAll(left.getOutputSymbols())
                .addAll(right.getOutputSymbols())
                .build();
        checkArgument(new HashSet<>(inputSymbols).containsAll(outputSymbols), "Left and right join inputs do not contain all output symbols");
        checkArgument(!isCrossJoin() || inputSymbols.size() == outputSymbols.size(), "Cross join does not support output symbols pruning or reordering");

        checkArgument(!(criteria.isEmpty() && leftHashSymbol.isPresent()), "Left hash symbol is only valid in an equijoin");
        checkArgument(!(criteria.isEmpty() && rightHashSymbol.isPresent()), "Right hash symbol is only valid in an equijoin");

        if (distributionType.isPresent()) {
            // The implementation of full outer join only works if the data is hash partitioned.
            checkArgument(
                    !(distributionType.get() == REPLICATED && (type == RIGHT || type == Type.FULL)),
                    "%s join do not work with %s distribution type",
                    type,
                    distributionType.get());
            // It does not make sense to PARTITION when there is nothing to partition on
            checkArgument(
                    !(distributionType.get() == PARTITIONED && criteria.isEmpty() && type != RIGHT && type != Type.FULL),
                    "Equi criteria are empty, so %s join should not have %s distribution type",
                    type,
                    distributionType.get());
        }

        for (Symbol symbol : dynamicFilters.values()) {
            checkArgument(right.getOutputSymbols().contains(symbol), "Right join input doesn't contain symbol for dynamic filter: %s", symbol);
        }
    }

    public JoinNode flipChildren()
    {
        return new JoinNode(
                getId(),
                flipType(type),
                right,
                left,
                flipJoinCriteria(criteria),
                flipOutputSymbols(getOutputSymbols(), left, right),
                filter,
                rightHashSymbol,
                leftHashSymbol,
                distributionType,
                spillable,
                ImmutableMap.of()); // dynamicFilters are invalid after flipping children
    }

    private static Type flipType(Type type)
    {
        switch (type) {
            case INNER:
                return Type.INNER;
            case FULL:
                return Type.FULL;
            case LEFT:
                return RIGHT;
            case RIGHT:
                return Type.LEFT;
            default:
                throw new IllegalStateException("No inverse defined for join type: " + type);
        }
    }

    private static List<EquiJoinClause> flipJoinCriteria(List<EquiJoinClause> joinCriteria)
    {
        return joinCriteria.stream()
                .map(EquiJoinClause::flip)
                .collect(toImmutableList());
    }

    private static List<Symbol> flipOutputSymbols(List<Symbol> outputSymbols, PlanNode left, PlanNode right)
    {
        List<Symbol> leftSymbols = outputSymbols.stream()
                .filter(symbol -> left.getOutputSymbols().contains(symbol))
                .collect(Collectors.toList());
        List<Symbol> rightSymbols = outputSymbols.stream()
                .filter(symbol -> right.getOutputSymbols().contains(symbol))
                .collect(Collectors.toList());
        return ImmutableList.<Symbol>builder()
                .addAll(rightSymbols)
                .addAll(leftSymbols)
                .build();
    }

    public JoinNode withoutDynamicFilters()
    {
        return new JoinNode(getId(), type, left, right, criteria, outputSymbols, filter, leftHashSymbol, rightHashSymbol, distributionType, spillable, ImmutableMap.of());
    }

    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED
    }

    public enum Type
    {
        INNER("InnerJoin"),
        LEFT("LeftJoin"),
        RIGHT("RightJoin"),
        FULL("FullJoin");

        private final String joinLabel;

        Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }

        public boolean mustPartition()
        {
            // With REPLICATED, the unmatched rows from right-side would be duplicated.
            return this == RIGHT || this == FULL;
        }

        public boolean mustReplicate(List<JoinNode.EquiJoinClause> criteria)
        {
            // There is nothing to partition on
            return criteria.isEmpty() && (this == INNER || this == LEFT);
        }
    }

    @JsonProperty("type")
    public Type getType()
    {
        return type;
    }

    @JsonProperty("left")
    public PlanNode getLeft()
    {
        return left;
    }

    @JsonProperty("right")
    public PlanNode getRight()
    {
        return right;
    }

    @JsonProperty("criteria")
    public List<EquiJoinClause> getCriteria()
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
        return ImmutableSet.copyOf(right.getOutputSymbols());
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
        return ImmutableList.of(left, right);
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty("distributionType")
    public Optional<DistributionType> getDistributionType()
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

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new JoinNode(getId(), type, newChildren.get(0), newChildren.get(1), criteria, outputSymbols, filter, leftHashSymbol, rightHashSymbol, distributionType, spillable, dynamicFilters);
    }

    public JoinNode withDistributionType(DistributionType distributionType)
    {
        return new JoinNode(getId(), type, left, right, criteria, outputSymbols, filter, leftHashSymbol, rightHashSymbol, Optional.of(distributionType), spillable, dynamicFilters);
    }

    public JoinNode withSpillable(boolean spillable)
    {
        return new JoinNode(getId(), type, left, right, criteria, outputSymbols, filter, leftHashSymbol, rightHashSymbol, distributionType, Optional.of(spillable), dynamicFilters);
    }

    public boolean isCrossJoin()
    {
        return criteria.isEmpty() && !filter.isPresent() && type == Type.INNER;
    }

    public static class EquiJoinClause
    {
        private final Symbol left;
        private final Symbol right;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("left") Symbol left, @JsonProperty("right") Symbol right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        @JsonProperty("left")
        public Symbol getLeft()
        {
            return left;
        }

        @JsonProperty("right")
        public Symbol getRight()
        {
            return right;
        }

        public EquiJoinClause flip()
        {
            return new EquiJoinClause(right, left);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || !this.getClass().equals(obj.getClass())) {
                return false;
            }

            EquiJoinClause other = (EquiJoinClause) obj;

            return Objects.equals(this.left, other.left) &&
                    Objects.equals(this.right, other.right);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(left, right);
        }

        @Override
        public String toString()
        {
            return format("%s = %s", left, right);
        }
    }
}
