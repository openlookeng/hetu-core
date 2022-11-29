package io.prestosql.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.prestosql.cache.CachedDataStorageProvider;
import io.prestosql.cache.elements.CachedDataStorage;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CacheTableFinishNode
    extends InternalPlanNode
{

    private final PlanNode source;
    private final TableWriterNode.WriterTarget target;
    private final Symbol rowCountSymbol;
    private final Optional<StatisticAggregationsDescriptor<Symbol>> statisticsAggregationDescriptor;
    private final Optional<CachedDataStorage> cachedDataStorage;

    @JsonCreator
    public CacheTableFinishNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") TableWriterNode.WriterTarget target,
            @JsonProperty("rowCountSymbol") Symbol rowCountSymbol,
            @JsonProperty("statisticsAggregationDescriptor") Optional<StatisticAggregationsDescriptor<Symbol>> statisticsAggregationDescriptor)
    {
        this(id, source, target, rowCountSymbol, statisticsAggregationDescriptor, Optional.empty());
    }

    public CacheTableFinishNode(PlanNodeId id, PlanNode source,
                                TableWriterNode.WriterTarget target, Symbol rowCountSymbol,
                                Optional<StatisticAggregationsDescriptor<Symbol>> statisticsAggregationDescriptor,
                                Optional<CachedDataStorage> cachedDataStorage)
    {
        super(id);
        checkArgument(target != null || source instanceof TableWriterNode);

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.rowCountSymbol = requireNonNull(rowCountSymbol, "rowCountSymbol is null");
        this.statisticsAggregationDescriptor = requireNonNull(statisticsAggregationDescriptor, "statisticsAggregationDescriptor is null");
        this.cachedDataStorage = requireNonNull(cachedDataStorage, "cachedDataStorage is null");
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public TableWriterNode.WriterTarget getTarget()
    {
        return target;
    }

    @JsonProperty
    public Symbol getRowCountSymbol()
    {
        return rowCountSymbol;
    }

    @JsonProperty
    public Optional<StatisticAggregationsDescriptor<Symbol>> getStatisticsAggregationDescriptor()
    {
        return statisticsAggregationDescriptor;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(source.getOutputSymbols());
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitCacheTableFinish(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new CacheTableFinishNode(
                getId(),
                Iterables.getOnlyElement(newChildren),
                target,
                rowCountSymbol,
                statisticsAggregationDescriptor,
                cachedDataStorage);
    }

    public Optional<CachedDataStorage> getCacheDataStorage()
    {
        return cachedDataStorage;
    }
}
