package io.prestosql.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.PartitioningScheme;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class CacheTableWriterNode
    extends InternalPlanNode
{
    private final PlanNode source;
    private final TableWriterNode.WriterTarget target;
    private final Symbol rowCountSymbol;
    private final Symbol fragmentSymbol;
    private final List<Symbol> columns;
    private final List<String> columnNames;
    private final Optional<PartitioningScheme> partitioningScheme;
    private final List<Symbol> outputs;

    @JsonCreator
    public CacheTableWriterNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") TableWriterNode.WriterTarget target,
            @JsonProperty("rowCountSymbol") Symbol rowCountSymbol,
            @JsonProperty("fragmentSymbol") Symbol fragmentSymbol,
            @JsonProperty("columns") List<Symbol> columns,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("partitioningScheme") Optional<PartitioningScheme> partitioningScheme)
    {
        super(id);

        requireNonNull(columns, "columns is null");
        requireNonNull(columnNames, "columnNames is null");
        checkArgument(columns.size() == columnNames.size(), "columns and columnNames sizes don't match");

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.rowCountSymbol = requireNonNull(rowCountSymbol, "rowCountSymbol is null");
        this.fragmentSymbol = requireNonNull(fragmentSymbol, "fragmentSymbol is null");
        this.columns = ImmutableList.copyOf(columns);
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.partitioningScheme = requireNonNull(partitioningScheme, "partitioningScheme is null");
        this.outputs = ImmutableList.copyOf(columns);
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
    public Symbol getFragmentSymbol()
    {
        return fragmentSymbol;
    }

    @JsonProperty
    public List<Symbol> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public Optional<PartitioningScheme> getPartitioningScheme()
    {
        return partitioningScheme;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitCacheTableWriter(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new CacheTableWriterNode(getId(), Iterables.getOnlyElement(newChildren), target, rowCountSymbol, fragmentSymbol, columns, columnNames, partitioningScheme);
    }
}
