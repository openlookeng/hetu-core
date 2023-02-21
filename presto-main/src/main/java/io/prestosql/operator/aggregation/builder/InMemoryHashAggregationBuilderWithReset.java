package io.prestosql.operator.aggregation.builder;

import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.UpdateMemory;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.List;
import java.util.Optional;

import static io.prestosql.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.prestosql.operator.GroupByHash.createGroupByHash;

public class InMemoryHashAggregationBuilderWithReset
        extends InMemoryHashAggregationBuilder
{
    private final List<AccumulatorFactory> accumulatorFactoriesm;
    private final AggregationNode.Step step;
    private final int expectedGroups;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final Optional<Integer> hashChannel;
    private final OperatorContext operatorContext;
    private final Optional<DataSize> maxPartialMemory;
    private final JoinCompiler joinCompiler;

    public InMemoryHashAggregationBuilderWithReset(
            List<AccumulatorFactory> accumulatorFactories,
            AggregationNode.Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory)
    {
        super(accumulatorFactories,
                AggregationNode.Step.partialInput(step),
                expectedGroups,
                groupByTypes,
                groupByChannels,
                hashChannel,
                isDictionaryAggregationEnabled(operatorContext.getSession()),
                maxPartialMemory,
                Optional.empty(),
                joinCompiler,
                updateMemory,
                AggregationNode.AggregationType.HASH);
        this.accumulatorFactoriesm = accumulatorFactories;
        this.step = AggregationNode.Step.partialInput(step);
        this.expectedGroups = expectedGroups;
        this.groupByTypes = groupByTypes;
        this.groupByChannels = groupByChannels;
        this.hashChannel = hashChannel;
        this.operatorContext = operatorContext;
        this.maxPartialMemory = maxPartialMemory;
        this.joinCompiler = joinCompiler;
    }

    @Override
    protected void resetGroupBy()
    {
        IntIterator intIterator = consecutiveGroupIds();
        while (intIterator.hasNext()) {
            int groupId = intIterator.nextInt();
            for (Aggregator aggregator : aggregators) {
                aggregator.reset(groupId);
            }
        }

        this.groupBy = createGroupByHash(
                groupByTypes,
                Ints.toArray(groupByChannels),
                hashChannel,
                expectedGroups,
                isDictionaryAggregationEnabled(operatorContext.getSession()),
                joinCompiler,
                updateMemory);
    }

    @Override
    public int getAggregationCount()
    {
        return accumulatorFactoriesm.size();
    }

    @Override
    public AggregationBuilder duplicate()
    {
        return new InMemoryHashAggregationBuilderWithReset(
                accumulatorFactoriesm,
                step,
                expectedGroups,
                groupByTypes,
                groupByChannels,
                hashChannel,
                operatorContext,
                maxPartialMemory,
                joinCompiler,
                updateMemory);
    }
}
