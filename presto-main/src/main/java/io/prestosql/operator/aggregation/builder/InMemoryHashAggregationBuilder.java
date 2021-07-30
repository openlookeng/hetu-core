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
package io.prestosql.operator.aggregation.builder;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.array.IntBigArray;
import io.prestosql.operator.HashCollisionsCounter;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.UpdateMemory;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Step;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.prestosql.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.prestosql.spi.type.BigintType.BIGINT;

public class InMemoryHashAggregationBuilder
        extends InMemoryAggregationBuilder
{
    public InMemoryHashAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory)
    {
        this(accumulatorFactories,
                step,
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
    }

    public InMemoryHashAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            Optional<Integer> overwriteIntermediateChannelOffset,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory)
    {
        this(accumulatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                groupByChannels,
                hashChannel,
                isDictionaryAggregationEnabled(operatorContext.getSession()),
                maxPartialMemory,
                overwriteIntermediateChannelOffset,
                joinCompiler,
                updateMemory,
                AggregationNode.AggregationType.HASH);
    }

    public InMemoryHashAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            boolean processDictionary,
            Optional<DataSize> maxPartialMemory,
            Optional<Integer> overwriteIntermediateChannelOffset,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory,
            AggregationNode.AggregationType aggregationType)
    {
        super(accumulatorFactories, step, expectedGroups, groupByTypes, groupByChannels, hashChannel, processDictionary,
                maxPartialMemory, overwriteIntermediateChannelOffset, joinCompiler, updateMemory, aggregationType);
    }

    public long getHashCollisions()
    {
        return groupBy.getHashCollisions();
    }

    public double getExpectedHashCollisions()
    {
        return groupBy.getExpectedHashCollisions();
    }

    @Override
    public void recordHashCollisions(HashCollisionsCounter hashCollisionsCounter)
    {
        hashCollisionsCounter.recordHashCollision(groupBy.getHashCollisions(), groupBy.getExpectedHashCollisions());
    }

    public static List<Type> toTypes(List<? extends Type> groupByType, Step step, List<AccumulatorFactory> factories, Optional<Integer> hashChannel)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        types.addAll(groupByType);
        if (hashChannel.isPresent()) {
            types.add(BIGINT);
        }
        for (AccumulatorFactory factory : factories) {
            types.add(new Aggregator(factory, step, Optional.empty()).getType());
        }
        return types.build();
    }

    @Override
    public WorkProcessor<Page> buildResult()
    {
        for (Aggregator aggregator : aggregators) {
            aggregator.prepareFinal();
        }
        return buildResult(consecutiveGroupIds());
    }

    public WorkProcessor<Page> buildHashSortedResult()
    {
        return buildResult(hashSortedGroupIds());
    }

    protected WorkProcessor<Page> buildResult(IntIterator groupIds)
    {
        final PageBuilder pageBuilder = new PageBuilder(buildTypes());
        return WorkProcessor.create(() -> {
            if (!groupIds.hasNext()) {
                return WorkProcessor.ProcessState.finished();
            }

            pageBuilder.reset();

            List<Type> types = groupBy.getTypes();
            while (!pageBuilder.isFull() && groupIds.hasNext()) {
                int groupId = groupIds.nextInt();

                groupBy.appendValuesTo(groupId, pageBuilder, 0);

                pageBuilder.declarePosition();
                for (int i = 0; i < aggregators.size(); i++) {
                    Aggregator aggregator = aggregators.get(i);
                    BlockBuilder output = pageBuilder.getBlockBuilder(types.size() + i);
                    aggregator.evaluate(groupId, output);
                }
            }

            return WorkProcessor.ProcessState.ofResult(pageBuilder.build());
        });
    }

    protected List<Type> buildTypes()
    {
        ArrayList<Type> types = new ArrayList<>(groupBy.getTypes());
        for (Aggregator aggregator : aggregators) {
            types.add(aggregator.getType());
        }
        return types;
    }

    private IntIterator hashSortedGroupIds()
    {
        IntBigArray groupIds = new IntBigArray();
        groupIds.ensureCapacity(groupBy.getGroupCount());
        for (int i = 0; i < groupBy.getGroupCount(); i++) {
            groupIds.set(i, i);
        }

        groupIds.sort(0, groupBy.getGroupCount(), (leftGroupId, rightGroupId) ->
                Long.compare(groupBy.getRawHash(leftGroupId), groupBy.getRawHash(rightGroupId)));

        return new AbstractIntIterator()
        {
            private final int totalPositions = groupBy.getGroupCount();
            private int position;

            @Override
            public boolean hasNext()
            {
                return position < totalPositions;
            }

            @Override
            public int nextInt()
            {
                return groupIds.get(position++);
            }
        };
    }
}
