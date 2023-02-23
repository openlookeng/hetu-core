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
    private final List<AccumulatorFactory> accumulatorFactories;
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
        this.accumulatorFactories = accumulatorFactories;
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
        return accumulatorFactories.size();
    }

    @Override
    public AggregationBuilder duplicate()
    {
        return new InMemoryHashAggregationBuilderWithReset(
                accumulatorFactories,
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
