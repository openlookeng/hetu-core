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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.operator.aggregation.Accumulator;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.partial.PartialAggregationController;
import io.prestosql.operator.scalar.CombineHashFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.operator.aggregation.builder.InMemoryHashAggregationBuilder.toTypes;
import static io.prestosql.sql.planner.optimizations.HashGenerationOptimizer.INITIAL_HASH_VALUE;
import static io.prestosql.type.TypeUtils.NULL_HASH_CODE;
import static java.util.Objects.requireNonNull;

public class GroupJoinAggregator
{
    private final Optional<Integer> hashChannel;
    private final Optional<Integer> groupIdChannel;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final List<Integer> globalAggregationGroupIds;
    private final AggregationNode.Step step;
    private final int expectedGroups;
    private final Optional<DataSize> maxPartialMemory;
    private final JoinCompiler joinCompiler;
    private final boolean useSystemMemory;
    private final Optional<PartialAggregationController> partialAggregationController;
    private final boolean produceDefaultOutput;

    protected final List<Type> types;

    public GroupJoinAggregator(Optional<Integer> hashChannel,
            Optional<Integer> groupIdChannel,
            List<AccumulatorFactory> accumulatorFactories,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            List<Integer> globalAggregationGroupIds,
            AggregationNode.Step step,
            int expectedGroups,
            Optional<DataSize> maxPartialMemory,
            JoinCompiler joinCompiler,
            boolean useSystemMemory,
            Optional<PartialAggregationController> partialAggregationController,
            boolean produceDefaultOutput)
    {
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        checkArgument(!partialAggregationController.isPresent() || step.isOutputPartial(),
                "partialAggregationController should be present only for partial aggregation");
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        this.groupIdChannel = requireNonNull(groupIdChannel, "groupIdChannel is null");
        this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
        this.groupByTypes = ImmutableList.copyOf(groupByTypes);
        this.groupByChannels = ImmutableList.copyOf(groupByChannels);
        this.globalAggregationGroupIds = ImmutableList.copyOf(globalAggregationGroupIds);
        this.step = requireNonNull(step, "step is null");
        this.expectedGroups = expectedGroups;
        this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.useSystemMemory = useSystemMemory;
        this.partialAggregationController = requireNonNull(partialAggregationController, "partialAggregationController is null");
        this.produceDefaultOutput = produceDefaultOutput;
        this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
    }

    public Optional<Integer> getHashChannel()
    {
        return hashChannel;
    }

    public Optional<Integer> getGroupIdChannel()
    {
        return groupIdChannel;
    }

    public List<AccumulatorFactory> getAccumulatorFactories()
    {
        return accumulatorFactories;
    }

    public List<Type> getGroupByTypes()
    {
        return groupByTypes;
    }

    public List<Integer> getGroupByChannels()
    {
        return groupByChannels;
    }

    public List<Integer> getGlobalAggregationGroupIds()
    {
        return globalAggregationGroupIds;
    }

    public AggregationNode.Step getStep()
    {
        return step;
    }

    public int getExpectedGroups()
    {
        return expectedGroups;
    }

    public Optional<DataSize> getMaxPartialMemory()
    {
        return maxPartialMemory;
    }

    public JoinCompiler getJoinCompiler()
    {
        return joinCompiler;
    }

    public boolean isUseSystemMemory()
    {
        return useSystemMemory;
    }

    public Optional<PartialAggregationController> getPartialAggregationController()
    {
        return partialAggregationController;
    }

    public boolean isProduceDefaultOutput()
    {
        return produceDefaultOutput;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    protected Page getGlobalAggregationOutput()
    {
        List<Accumulator> accumulators = accumulatorFactories.stream()
                .map(AccumulatorFactory::createAccumulator)
                .collect(Collectors.toList());

        // global aggregation output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder output = new PageBuilder(globalAggregationGroupIds.size(), types);

        for (int groupId : globalAggregationGroupIds) {
            output.declarePosition();
            int channel = 0;

            for (; channel < groupByTypes.size(); channel++) {
                if (channel == groupIdChannel.get()) {
                    output.getBlockBuilder(channel).writeLong(groupId);
                }
                else {
                    output.getBlockBuilder(channel).appendNull();
                }
            }

            if (hashChannel.isPresent()) {
                long hashValue = calculateDefaultOutputHash(groupByTypes, groupIdChannel.get(), groupId);
                output.getBlockBuilder(channel++).writeLong(hashValue);
            }

            for (int j = 0; j < accumulators.size(); channel++, j++) {
                if (step.isOutputPartial()) {
                    accumulators.get(j).evaluateIntermediate(output.getBlockBuilder(channel));
                }
                else {
                    accumulators.get(j).evaluateFinal(output.getBlockBuilder(channel));
                }
            }
        }

        if (output.isEmpty()) {
            return null;
        }
        return output.build();
    }

    private static long calculateDefaultOutputHash(List<Type> groupByChannels, int groupIdChannel, int groupId)
    {
        // Default output has NULLs on all columns except of groupIdChannel
        long result = INITIAL_HASH_VALUE;
        for (int channel = 0; channel < groupByChannels.size(); channel++) {
            if (channel != groupIdChannel) {
                result = CombineHashFunction.getHash(result, NULL_HASH_CODE);
            }
            else {
                result = CombineHashFunction.getHash(result, BigintType.hash(groupId));
            }
        }
        return result;
    }

    protected boolean hasOrderBy()
    {
        return accumulatorFactories.stream().anyMatch(AccumulatorFactory::hasOrderBy);
    }

    protected boolean hasDistinct()
    {
        return accumulatorFactories.stream().anyMatch(AccumulatorFactory::hasDistinct);
    }

    public static GroupJoinAggregator buildGroupJoinAggregator(List<Type> groupByTypes,
            List<Integer> groupByChannels,
            List<Integer> globalAggregationGroupIds,
            AggregationNode.Step step,
            List<AccumulatorFactory> accumulatorFactories,
            Optional<Integer> hashChannel,
            Optional<Integer> groupIdChannel,
            int expectedGroups,
            Optional<DataSize> maxPartialMemory,
            JoinCompiler joinCompiler,
            boolean useSystemMemory,
            Optional<PartialAggregationController> partialAggregationController,
            boolean produceDefaultOutput)
    {
        return new GroupJoinAggregator(hashChannel,
                groupIdChannel,
                accumulatorFactories,
                groupByTypes,
                groupByChannels,
                globalAggregationGroupIds,
                step,
                expectedGroups,
                maxPartialMemory,
                joinCompiler,
                useSystemMemory,
                partialAggregationController,
                produceDefaultOutput);
    }
}
