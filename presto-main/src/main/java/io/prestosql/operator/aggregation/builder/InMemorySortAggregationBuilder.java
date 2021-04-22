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

package io.prestosql.operator.aggregation.builder;

import io.airlift.units.DataSize;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.UpdateMemory;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.type.BooleanType.BOOLEAN;

public class InMemorySortAggregationBuilder
        extends InMemoryAggregationBuilder
{
    public InMemorySortAggregationBuilder(List<AccumulatorFactory> accumulatorFactories, AggregationNode.Step step,
                                          int expectedGroups, List<Type> groupByTypes, List<Integer> groupByChannels,
                                          Optional<Integer> hashChannel, OperatorContext operatorContext,
                                          Optional<DataSize> maxPartialMemory, JoinCompiler joinCompiler,
                                          UpdateMemory updateMemory)
    {
        super(accumulatorFactories, step, expectedGroups, groupByTypes, groupByChannels, hashChannel, operatorContext,
                maxPartialMemory, joinCompiler, updateMemory, AggregationNode.AggregationType.SORT_BASED);
    }

    public WorkProcessor<Page> buildResult(AggregationNode.Step step)
    {
        for (Aggregator aggregator : aggregators) {
            aggregator.prepareFinal();
        }
        return buildResult(consecutiveGroupIds(), step);
    }

    public List<Type> buildTypes(AggregationNode.Step step)
    {
        ArrayList<Type> types = new ArrayList<>(groupBy.getTypes());
        for (Aggregator aggregator : aggregators) {
            types.add(aggregator.getType());
        }
        // BOOLEAN block is added to identify row is finalized
        if (AggregationNode.Step.PARTIAL.equals(step)) {
            types.add(BooleanType.BOOLEAN);
        }
        return types;
    }

    private WorkProcessor<Page> buildResult(IntIterator groupIds, AggregationNode.Step step)
    {
        final PageBuilder pageBuilder = new PageBuilder(buildTypes(step));
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

                if (step == AggregationNode.Step.PARTIAL) {
                    BlockBuilder output1 = pageBuilder.getBlockBuilder(types.size() + aggregators.size());
                    if (groupId == 0 || !groupIds.hasNext()) {
                        BOOLEAN.writeBoolean(output1, false);
                    }
                    else {
                        // finalized values are set to true
                        BOOLEAN.writeBoolean(output1, true);
                    }
                }
            }

            return WorkProcessor.ProcessState.ofResult(pageBuilder.build());
        });
    }
}
