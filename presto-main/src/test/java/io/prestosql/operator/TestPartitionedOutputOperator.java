/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.PartitionedOutputBuffer;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.operator.exchange.LocalPartitionGenerator;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.SnapshotTestUtil;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.TestingTaskContext;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.buffer.BufferState.OPEN;
import static io.prestosql.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.prestosql.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class TestPartitionedOutputOperator
{
    private static final int PARTITION_COUNT = 512;
    private static final DataSize MAX_MEMORY = new DataSize(1, GIGABYTE);
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-EXECUTOR-%s"));
    private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("test-%s"));

    @Test
    public void testPartitionedOutputOperatorSnapshot()
    {
        PartitionedOutputOperator operator = createPartitionedOutputOperator();
        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(3, 1)
                .addSequencePage(3, 4)
                .build();
        //Add first page, then capture and compare, then add second page, then restore, then compare, then add second page, then finish, then compare
        operator.addInput(input.get(0));
        Object snapshot = operator.capture(operator.getOperatorContext().getDriverContext().getSerde());
        assertEquals(SnapshotTestUtil.toFullSnapshotMapping(snapshot), createExpectedMappingBeforeFinish());
        operator.addInput(input.get(1));
        operator.restore(snapshot, operator.getOperatorContext().getDriverContext().getSerde());
        snapshot = operator.capture(operator.getOperatorContext().getDriverContext().getSerde());
        assertEquals(SnapshotTestUtil.toFullSnapshotMapping(snapshot), createExpectedMappingBeforeFinish());
        operator.addInput(input.get(1));
        operator.finish();
        snapshot = operator.capture(operator.getOperatorContext().getDriverContext().getSerde());
        assertEquals(SnapshotTestUtil.toFullSnapshotMapping(snapshot), createExpectedMappingAfterFinish());
    }

    private Map<String, Object> createExpectedMappingBeforeFinish()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        Map<String, Object> partitionFunctionMapping = new HashMap<>();
        expectedMapping.put("operatorContext", 0);
        expectedMapping.put("partitionFunction", partitionFunctionMapping);
        expectedMapping.put("systemMemoryContext", 65563L);
        expectedMapping.put("finished", false);
        partitionFunctionMapping.put("pageBuilders", Object[].class);
        partitionFunctionMapping.put("rowsAdded", 0L);
        partitionFunctionMapping.put("pagesAdded", 0L);
        partitionFunctionMapping.put("hasAnyRowBeenReplicated", false);
        return expectedMapping;
    }

    private Map<String, Object> createExpectedMappingAfterFinish()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        Map<String, Object> partitionFunctionMapping = new HashMap<>();
        expectedMapping.put("operatorContext", 0);
        expectedMapping.put("partitionFunction", partitionFunctionMapping);
        expectedMapping.put("systemMemoryContext", 65590L);
        expectedMapping.put("finished", true);
        partitionFunctionMapping.put("pageBuilders", Object[].class);
        partitionFunctionMapping.put("rowsAdded", 6L);
        partitionFunctionMapping.put("pagesAdded", 6L);
        partitionFunctionMapping.put("hasAnyRowBeenReplicated", false);
        return expectedMapping;
    }

    private PartitionedOutputOperator createPartitionedOutputOperator()
    {
        PartitionFunction partitionFunction = new LocalPartitionGenerator(new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0}), PARTITION_COUNT);
        OutputBuffers buffers = createInitialEmptyOutputBuffers(PARTITIONED);
        for (int partition = 0; partition < PARTITION_COUNT; partition++) {
            buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
        }
        PartitionedOutputBuffer buffer = createPartitionedBuffer(
                buffers.withNoMoreBufferIds(),
                new DataSize(Long.MAX_VALUE, BYTE)); // don't let output buffer block
        PartitionedOutputOperator.PartitionedOutputFactory operatorFactory = new PartitionedOutputOperator.PartitionedOutputFactory(
                partitionFunction,
                ImmutableList.of(0),
                ImmutableList.of(Optional.empty()),
                false,
                OptionalInt.empty(),
                buffer,
                new DataSize(1, GIGABYTE));
        TaskContext taskContext = createTaskContext();
        return (PartitionedOutputOperator) operatorFactory
                .createOutputOperator(0, new PlanNodeId("plan-node-0"), TYPES, Function.identity(), taskContext)
                .createOperator(createDriverContext(taskContext));
    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.builder(EXECUTOR, SCHEDULER, TEST_SESSION)
                .setMemoryPoolSize(MAX_MEMORY)
                .build();
    }

    private DriverContext createDriverContext(TaskContext taskContext)
    {
        return taskContext
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private PartitionedOutputBuffer createPartitionedBuffer(OutputBuffers buffers, DataSize dataSize)
    {
        return new PartitionedOutputBuffer(
                "task-instance-id",
                new StateMachine<>("bufferState", SCHEDULER, OPEN, TERMINAL_BUFFER_STATES),
                buffers,
                dataSize,
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                SCHEDULER);
    }
}
