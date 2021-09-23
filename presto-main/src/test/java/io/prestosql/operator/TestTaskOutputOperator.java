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
package io.prestosql.operator;

import io.airlift.units.DataSize;
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.PartitionedOutputBuffer;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.SnapshotTestUtil;
import io.prestosql.testing.TestingTaskContext;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
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

public class TestTaskOutputOperator
{
    private static final DataSize MAX_MEMORY = new DataSize(1, GIGABYTE);
    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-EXECUTOR-%s"));
    private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("test-%s"));

    @Test
    public void testSnapshot()
    {
        TaskOutputOperator operator = createTaskOutputOperator();
        Object snapshot = operator.capture(operator.getOperatorContext().getDriverContext().getSerde());
        assertEquals(SnapshotTestUtil.toSimpleSnapshotMapping(snapshot), createExpectedMapping());
        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(3, 1)
                .build();
        operator.addInput(input.get(0));
        operator.restore(snapshot, operator.getOperatorContext().getDriverContext().getSerde());
        snapshot = operator.capture(operator.getOperatorContext().getDriverContext().getSerde());
        assertEquals(SnapshotTestUtil.toSimpleSnapshotMapping(snapshot), createExpectedMapping());
    }

    private Map<String, Object> createExpectedMapping()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("operatorContext", 0);
        expectedMapping.put("finished", false);
        return expectedMapping;
    }

    private TaskOutputOperator createTaskOutputOperator()
    {
        ScheduledExecutorService taskNotificationExecutor = newScheduledThreadPool(1, threadsNamed("task-notification-%s"));
        PartitionedOutputBuffer outputBuffer = newTestingOutputBuffer(taskNotificationExecutor);
        TaskOutputOperator.TaskOutputOperatorFactory factory = new TaskOutputOperator.TaskOutputOperatorFactory(
                1,
                new PlanNodeId("plan-node-0"),
                outputBuffer,
                Function.identity());
        return (TaskOutputOperator) factory.createOperator(createDriverContext(createTaskContext()));
    }

    private PartitionedOutputBuffer newTestingOutputBuffer(ScheduledExecutorService taskNotificationExecutor)
    {
        return new PartitionedOutputBuffer(
                new StateMachine<>("bufferState", taskNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(new OutputBuffers.OutputBufferId(0), 0)
                        .withNoMoreBufferIds(),
                new DataSize(1, MEGABYTE),
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                taskNotificationExecutor);
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
}
