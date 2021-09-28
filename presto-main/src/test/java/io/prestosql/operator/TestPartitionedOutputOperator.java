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

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.buffer.PartitionedOutputBuffer;
import io.prestosql.operator.exchange.LocalPartitionGenerator;
import io.prestosql.snapshot.SnapshotUtils;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.SnapshotTestUtil;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.TestingTaskContext;
import org.mockito.ArgumentCaptor;
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
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class TestPartitionedOutputOperator
{
    private static final int PARTITION_COUNT = 512;
    private static final DataSize MAX_MEMORY = new DataSize(1, GIGABYTE);
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-EXECUTOR-%s"));
    private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("test-%s"));

    @Test
    public void testPartitionedOutputOperatorSnapshot()
            throws Exception
    {
        SnapshotUtils snapshotUtils = mock(SnapshotUtils.class);
        PartitionedOutputBuffer buffer = mock(PartitionedOutputBuffer.class);

        PartitionedOutputOperator operator = createPartitionedOutputOperator(snapshotUtils, buffer);
        operator.getOperatorContext().getDriverContext().getPipelineContext().getTaskContext().getSnapshotManager().setTotalComponents(1);
        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(1, 1)
                .addSequencePage(2, 4)
                .build();
        MarkerPage marker = MarkerPage.snapshotPage(1);
        MarkerPage marker2 = MarkerPage.snapshotPage(2);
        MarkerPage marker3 = MarkerPage.snapshotPage(3);
        MarkerPage resume = MarkerPage.resumePage(1);

        //Add first page, then capture and compare, then add second page, then restore, then compare, then add second page, then finish, then compare
        operator.addInput(input.get(0));
        operator.addInput(marker);
        ArgumentCaptor<Object> stateArgument = ArgumentCaptor.forClass(Object.class);
        verify(snapshotUtils, times(1)).storeState(anyObject(), stateArgument.capture());
        Object snapshot = stateArgument.getValue();
//        assertEquals(SnapshotTestUtil.toFullSnapshotMapping(snapshot), createExpectedMappingBeforeFinish());

        when(snapshotUtils.loadState(anyObject())).thenReturn(Optional.of(snapshot));
        operator.addInput(input.get(1));
        operator.addInput(resume);

        operator.addInput(marker2);
        verify(snapshotUtils, times(2)).storeState(anyObject(), stateArgument.capture());
        snapshot = stateArgument.getValue();
        Object snapshotEntry = ((Map<String, Object>) snapshot).get("query/2/1/1/0/0/0");
        assertEquals(SnapshotTestUtil.toFullSnapshotMapping(snapshotEntry), createExpectedMappingBeforeFinish());

        operator.addInput(input.get(1));
        operator.finish();
        operator.addInput(marker3);
        verify(snapshotUtils, times(3)).storeState(anyObject(), stateArgument.capture());
        snapshot = stateArgument.getValue();
        snapshotEntry = ((Map<String, Object>) snapshot).get("query/3/1/1/0/0/0");
        assertEquals(SnapshotTestUtil.toFullSnapshotMapping(snapshotEntry), createExpectedMappingAfterFinish());

        ArgumentCaptor<List> pagesArgument = ArgumentCaptor.forClass(List.class);
        verify(buffer, times(9)).enqueue(anyInt(), pagesArgument.capture(), anyString());
        List<List> pages = pagesArgument.getAllValues();
        // 9 pages:
        // 1 (page 1 partitioned)
        // 1 (marker 1)
        // 2 (page 2 before resume)
        // 1 (resume marker)
        // 1 (marker 2)
        // 2 (page 2 after resume)
        // 1 (marker 3)
        assertEquals(pages.size(), 9);
        assertTrue(((SerializedPage) pages.get(1).get(0)).isMarkerPage());
        assertTrue(((SerializedPage) pages.get(4).get(0)).isMarkerPage());
        assertTrue(((SerializedPage) pages.get(5).get(0)).isMarkerPage());
        assertTrue(((SerializedPage) pages.get(8).get(0)).isMarkerPage());
    }

    private Map<String, Object> createExpectedMappingBeforeFinish()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        Map<String, Object> partitionFunctionMapping = new HashMap<>();
        expectedMapping.put("operatorContext", 0);
        expectedMapping.put("partitionFunction", partitionFunctionMapping);
        expectedMapping.put("systemMemoryContext", 65536L);
        expectedMapping.put("finished", false);
        partitionFunctionMapping.put("rowsAdded", 1L); // Input page 1 has 3 rows; partitioned to 3 separate pages
        partitionFunctionMapping.put("pagesAdded", 1L);
        partitionFunctionMapping.put("hasAnyRowBeenReplicated", false);
        return expectedMapping;
    }

    private Map<String, Object> createExpectedMappingAfterFinish()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        Map<String, Object> partitionFunctionMapping = new HashMap<>();
        expectedMapping.put("operatorContext", 0);
        expectedMapping.put("partitionFunction", partitionFunctionMapping);
        expectedMapping.put("systemMemoryContext", 65554L);
        expectedMapping.put("finished", true);
        partitionFunctionMapping.put("rowsAdded", 3L);
        partitionFunctionMapping.put("pagesAdded", 3L);
        partitionFunctionMapping.put("hasAnyRowBeenReplicated", false);
        return expectedMapping;
    }

    private PartitionedOutputOperator createPartitionedOutputOperator(SnapshotUtils snapshotUtils, PartitionedOutputBuffer buffer)
    {
        PartitionFunction partitionFunction = new LocalPartitionGenerator(new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0}), PARTITION_COUNT);
        PartitionedOutputOperator.PartitionedOutputFactory operatorFactory = new PartitionedOutputOperator.PartitionedOutputFactory(
                partitionFunction,
                ImmutableList.of(0),
                ImmutableList.of(Optional.empty()),
                false,
                OptionalInt.empty(),
                buffer,
                new DataSize(1, GIGABYTE));
        TaskContext taskContext = createTaskContext(snapshotUtils);
        return (PartitionedOutputOperator) operatorFactory
                .createOutputOperator(0, new PlanNodeId("plan-node-0"), TYPES, Function.identity(), taskContext)
                .createOperator(createDriverContext(taskContext));
    }

    private TaskContext createTaskContext(SnapshotUtils snapshotUtils)
    {
        TaskContext taskContext = TestingTaskContext.builder(EXECUTOR, SCHEDULER, TEST_SNAPSHOT_SESSION, snapshotUtils)
                .setMemoryPoolSize(MAX_MEMORY)
                .build();
        // So that
        return TestingTaskContext.createTaskContext(taskContext.getQueryContext(), EXECUTOR, TEST_SNAPSHOT_SESSION, new TaskId("query.1.1"));
    }

    private DriverContext createDriverContext(TaskContext taskContext)
    {
        return taskContext
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
