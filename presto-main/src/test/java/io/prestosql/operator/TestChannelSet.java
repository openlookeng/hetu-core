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

import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.SnapshotTestUtil;
import io.prestosql.spi.type.BigintType;
import io.prestosql.sql.gen.JoinCompiler;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestChannelSet
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private TaskContext taskContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testChannelSetSnapshot()
    {
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), "dummyOperator");

        ChannelSet.ChannelSetBuilder channelSetBuilder1 = new ChannelSet.ChannelSetBuilder(BigintType.BIGINT,
                Optional.of(1),
                6,
                operatorContext,
                new JoinCompiler(createTestMetadataManager()));
        ChannelSet.ChannelSetBuilder channelSetBuilder2 = new ChannelSet.ChannelSetBuilder(BigintType.BIGINT,
                Optional.of(1),
                6,
                operatorContext,
                new JoinCompiler(createTestMetadataManager()));

        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(3, 1)
                .addSequencePage(3, 4)
                .build();

        channelSetBuilder1.addPage(input.get(0));
        channelSetBuilder1.addPage(input.get(1));
        ChannelSet channelSet1 = channelSetBuilder1.build();
        Object channelSet1Snapshot = channelSet1.capture(operatorContext.getDriverContext().getSerde());
        assertEquals(SnapshotTestUtil.toFullSnapshotMapping(channelSet1Snapshot), createChannelSetExpectedMapping());

        channelSetBuilder2.addPage(input.get(0));
        Object channelSetBuilder2Snapshot = channelSetBuilder2.capture(operatorContext.getDriverContext().getSerde());
        assertEquals(SnapshotTestUtil.toFullSnapshotMapping(channelSetBuilder2Snapshot), createChannelSetBuilder2ExpectedMapping());
        channelSetBuilder2.addPage(input.get(1));
        channelSetBuilder2.restore(channelSetBuilder2Snapshot, operatorContext.getDriverContext().getSerde());
        channelSetBuilder2.addPage(input.get(1));
        ChannelSet channelSet2 = channelSetBuilder2.build();
        Object channelSet2Snapshot = channelSet2.capture(operatorContext.getDriverContext().getSerde());
        assertEquals(SnapshotTestUtil.toFullSnapshotMapping(channelSet2Snapshot), createChannelSetExpectedMapping());
    }

    private Map<String, Object> createChannelSetExpectedMapping()
    {
        Map<String, Object> groupByHashMapping = new HashMap<>();
        Map<String, Object> valuesMapping = new HashMap<>();
        Map<String, Object> groupIdsMapping = new HashMap<>();
        Map<String, Object> valuesByGroupIdMapping = new HashMap<>();

        groupByHashMapping.put("hashCapacity", 8);
        groupByHashMapping.put("currentPageSizeInBytes", 220L);
        groupByHashMapping.put("maxFill", 6);
        groupByHashMapping.put("mask", 7);
        groupByHashMapping.put("values", valuesMapping);
        groupByHashMapping.put("groupIds", groupIdsMapping);
        groupByHashMapping.put("nullGroupId", -1);
        groupByHashMapping.put("valuesByGroupId", valuesByGroupIdMapping);
        groupByHashMapping.put("nextGroupId", 0);
        groupByHashMapping.put("hashCollisions", 0L);
        groupByHashMapping.put("expectedHashCollisions", 0.0);
        groupByHashMapping.put("preallocatedMemoryInBytes", 0L);

        valuesMapping.put("array", long[][].class);
        valuesMapping.put("capacity", 1024);
        valuesMapping.put("segments", 1);

        groupIdsMapping.put("array", int[][].class);
        groupIdsMapping.put("capacity", 1024);
        groupIdsMapping.put("segments", 1);

        valuesByGroupIdMapping.put("array", long[][].class);
        valuesByGroupIdMapping.put("capacity", 1024);
        valuesByGroupIdMapping.put("segments", 1);

        return groupByHashMapping;
    }

    private Map<String, Object> createChannelSetBuilder2ExpectedMapping()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        Map<String, Object> groupByHashMapping = new HashMap<>();
        Map<String, Object> valuesMapping = new HashMap<>();
        Map<String, Object> groupIdsMapping = new HashMap<>();
        Map<String, Object> valuesByGroupIdMapping = new HashMap<>();

        expectedMapping.put("hash", groupByHashMapping);

        groupByHashMapping.put("hashCapacity", 8);
        groupByHashMapping.put("currentPageSizeInBytes", 220L);
        groupByHashMapping.put("maxFill", 6);
        groupByHashMapping.put("mask", 7);
        groupByHashMapping.put("values", valuesMapping);
        groupByHashMapping.put("groupIds", groupIdsMapping);
        groupByHashMapping.put("nullGroupId", -1);
        groupByHashMapping.put("valuesByGroupId", valuesByGroupIdMapping);
        groupByHashMapping.put("nextGroupId", 0);
        groupByHashMapping.put("hashCollisions", 0L);
        groupByHashMapping.put("expectedHashCollisions", 0.0);
        groupByHashMapping.put("preallocatedMemoryInBytes", 0L);

        valuesMapping.put("array", long[][].class);
        valuesMapping.put("capacity", 1024);
        valuesMapping.put("segments", 1);

        groupIdsMapping.put("array", int[][].class);
        groupIdsMapping.put("capacity", 1024);
        groupIdsMapping.put("segments", 1);

        valuesByGroupIdMapping.put("array", long[][].class);
        valuesByGroupIdMapping.put("capacity", 1024);
        valuesByGroupIdMapping.put("segments", 1);

        expectedMapping.put("operatorContext", 0);
        expectedMapping.put("localMemoryContext", 0L);

        return expectedMapping;
    }
}
