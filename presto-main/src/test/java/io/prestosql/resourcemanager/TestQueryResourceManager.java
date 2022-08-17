/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.resourcemanager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.StageId;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.StageStats;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskStatus;
import io.prestosql.execution.buffer.OutputBufferInfo;
import io.prestosql.operator.TaskStats;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.eventlistener.StageGcStatistics;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.execution.StageState.RUNNING;
import static io.prestosql.execution.TaskState.PLANNED;
import static io.prestosql.execution.buffer.BufferState.OPEN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;

public class TestQueryResourceManager
{
    private final QueryId queryId = new QueryId("test_query_001");
    private BasicResourceStats basicResourceStats = new BasicResourceStats();
    Map<String, BasicResourceStats> nodeResourceMap = new HashMap<>();
    private QueryResourceManagerService.ResourceUpdateListener resourceUpdateListener;
    private QueryResourceManager queryResourceManager;
    private Session session;

    private final StageStats sampleStats = new StageStats(
            new DateTime(0),
            getTestDistribution(1),

            4,
            5,
            6,

            7,
            8,
            10,
            26,
            11,

            12.0,
            new DataSize(13, BYTE),
            new DataSize(14, BYTE),
            new DataSize(15, BYTE),
            new DataSize(16, BYTE),
            new DataSize(17, BYTE),

            new Duration(15, NANOSECONDS),
            new Duration(16, NANOSECONDS),
            new Duration(18, NANOSECONDS),
            false,
            ImmutableSet.of(),

            new DataSize(191, BYTE),
            201,

            new DataSize(192, BYTE),
            202,

            new DataSize(19, BYTE),
            20,

            new DataSize(21, BYTE),
            22,

            new DataSize(23, BYTE),
            new DataSize(24, BYTE),
            25,

            new DataSize(26, BYTE),

            new StageGcStatistics(
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107),

            ImmutableList.of());

    private final List<StageInfo> stageInfos = new ArrayList<>();

    public TestQueryResourceManager()
    {
        resourceUpdateListener = new QueryResourceManagerService.ResourceUpdateListener() {
            public void resourceUpdate(QueryId queryId, BasicResourceStats stats, Map<String, BasicResourceStats> nodeStats)
            {
                basicResourceStats.cpuTime = Duration.succinctDuration(basicResourceStats.cpuTime.getValue() + stats.cpuTime.getValue(), stats.cpuTime.getUnit());
                basicResourceStats.ioCurrent = DataSize.succinctBytes(basicResourceStats.ioCurrent.toBytes() + stats.ioCurrent.toBytes());
                basicResourceStats.memCurrent = DataSize.succinctBytes(basicResourceStats.memCurrent.toBytes() + stats.memCurrent.toBytes());
                basicResourceStats.revocableMem = DataSize.succinctBytes(basicResourceStats.revocableMem.toBytes() + stats.revocableMem.toBytes());

                nodeResourceMap.putAll(nodeStats);
            }
        };
        queryResourceManager = new QueryResourceManager(queryId, null, resourceUpdateListener);
    }

    private static StageInfo addStageInfo(QueryId queryId, int id, int taskCount, List<StageInfo> subStage)
    {
        StageId stageId = new StageId(queryId, id);
        List<TaskInfo> taskInfos = new ArrayList<>();

        for (int i = 0; i < taskCount; i++) {
            taskInfos.add(addTaskInfo(stageId, "node-" + i, i + 1));
        }

        StageStats stageStats = new StageStats(new DateTime(0),
                getTestDistribution(1),
                4,
                5,
                6,
                7,
                8,
                10,
                26,
                11,
                12.0,
                new DataSize(10 * taskCount, BYTE),
                new DataSize(100 * taskCount, BYTE),
                new DataSize(15, BYTE),
                new DataSize(16, BYTE),
                new DataSize(17, BYTE),

                new Duration(15, NANOSECONDS),
                new Duration(10 * taskCount, MILLISECONDS),
                new Duration(18, NANOSECONDS),
                false,
                ImmutableSet.of(),

                new DataSize(191, BYTE),
                201,

                new DataSize(50 * taskCount, BYTE),
                202,

                new DataSize(19, BYTE),
                20,

                new DataSize(21, BYTE),
                22,

                new DataSize(23, BYTE),
                new DataSize(24, BYTE),
                25,

                new DataSize(26, BYTE),

                new StageGcStatistics(
                        101,
                        102,
                        103,
                        104,
                        105,
                        106,
                        107),
                ImmutableList.of());

        return new StageInfo(stageId,
                RUNNING,
                false,
                0,
                URI.create("http://localhost:8080/self"),
                null,
                ImmutableList.of(),
                stageStats,
                taskInfos,
                subStage,
                ImmutableMap.of(),
                null);
    }

    private static TaskInfo addTaskInfo(StageId stageId, String nodeId, int taskId)
    {
        TaskStatus taskStatus = new TaskStatus(
                new TaskId(stageId, taskId),
                "confirmationInstanceId",
                1,
                PLANNED,
                URI.create("http://localhost:8080/self"),
                nodeId,
                ImmutableSet.of(),
                ImmutableList.of(),
                0,
                0,
                false,
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                0,
                new Duration(0, MILLISECONDS),
                ImmutableMap.of(),
                Optional.empty());
        TaskStats taskStats = new TaskStats(DateTime.now(),
                null,
                null,
                null,
                null,
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0.0,
                new DataSize(10, BYTE),
                new DataSize(100, BYTE),
                new DataSize(0, BYTE),
                new Duration(0, MILLISECONDS),
                new Duration(10, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                false,
                ImmutableSet.of(),
                new DataSize(0, BYTE),
                0,
                new DataSize(50, BYTE),
                0,
                new DataSize(0, BYTE),
                0,
                new DataSize(0, BYTE),
                0,
                new DataSize(0, BYTE),
                0,
                new DataSize(0, BYTE),
                0,
                new Duration(0, MILLISECONDS),
                ImmutableList.of());

        return new TaskInfo(taskStatus, DateTime.now(),
                new OutputBufferInfo("UNINITIALIZED", OPEN, true, true, 0, 0, 0, 0, ImmutableList.of()),
                ImmutableSet.of(),
                taskStats,
                true);
    }

    @Test
    public void testSetResourceLimit()
    {
        queryResourceManager.setResourceLimit(DataSize.succinctBytes(100),
                Duration.succinctDuration(200, TimeUnit.MINUTES),
                DataSize.succinctBytes(300));
        assertEquals(DataSize.succinctBytes(100), queryResourceManager.getMemoryLimit());
    }

    @Test
    public void testUpdateStats()
    {
        /* reset stats */
        basicResourceStats = new BasicResourceStats();

        BasicResourceStats stats = new BasicResourceStats(DataSize.succinctBytes(12),
                Duration.succinctDuration(16.0, NANOSECONDS),
                DataSize.succinctBytes(192), DataSize.succinctBytes(0));
        queryResourceManager.updateStats(ImmutableList.of(sampleStats));  // 12.0, 16, 192
        assertEquals(stats, basicResourceStats);

        queryResourceManager.updateStats(ImmutableList.of(sampleStats, sampleStats, sampleStats));  // 12.0, 16, 192

        stats = new BasicResourceStats(DataSize.succinctBytes(12 * 4),
                Duration.succinctDuration(16.0 * 4, NANOSECONDS),
                DataSize.succinctBytes(192 * 4), DataSize.succinctBytes(0));
        assertEquals(stats, basicResourceStats);
    }

    @Test
    public void testExceedLimit()
    {
        /* reset stats */
        basicResourceStats = new BasicResourceStats();

        queryResourceManager.setResourceLimit(DataSize.succinctBytes(10),
                Duration.succinctDuration(20, NANOSECONDS),
                DataSize.succinctBytes(30));
        queryResourceManager.updateStats(ImmutableList.of(sampleStats, sampleStats, sampleStats));  // 12.0, 16, 192

        BasicResourceStats stats = new BasicResourceStats(DataSize.succinctBytes(12 * 3),
                Duration.succinctDuration(16.0 * 3, NANOSECONDS),
                DataSize.succinctBytes(192 * 3), DataSize.succinctBytes(0));
        assertEquals(stats, basicResourceStats);
    }

    @Test
    public void testUpdateStatsByStagesInfo()
    {
        /* reset stats */
        basicResourceStats = new BasicResourceStats();
        nodeResourceMap = new HashMap<>();

        List<StageInfo> stageInfos = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            stageInfos.add(addStageInfo(queryId, i, 5, ImmutableList.of()));
        }

        BasicResourceStats stats = new BasicResourceStats(DataSize.succinctBytes(10 * 5 * 2),
                Duration.succinctDuration(10.0 * 5 * 2, MILLISECONDS),
                DataSize.succinctBytes(50 * 5 * 2),
                DataSize.succinctBytes(100 * 5 * 2));

        Map<String, BasicResourceStats> nodeExpect = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            nodeExpect.put("node-" + i,
                    new BasicResourceStats(DataSize.succinctBytes(10 * 2),
                        Duration.succinctDuration(10.0 * 2, MILLISECONDS),
                        DataSize.succinctBytes(50 * 2),
                        DataSize.succinctBytes(100 * 2)));
        }

        queryResourceManager.updateStats(ImmutableList.copyOf(stageInfos));
        assertEquals(stats, basicResourceStats);
        assertEquals(nodeExpect, nodeResourceMap);

        /* Test no-change stats case */
        queryResourceManager.updateStats(ImmutableList.copyOf(stageInfos));
        assertEquals(nodeExpect, nodeResourceMap);

        /* Test small delta change */
        stageInfos.add(addStageInfo(queryId, 2, 5, ImmutableList.of()));
        queryResourceManager.updateStats(ImmutableList.copyOf(stageInfos));
        for (int i = 0; i < 5; i++) {
            nodeExpect.put("node-" + i,
                    new BasicResourceStats(DataSize.succinctBytes(10 * 3),
                            Duration.succinctDuration(10.0 * 3, MILLISECONDS),
                            DataSize.succinctBytes(50 * 3),
                            DataSize.succinctBytes(100 * 3)));
        }
        assertEquals(nodeExpect, nodeResourceMap);
    }

    private static Distribution.DistributionSnapshot getTestDistribution(int count)
    {
        Distribution distribution = new Distribution();
        for (int i = 0; i < count; i++) {
            distribution.add(i);
        }
        return distribution.snapshot();
    }
}
