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
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.StageStats;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.eventlistener.StageGcStatistics;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;

public class TestQueryResourceManager
{
    private final QueryId queryId = new QueryId("test_query_001");
    private BasicResourceStats basicResourceStats = new BasicResourceStats();
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

    public TestQueryResourceManager()
    {
        resourceUpdateListener = new QueryResourceManagerService.ResourceUpdateListener() {
            public void resourceUpdate(QueryId queryId, BasicResourceStats stats)
            {
                basicResourceStats.cpuTime = Duration.succinctDuration(basicResourceStats.cpuTime.getValue() + stats.cpuTime.getValue(), stats.cpuTime.getUnit());
                basicResourceStats.ioCurrent = DataSize.succinctBytes(basicResourceStats.ioCurrent.toBytes() + stats.ioCurrent.toBytes());
                basicResourceStats.memCurrent = DataSize.succinctBytes(basicResourceStats.memCurrent.toBytes() + stats.memCurrent.toBytes());
            }
        };
        queryResourceManager = new QueryResourceManager(queryId, null, resourceUpdateListener);
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
                DataSize.succinctBytes(192));
        queryResourceManager.updateStats(ImmutableList.of(sampleStats));  // 12.0, 16, 192
        assertEquals(stats, basicResourceStats);

        queryResourceManager.updateStats(ImmutableList.of(sampleStats, sampleStats, sampleStats));  // 12.0, 16, 192

        stats = new BasicResourceStats(DataSize.succinctBytes(12 * 4),
                Duration.succinctDuration(16.0 * 4, NANOSECONDS),
                DataSize.succinctBytes(192 * 4));
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
                DataSize.succinctBytes(192 * 3));
        assertEquals(stats, basicResourceStats);
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
