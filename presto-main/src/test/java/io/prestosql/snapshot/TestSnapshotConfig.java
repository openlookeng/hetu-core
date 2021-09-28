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
package io.prestosql.snapshot;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestSnapshotConfig
{
    @Test
    public void testDefault()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(SnapshotConfig.class)
                .setSnapshotProfile(null)
                .setSnapshotIntervalType(SnapshotConfig.IntervalType.TIME)
                .setSnapshotTimeInterval(new Duration(5, TimeUnit.MINUTES))
                .setSnapshotSplitCountInterval(1000)
                .setSnapshotMaxRetries(10)
                .setSnapshotRetryTimeout(new Duration(10, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hetu.experimental.snapshot.profile", "snapshot-config-hdfs")
                .put("hetu.internal.snapshot.intervalType", "SPLIT_COUNT")
                .put("hetu.internal.snapshot.timeInterval", "3m")
                .put("hetu.internal.snapshot.splitCountInterval", "1000000")
                .put("hetu.snapshot.maxRetries", "20")
                .put("hetu.snapshot.retryTimeout", "5m")
                .build();

        SnapshotConfig expected = new SnapshotConfig()
                .setSnapshotProfile("snapshot-config-hdfs")
                .setSnapshotIntervalType(SnapshotConfig.IntervalType.SPLIT_COUNT)
                .setSnapshotTimeInterval(new Duration(3, TimeUnit.MINUTES))
                .setSnapshotSplitCountInterval(1000000)
                .setSnapshotMaxRetries(20)
                .setSnapshotRetryTimeout(new Duration(5, TimeUnit.MINUTES));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
