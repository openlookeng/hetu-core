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
package io.prestosql.utils;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestHetuConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(HetuConfig.class)
                .setFilterEnabled(false)
                .setIndexStoreUri("/opt/hetu/indices/")
                .setIndexStoreFileSystemProfile("local-config-default")
                .setMaxIndicesInCache(10000000L)
                .setExecutionPlanCacheEnabled(false)
                .setExecutionPlanCacheTimeout(60000L)
                .setExecutionPlanCacheMaxItems(1000L)
                .setEmbeddedStateStoreEnabled(false)
                .setMultipleCoordinatorEnabled(false)
                .setStateFetchInterval(new Duration(100, TimeUnit.MILLISECONDS))
                .setStateUpdateInterval(new Duration(100, TimeUnit.MILLISECONDS))
                .setQuerySubmitTimeout(new Duration(10, TimeUnit.SECONDS))
                .setStateExpireTime(new Duration(10, TimeUnit.SECONDS))
                .setDataCenterSplits(5)
                .setDataCenterConsumerTimeout(new Duration(10, TimeUnit.MINUTES))
                .setSeedStoreEnabled(false)
                .setSplitCacheMapEnabled(false)
                .setSplitCacheStateUpdateInterval(new Duration(2, TimeUnit.SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hetu.heuristicindex.filter.enabled", "true")
                .put("hetu.heuristicindex.indexstore.uri", "/tmp")
                .put("hetu.heuristicindex.indexstore.filesystem.profile", "index-test")
                .put("hetu.heuristicindex.filter.cache.max-indices-number", "10")
                .put("hetu.executionplan.cache.enabled", "true")
                .put("hetu.executionplan.cache.timeout", "6000")
                .put("hetu.executionplan.cache.limit", "10000")
                .put("hetu.embedded-state-store.enabled", "true")
                .put("hetu.multiple-coordinator.enabled", "true")
                .put("hetu.multiple-coordinator.query-submit-timeout", "20s")
                .put("hetu.multiple-coordinator.state-expire-time", "20s")
                .put("hetu.multiple-coordinator.state-fetch-interval", "5s")
                .put("hetu.multiple-coordinator.state-update-interval", "5s")
                .put("hetu.seed-store.enabled", "true")
                .put("hetu.data.center.split.count", "10")
                .put("hetu.data.center.consumer.timeout", "5m")
                .put("hetu.split-cache-map.enabled", "true")
                .put("hetu.split-cache-map.state-update-interval", "5s")
                .build();

        HetuConfig expected = new HetuConfig()
                .setFilterEnabled(true)
                .setIndexStoreUri("/tmp")
                .setIndexStoreFileSystemProfile("index-test")
                .setMaxIndicesInCache(10L)
                .setExecutionPlanCacheEnabled(true)
                .setExecutionPlanCacheTimeout(6000L)
                .setExecutionPlanCacheMaxItems(10000L)
                .setEmbeddedStateStoreEnabled(true)
                .setMultipleCoordinatorEnabled(true)
                .setQuerySubmitTimeout(new Duration(20, TimeUnit.SECONDS))
                .setStateExpireTime(new Duration(20, TimeUnit.SECONDS))
                .setStateFetchInterval(new Duration(5, TimeUnit.SECONDS))
                .setStateUpdateInterval(new Duration(5, TimeUnit.SECONDS))
                .setSeedStoreEnabled(true)
                .setDataCenterSplits(10)
                .setDataCenterConsumerTimeout(new Duration(5, TimeUnit.MINUTES))
                .setSplitCacheMapEnabled(true)
                .setSplitCacheStateUpdateInterval(new Duration(5, TimeUnit.SECONDS));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
