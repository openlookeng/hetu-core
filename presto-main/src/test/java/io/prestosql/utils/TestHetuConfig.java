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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.GIGABYTE;

public class TestHetuConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(HetuConfig.class)
                .setFilterEnabled(false)
                .setIndexStoreUri("/opt/hetu/indices/")
                .setIndexStoreFileSystemProfile("local-config-default")
                .setIndexCacheMaxMemory(new DataSize(10, GIGABYTE))
                .setIndexCacheTTL(new Duration(24, TimeUnit.HOURS))
                .setIndexCacheLoadingThreads(10L)
                .setIndexCacheLoadingDelay(new Duration(10, TimeUnit.SECONDS))
                .setIndexCacheSoftReferenceEnabled(true)
                .setExecutionPlanCacheEnabled(false)
                .setExecutionPlanCacheTimeout(86400000L)
                .setExecutionPlanCacheMaxItems(10000L)
                .setEmbeddedStateStoreEnabled(false)
                .setMultipleCoordinatorEnabled(false)
                .setStateFetchInterval(new Duration(100, TimeUnit.MILLISECONDS))
                .setStateUpdateInterval(new Duration(100, TimeUnit.MILLISECONDS))
                .setQuerySubmitTimeout(new Duration(10, TimeUnit.SECONDS))
                .setStateExpireTime(new Duration(10, TimeUnit.SECONDS))
                .setDataCenterSplits(5)
                .setDataCenterConsumerTimeout(new Duration(10, TimeUnit.MINUTES))
                .setSplitCacheMapEnabled(false)
                .setSplitCacheStateUpdateInterval(new Duration(2, TimeUnit.SECONDS))
                .setTraceStackVisible(false)
                .setIndexToPreload(""));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hetu.heuristicindex.filter.enabled", "true")
                .put("hetu.heuristicindex.indexstore.uri", "/tmp")
                .put("hetu.heuristicindex.indexstore.filesystem.profile", "index-test")
                .put("hetu.heuristicindex.filter.cache.max-memory", "2GB")
                .put("hetu.heuristicindex.filter.cache.loading-threads", "5")
                .put("hetu.heuristicindex.filter.cache.loading-delay", "1000ms")
                .put("hetu.heuristicindex.filter.cache.ttl", "20m")
                .put("hetu.heuristicindex.filter.cache.soft-reference", "false")
                .put("hetu.executionplan.cache.enabled", "true")
                .put("hetu.executionplan.cache.timeout", "6000")
                .put("hetu.executionplan.cache.limit", "20000")
                .put("hetu.embedded-state-store.enabled", "true")
                .put("hetu.multiple-coordinator.enabled", "true")
                .put("hetu.multiple-coordinator.query-submit-timeout", "20s")
                .put("hetu.multiple-coordinator.state-expire-time", "20s")
                .put("hetu.multiple-coordinator.state-fetch-interval", "5s")
                .put("hetu.multiple-coordinator.state-update-interval", "5s")
                .put("hetu.data.center.split.count", "10")
                .put("hetu.data.center.consumer.timeout", "5m")
                .put("hetu.split-cache-map.enabled", "true")
                .put("hetu.split-cache-map.state-update-interval", "5s")
                .put("stack-trace-visible", "true")
                .put("hetu.heuristicindex.filter.cache.preload-indices", "idx1,idx2")
                .build();

        HetuConfig expected = new HetuConfig()
                .setFilterEnabled(true)
                .setIndexStoreUri("/tmp")
                .setIndexStoreFileSystemProfile("index-test")
                .setIndexCacheMaxMemory(new DataSize(2, GIGABYTE))
                .setIndexCacheTTL(new Duration(20, TimeUnit.MINUTES))
                .setIndexCacheLoadingThreads(5L)
                .setIndexCacheLoadingDelay(new Duration(1000, TimeUnit.MILLISECONDS))
                .setIndexCacheSoftReferenceEnabled(false)
                .setExecutionPlanCacheEnabled(true)
                .setExecutionPlanCacheTimeout(6000L)
                .setExecutionPlanCacheMaxItems(20000L)
                .setEmbeddedStateStoreEnabled(true)
                .setMultipleCoordinatorEnabled(true)
                .setQuerySubmitTimeout(new Duration(20, TimeUnit.SECONDS))
                .setStateExpireTime(new Duration(20, TimeUnit.SECONDS))
                .setStateFetchInterval(new Duration(5, TimeUnit.SECONDS))
                .setStateUpdateInterval(new Duration(5, TimeUnit.SECONDS))
                .setDataCenterSplits(10)
                .setDataCenterConsumerTimeout(new Duration(5, TimeUnit.MINUTES))
                .setSplitCacheMapEnabled(true)
                .setSplitCacheStateUpdateInterval(new Duration(5, TimeUnit.SECONDS))
                .setTraceStackVisible(true)
                .setIndexToPreload("idx1,idx2");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
