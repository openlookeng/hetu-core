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

package io.hetu.core.plugin.singledata.shardingsphere;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static org.testng.Assert.assertEquals;

public class TestShardingSphereConfig
{
    @Test
    public void testDefaultConfig()
    {
        ShardingSphereConfig defaultConfig = new ShardingSphereConfig();
        assertEquals(defaultConfig.getZkRetryIntervalMilliseconds(), 500);
        assertEquals(defaultConfig.getZkMaxRetries(), 3);
        assertEquals(defaultConfig.getZkTimeToLiveSeconds(), 60);
        assertEquals(defaultConfig.getZkOperationTimeoutMilliseconds(), 500);
        assertEquals(defaultConfig.getZkDigest(), "");
        assertEquals(defaultConfig.getEtcdTimeToLiveSeconds(), 30L);
        assertEquals(defaultConfig.getEtcdConnectionTimeout(), 30L);
    }

    @Test
    public void testShardingSphereConfig()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("shardingsphere.type", "zookeeper")
                .put("shardingsphere.namespace", "test")
                .put("shardingsphere.server-lists", "localhost:2181")
                .put("shardingsphere.database-name", "test")
                .put("shardingsphere.zookeeper.retry-interval-milliseconds", "300")
                .put("shardingsphere.zookeeper.max-retries", "400")
                .put("shardingsphere.zookeeper.time-to-live-seconds", "400")
                .put("shardingsphere.zookeeper.operation-timeout-milliseconds", "400")
                .put("shardingsphere.zookeeper.digest", "testDigest")
                .put("shardingsphere.etcd.time-to-live-seconds", "800")
                .put("shardingsphere.etcd.connection-timeout", "800")
                .build();

        ShardingSphereConfig expected = new ShardingSphereConfig()
                .setType("zookeeper")
                .setNamespace("test")
                .setServerLists("localhost:2181")
                .setDatabaseName("test")
                .setZkRetryIntervalMilliseconds(300)
                .setZkMaxRetries(400)
                .setZkTimeToLiveSeconds(400)
                .setZkOperationTimeoutMilliseconds(400)
                .setZkDigest("testDigest")
                .setEtcdTimeToLiveSeconds(800L)
                .setEtcdConnectionTimeout(800L);

        assertFullMapping(properties, expected);
    }
}
