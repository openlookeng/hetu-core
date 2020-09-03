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
package io.hetu.core.security.authentication;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class TestHazelcastAuthenticationDisabled
{
    private static final int PORT1 = 5710;
    private static final int PORT2 = 5711;
    private HazelcastInstance hazelcastInstance1;
    private HazelcastInstance hazelcastInstance2;

    @BeforeSuite
    public void setup()
    {
        String clusterName = "cluster-" + UUID.randomUUID();

        Config config1 = new Config();
        config1.setClusterName(clusterName);
        NetworkConfig network = config1.getNetworkConfig();
        network.setPortAutoIncrement(false);
        network.setPort(PORT1);
        hazelcastInstance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setClusterName(clusterName);
        network = config2.getNetworkConfig();
        network.setPortAutoIncrement(false);
        network.setPort(PORT2);
        hazelcastInstance2 = Hazelcast.newHazelcastInstance(config2);
    }

    @AfterSuite
    private void tearDown()
    {
        hazelcastInstance1.shutdown();
        hazelcastInstance2.shutdown();
    }

    @Test
    public void testHazelcastAuthenticationDisabled()
    {
        String value1 = "aaa";
        String value2 = "bbb";

        Config config = new Config();
        HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance(config);
        Map<Integer, String> clusterMap1 = hazelcastInstance1.getMap("MyMap");
        clusterMap1.put(1, value1);

        HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance(config);
        Map<Integer, String> clusterMap2 = hazelcastInstance2.getMap("MyMap");
        clusterMap2.put(2, value2);

        assertEquals(clusterMap1.get(2), value2);
        assertEquals(clusterMap2.get(1), value1);
    }
}
