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
package io.hetu.core.statestore.hazelcast;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.statestore.StateStoreBootstrapper;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_MODE_CONFIG_NAME;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_PORT_CONFIG_NAME;
import static org.testng.Assert.assertTrue;

/**
 * Test for HazelcastStateStoreBootstrapper
 *
 * @since 2020-03-04
 */
public class TestHazelcastStateStoreBootstrapper
{
    private static final String LOCALHOST = "127.0.0.1";
    private static final String PORT = "5707";
    private static final String PORT2 = "5715";

    /**
     * Test Bootstrap
     */
    @Test
    public void testBootstrap()
    {
        Map<String, String> config = new HashMap<>(0);
        config.put(DISCOVERY_MODE_CONFIG_NAME, "tcp-ip");
        config.put("state-store.cluster", "cluster-" + UUID.randomUUID());
        config.put("hazelcast.cp-system.member-count", "3");
        config.put(DISCOVERY_PORT_CONFIG_NAME, PORT);

        StateStoreBootstrapper bootstrapper = new HazelcastStateStoreBootstrapper();
        StateStore stateStore = bootstrapper.bootstrap(ImmutableSet.of(LOCALHOST), config);
        assertTrue(stateStore instanceof HazelcastStateStore);
        ((HazelcastStateStore) stateStore).shutdown();
    }

    /**
     * Test MulticastDiscovery
     */
    @Test
    public void testMulticastDiscovery()
    {
        Map<String, String> config = new HashMap<>(0);
        config.put(DISCOVERY_MODE_CONFIG_NAME, "multicast");
        config.put("state-store.cluster", "cluster");
        config.put(DISCOVERY_PORT_CONFIG_NAME, PORT2);

        StateStoreBootstrapper bootstrapper = new HazelcastStateStoreBootstrapper();
        StateStore stateStore = bootstrapper.bootstrap(ImmutableSet.of(LOCALHOST), config);
        assertTrue(stateStore instanceof HazelcastStateStore);
        ((HazelcastStateStore) stateStore).shutdown();
    }

    /**
     * Test unsupportedDiscovery
     */
    @Test(expectedExceptions = RuntimeException.class)
    public void testUnsupportedDiscovery()
    {
        Map<String, String> config = new HashMap<>(0);
        config.put(DISCOVERY_MODE_CONFIG_NAME, "zookeeper");

        StateStoreBootstrapper bootstrapper = new HazelcastStateStoreBootstrapper();
        bootstrapper.bootstrap(ImmutableSet.of(LOCALHOST), config);
    }
}
