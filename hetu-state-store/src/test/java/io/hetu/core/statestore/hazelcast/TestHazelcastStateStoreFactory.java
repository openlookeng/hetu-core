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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.hetu.core.seedstore.filebased.FileBasedSeed;
import io.hetu.core.seedstore.filebased.FileBasedSeedStoreFactory;
import io.prestosql.spi.seedstore.Seed;
import io.prestosql.spi.seedstore.SeedStore;
import io.prestosql.spi.seedstore.SeedStoreFactory;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.statestore.StateStoreBootstrapper;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.hetu.core.statestore.Constants.STATE_STORE_CLUSTER_CONFIG_NAME;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_MODE_CONFIG_NAME;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_MODE_TCPIP;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_PORT_CONFIG_NAME;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_TCPIP_SEEDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Test for HazelcastStateStoreFactory
 *
 * @since 2019-11-29
 */
public class TestHazelcastStateStoreFactory
{
    private static final String LOCALHOST = "127.0.0.1";
    private static final String PORT = "5708";
    private static final String PORT2 = "5728";
    private static final String MEMBER_ADDRESS = LOCALHOST + ":" + PORT;
    private static final String MEMBER_ADDRESS2 = LOCALHOST + ":" + PORT2;
    private static final String TEST_STATE_STORE_NAME = "test-state-store";
    private static final String TEST_CLUSTER_NAME = "cluster-" + UUID.randomUUID();
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VALUE = "test-value";
    private static final String HAZELCAST = "hazelcast";

    /**
     * Test state store creation with tcp-ip mode
     *
     * @throws IOException IOException thrown if seedStore read write errors happen
     */
    @Test
    public void testTcpipCreate()
            throws IOException
    {
        HazelcastStateStoreFactory factory = new HazelcastStateStoreFactory();
        assertEquals(factory.getName(), HAZELCAST);
        SeedStoreFactory seedStoreFactory = new FileBasedSeedStoreFactory();
        assertEquals(seedStoreFactory.getName(), "filebased");

        Map<String, String> properties = new HashMap<>(0);
        properties.put(STATE_STORE_CLUSTER_CONFIG_NAME, TEST_CLUSTER_NAME);
        properties.put(DISCOVERY_MODE_CONFIG_NAME, DISCOVERY_MODE_TCPIP);

        SeedStore seedStore = mock(SeedStore.class);
        Seed seed = new FileBasedSeed.FileBasedSeedBuilder(MEMBER_ADDRESS).build();
        when(seedStore.get()).thenReturn(ImmutableList.of(seed));

        setupHazelcastInstance();

        StateStore stateStore = factory.create(TEST_STATE_STORE_NAME, seedStore, properties);
        assertEquals(stateStore.getName(), TEST_STATE_STORE_NAME);

        StateCollection collection = stateStore.createStateCollection("test", StateCollection.Type.MAP);
        ((StateMap<String, String>) collection).put(TEST_KEY, TEST_VALUE);
        assertEquals(collection.size(), 1);
        assertEquals(((StateMap<String, String>) collection).get(TEST_KEY), TEST_VALUE);

        ((HazelcastStateStore) stateStore).shutdown();
    }

    /**
     * Test state store creation with tcp-ip with static seeds configured
     */
    @Test
    public void testTcpipCreateWithStaticSeeds()
    {
        HazelcastStateStoreFactory factory = new HazelcastStateStoreFactory();
        assertEquals(factory.getName(), HAZELCAST);
        SeedStoreFactory seedStoreFactory = new FileBasedSeedStoreFactory();
        assertEquals(seedStoreFactory.getName(), "filebased");

        // bootstrap state store with static seeds
        Map<String, String> config = new HashMap<>(0);
        config.put(DISCOVERY_MODE_CONFIG_NAME, DISCOVERY_MODE_TCPIP);
        config.put(STATE_STORE_CLUSTER_CONFIG_NAME, "static-seed-cluster");
        config.put(DISCOVERY_PORT_CONFIG_NAME, PORT2);
        config.put(DISCOVERY_TCPIP_SEEDS, MEMBER_ADDRESS2);
        StateStoreBootstrapper bootstrapper = new HazelcastStateStoreBootstrapper();
        bootstrapper.bootstrap(ImmutableSet.of(MEMBER_ADDRESS2), config);

        // create state store client
        String stateStoreName = "static-seed-state-store";
        StateStore stateStore = factory.create(stateStoreName, null, config);
        assertEquals(stateStore.getName(), stateStoreName);

        StateCollection collection = stateStore.createStateCollection("test", StateCollection.Type.MAP);
        ((StateMap<String, String>) collection).put(TEST_KEY, TEST_VALUE);
        assertEquals(collection.size(), 1);
        assertEquals(((StateMap<String, String>) collection).get(TEST_KEY), TEST_VALUE);

        ((HazelcastStateStore) stateStore).shutdown();
    }

    /**
     * Test what happens to hazelcast factory if no seeds get from seed store
     *
     * @throws IOException IOException thrown if seedStore read write errors happen
     */
    @Test(expectedExceptions = RuntimeException.class)
    public void testUnableGetSeeds()
            throws IOException
    {
        HazelcastStateStoreFactory factory = new HazelcastStateStoreFactory();
        assertEquals(factory.getName(), HAZELCAST);
        SeedStoreFactory seedStoreFactory = new FileBasedSeedStoreFactory();
        assertEquals(seedStoreFactory.getName(), "filebased");

        Map<String, String> properties = new HashMap<>(0);
        properties.put(STATE_STORE_CLUSTER_CONFIG_NAME, TEST_CLUSTER_NAME);
        properties.put(DISCOVERY_MODE_CONFIG_NAME, DISCOVERY_MODE_TCPIP);

        SeedStore seedStore = mock(SeedStore.class);
        when(seedStore.get()).thenReturn(ImmutableList.of());

        factory.create(TEST_STATE_STORE_NAME, seedStore, properties);
    }

    /**
     * Test no hazelcast config provided
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNoConfig()
    {
        HazelcastStateStoreFactory factory = new HazelcastStateStoreFactory();
        factory.create(TEST_STATE_STORE_NAME, null, null);
    }

    private void setupHazelcastInstance()
    {
        Map<String, String> config = new HashMap<>(0);
        config.put(DISCOVERY_MODE_CONFIG_NAME, DISCOVERY_MODE_TCPIP);
        config.put(STATE_STORE_CLUSTER_CONFIG_NAME, TEST_CLUSTER_NAME);
        config.put(DISCOVERY_PORT_CONFIG_NAME, PORT);

        StateStoreBootstrapper bootstrapper = new HazelcastStateStoreBootstrapper();
        bootstrapper.bootstrap(ImmutableSet.of(MEMBER_ADDRESS), config);
    }
}
