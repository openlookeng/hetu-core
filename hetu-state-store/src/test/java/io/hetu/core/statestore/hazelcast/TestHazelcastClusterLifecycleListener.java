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
import io.prestosql.spi.seedstore.Seed;
import io.prestosql.spi.seedstore.SeedStore;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.statestore.StateStoreBootstrapper;
import io.prestosql.spi.statestore.StateStoreFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_PORT_CONFIG_NAME;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test class for HazelcastClusterLifecycleListener
 *
 * @since 2020-03-24
 */
public class TestHazelcastClusterLifecycleListener
{
    private static final String TEST_CLUSTER_NAME = "test-lifecycle-listener-" + UUID.randomUUID();
    private static final String LOCALHOST = "127.0.0.1";
    private static final String PORT1 = "5715";
    private static final String PORT2 = "5716";
    private static final String MEMBER_1_ADDRESS = LOCALHOST + ":" + PORT1;
    private static final String MEMBER_2_ADDRESS = LOCALHOST + ":" + PORT2;
    private boolean isClusterShutdownEventCaptured;

    @Test
    void testClusterShutdown()
            throws InterruptedException
    {
        final long sleep = 30000L;
        isClusterShutdownEventCaptured = false;
        final StateStore member1 = createStateStoreCluster(PORT1);
        final StateStore member2 = createStateStoreCluster(PORT2);
        HashSet<Seed> seeds = new HashSet<>(0);
        seeds.add(new MockSeed(MEMBER_1_ADDRESS));
        seeds.add(new MockSeed(MEMBER_2_ADDRESS));
        StateStore client = createStateStoreClient(seeds);
        assertFalse(isClusterShutdownEventCaptured);
        ((HazelcastStateStore) member1).shutdown();
        assertFalse(isClusterShutdownEventCaptured);
        ((HazelcastStateStore) member2).shutdown();
        Thread.sleep(sleep);
        assertTrue(isClusterShutdownEventCaptured);
    }

    private StateStore createStateStoreClient(Set<Seed> seeds)
    {
        Map<String, String> config = new HashMap<>(0);
        config.put("hazelcast.discovery.mode", "tcp-ip");
        config.put("state-store.cluster", TEST_CLUSTER_NAME);
        config.put(DISCOVERY_PORT_CONFIG_NAME, PORT1);

        MockSeedStore mockSeedStore = new MockSeedStore();
        mockSeedStore.add(seeds);
        StateStoreFactory factory = new HazelcastStateStoreFactory();
        StateStore stateStore = factory.create("testHazelcast", mockSeedStore, config);
        stateStore.registerClusterFailureHandler(event -> isClusterShutdownEventCaptured = true);
        return stateStore;
    }

    private StateStore createStateStoreCluster(String port)
    {
        Map<String, String> config = new HashMap<>(0);
        config.put("hazelcast.discovery.mode", "tcp-ip");
        config.put("state-store.cluster", TEST_CLUSTER_NAME);
        config.put(DISCOVERY_PORT_CONFIG_NAME, port);

        StateStoreBootstrapper bootstrapper = new HazelcastStateStoreBootstrapper();
        return bootstrapper.bootstrap(ImmutableSet.of(MEMBER_1_ADDRESS, MEMBER_2_ADDRESS), config);
    }

    /**
     * MockSeed used for TestHazelcastClusterLifecycleListener
     *
     * @since 2020-03-24
     */
    class MockSeed
            implements Seed
    {
        private static final long serialVersionUID = 4L;

        String location;
        long timeStamp;

        /**
         * Constructor for the mock seed
         *
         * @param location host location of this seed
         */
        MockSeed(String location)
        {
            this.location = location;
            timeStamp = 0L;
        }

        @Override
        public String getLocation()
        {
            return location;
        }

        @Override
        public long getTimestamp()
        {
            return 0L;
        }

        @Override
        public String serialize()
                throws IOException
        {
            return "MOCK SEED. SHOULD NOT SERIALIZE.";
        }

        public void setTimeStamp(long timeStamp)
        {
            this.timeStamp = timeStamp;
        }
    }

    /**
     * MockSeedStore used for TestHazelcastClusterLifecycleListener
     *
     * @since 2020-03-24
     */
    class MockSeedStore
            implements SeedStore
    {
        private static final int INITIAL_SIZE = 2;
        private Set<Seed> seeds;

        /**
         * Constructor for the mock seed store
         */
        MockSeedStore()
        {
            this.seeds = new HashSet<>(INITIAL_SIZE);
        }

        @Override
        public Collection<Seed> add(Collection<Seed> seedsToAdd)
        {
            this.seeds.addAll(seedsToAdd);
            return this.seeds;
        }

        @Override
        public Collection<Seed> get()
        {
            return seeds;
        }

        @Override
        public Collection<Seed> remove(Collection<Seed> seedsToRemove)
        {
            this.seeds.removeAll(seedsToRemove);
            return this.seeds;
        }

        @Override
        public Seed create(Map<String, String> properties)
        {
            String location = properties.get(Seed.LOCATION_PROPERTY_NAME);
            return new MockSeed(location);
        }

        @Override
        public String getName()
        {
            return "mock";
        }

        @Override
        public void setName(String name)
        {
        }
    }
}
