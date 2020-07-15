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
import java.util.concurrent.TimeUnit;

import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_PORT_CONFIG_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test for HazelcastClusterMembershipListener
 *
 * @since 2020-03-06
 */
public class TestHazelcastClusterMembershipListener
{
    private static final String TEST_CLUSTER_NAME = "test-membership-listener-" + UUID.randomUUID();
    private static final String LOCALHOST = "127.0.0.1";
    private static final String PORT1 = "5709";
    private static final String PORT2 = "5710";
    private static final String MEMBER_1_ADDRESS = LOCALHOST + ":" + PORT1;
    private static final String MEMBER_2_ADDRESS = LOCALHOST + ":" + PORT2;
    boolean isNotified;

    /**
     * Test Hazelcast member removed
     *
     * @throws InterruptedException InterruptedException when thread sleep interrupted
     */
    @Test
    public void testMemberRemoved()
            throws InterruptedException
    {
        final long sleep500 = 500L;
        final long sleep50 = 50L;
        final long timeout3000 = 3000L;
        StateStore member1 = setupHazelcastInstance(PORT1);
        member1.registerNodeFailureHandler(node -> {
            assertEquals((String) node, MEMBER_2_ADDRESS);
            isNotified = true;
        });
        StateStore member2 = setupHazelcastInstance(PORT2);

        TimeUnit.MILLISECONDS.sleep(sleep500);
        ((HazelcastStateStore) member2).shutdown();

        long timeout = timeout3000;
        while (isNotified == false && timeout > 0L) {
            TimeUnit.MILLISECONDS.sleep(sleep50);
            timeout -= sleep50;
        }
        assertTrue(isNotified);
        ((HazelcastStateStore) member1).shutdown();
    }

    private StateStore setupHazelcastInstance(String port)
    {
        Map<String, String> config = new HashMap<>(0);
        config.put("hazelcast.discovery.mode", "tcp-ip");
        config.put("state-store.cluster", TEST_CLUSTER_NAME);
        config.put(DISCOVERY_PORT_CONFIG_NAME, port);

        StateStoreBootstrapper bootstrapper = new HazelcastStateStoreBootstrapper();
        return bootstrapper.bootstrap(ImmutableSet.of(MEMBER_1_ADDRESS, MEMBER_2_ADDRESS), config);
    }
}
