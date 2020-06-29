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

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.prestosql.spi.statestore.StateSet;
import io.prestosql.spi.statestore.StateStore;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.prestosql.spi.statestore.StateCollection.Type;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test for HazelcastState Set
 *
 * @since 2019-11-29
 */
public class TestHazelcastStateSet
{
    private static final String STATE_STORE_NAME = "test";
    private static final Type STATE_COLLECTION_TYPE = Type.SET;
    private static final String TEST_VALUE1 = "testValue1";
    private static final String TEST_VALUE2 = "testValue2";
    private static final String TEST_VALUE3 = "testValue3";
    private static final int PORT = 5703;

    private HazelcastInstance hzInstance;
    private StateStore stateStore;

    /**
     * set up before unit test run
     */
    @BeforeSuite
    private void setup()
    {
        Config config = new Config();
        config.setClusterName("cluster-test-set-" + UUID.randomUUID());
        // Specify ports to make sure different test cases won't connect to same cluster
        NetworkConfig network = config.getNetworkConfig();
        network.setPortAutoIncrement(false);
        network.setPort(PORT);
        hzInstance = Hazelcast.newHazelcastInstance(config);
        stateStore = new HazelcastStateStore(hzInstance, STATE_STORE_NAME);
    }

    /**
     * Tear down after unit test run
     */
    @AfterSuite
    private void tearDown()
            throws InterruptedException
    {
        final long sleep = 500L;
        hzInstance.shutdown();
        TimeUnit.MILLISECONDS.sleep(sleep);
    }

    /**
     * Test add and get
     */
    @Test
    public void testAddAndGet()
    {
        final int size1 = 1;
        final int size2 = 2;
        StateSet<String> stateSet = setUpStateSet("TestGet");
        assertTrue(stateSet.isEmpty());
        stateSet.add(TEST_VALUE1);
        assertEquals(stateSet.contains(TEST_VALUE1), true);
        assertEquals(stateSet.size(), size1);
        stateSet.add(TEST_VALUE1);
        assertEquals(stateSet.size(), size1);
        stateSet.add(TEST_VALUE2);
        assertEquals(stateSet.size(), size2);
    }

    /**
     * Test getALl
     */
    @Test
    public void testGetAll()
    {
        final int size = 2;
        StateSet<String> stateSet = setUpStateSet("TestGetAll");
        HashSet<String> testSet = new HashSet<>(0);
        testSet.add(TEST_VALUE1);
        testSet.add(TEST_VALUE2);
        stateSet.addAll(testSet);
        assertEquals(stateSet.size(), size);
        assertEquals(stateSet.contains(TEST_VALUE1), true);
        assertEquals(stateSet.contains(TEST_VALUE2), true);
        assertEquals(hzInstance.getSet("TestGetAll").contains(TEST_VALUE1), true);
        assertEquals(stateSet.contains(TEST_VALUE2), true);
    }

    /**
     * Test remove
     */
    @Test
    public void testRemove()
    {
        final int size = 1;
        StateSet<String> stateSet = setUpStateSet("TestRemove");
        stateSet.add(TEST_VALUE1);
        stateSet.add(TEST_VALUE2);
        assertEquals(stateSet.remove(TEST_VALUE1), true);
        assertEquals(stateSet.size(), size);
        assertEquals(hzInstance.getSet("TestRemove").size(), size);
    }

    /**
     * Test removeAll
     */
    @Test
    public void testRemoveAll()
    {
        final int size = 1;
        StateSet<String> stateSet = setUpStateSet("TestRemoveAll");
        stateSet.add(TEST_VALUE1);
        stateSet.add(TEST_VALUE2);
        stateSet.add(TEST_VALUE3);
        HashSet tests = new HashSet(0);
        tests.add(TEST_VALUE1);
        tests.add(TEST_VALUE2);

        assertEquals(stateSet.removeAll(tests), true);
        assertEquals(stateSet.size(), size);
        assertEquals(hzInstance.getSet("TestRemoveAll").size(), size);
    }

    /**
     * Test getName and getType
     */
    @Test
    public void testGetNameAndType()
    {
        StateSet<String> stateSet = setUpStateSet("TestGetName");
        assertEquals(stateStore.getName(), STATE_STORE_NAME);
        assertEquals(stateSet.getName(), "TestGetName");
        assertEquals(stateSet.getType(), Type.SET);
    }

    /**
     * Test clear
     */
    @Test
    public void testClear()
    {
        final int size = 3;
        StateSet<String> stateSet = setUpStateSet("TestClear");
        stateSet.add(TEST_VALUE1);
        stateSet.add(TEST_VALUE2);
        stateSet.add(TEST_VALUE3);
        assertEquals(stateSet.size(), size);
        stateSet.clear();
        assertTrue(stateSet.isEmpty());
    }

    /**
     * Test destroy
     */
    @Test
    public void testDestroy()
    {
        final int size = hzInstance.getDistributedObjects().size();
        StateSet<String> stateSet = setUpStateSet("TestDestroy");
        assertEquals(hzInstance.getDistributedObjects().size(), size + 1);
        boolean isContain = hzInstance.getDistributedObjects().stream().map(object -> object.getName()).collect(Collectors.toList()).contains("TestDestroy");
        assertEquals(isContain, true);
        stateSet.destroy();
        assertEquals(hzInstance.getDistributedObjects().size(), size);
        isContain = hzInstance.getDistributedObjects().stream().map(object -> object.getName()).collect(Collectors.toList()).contains("TestDestroy");
        assertEquals(isContain, false);
    }

    /**
     * Create separate StateCollection for each testcase
     */
    private StateSet<String> setUpStateSet(String collectionName)
    {
        stateStore.createStateCollection(collectionName, STATE_COLLECTION_TYPE);
        return (StateSet<String>) stateStore.getStateCollection(collectionName);
    }
}
