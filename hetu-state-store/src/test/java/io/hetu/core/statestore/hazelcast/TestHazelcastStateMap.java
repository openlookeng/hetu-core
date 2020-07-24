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
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.airlift.slice.Slice;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.statestore.listener.EntryAddedListener;
import io.prestosql.spi.statestore.listener.EntryRemovedListener;
import io.prestosql.spi.statestore.listener.EntryUpdatedListener;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.statestore.StateCollection.Type;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Test for Hazelcast StateMap
 *
 * @since 2019-11-29
 */
public class TestHazelcastStateMap
{
    private static final String STATE_STORE_NAME = "test";
    private static final Type STATE_COLLECTION_TYPE = Type.MAP;
    private static final String TEST_KEY1 = "testKey1";
    private static final String TEST_KEY2 = "testKey2";
    private static final String TEST_VALUE1 = "testValue1";
    private static final String TEST_VALUE2 = "testValue2";
    private static final String NOT_EXIST = "notExist";
    private static final int PORT = 5702;

    private HazelcastInstance hzInstance;
    private StateStore stateStore;

    /**
     * set up before unit test run
     */
    @BeforeSuite
    private void setup()
    {
        Config config = new Config();
        SerializerConfig sc = new SerializerConfig().setImplementation(new HazelCastSliceSerializer()).setTypeClass(Slice.class);
        config.getSerializationConfig().addSerializerConfig(sc);
        config.setClusterName("cluster-test-map-" + UUID.randomUUID());
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
     * Test get
     */
    @Test
    public void testGet()
    {
        final int size0 = 0;
        final int size1 = 1;
        StateMap<String, String> stateMap = setUpStateMap("TestGet");
        assertEquals(stateMap.size(), size0);
        assertTrue(stateMap.isEmpty());
        stateMap.put(TEST_KEY1, TEST_VALUE1);
        assertTrue(stateMap.containsKey(TEST_KEY1));
        assertEquals(stateMap.get(TEST_KEY1), TEST_VALUE1);
        assertEquals(stateMap.size(), size1);
    }

    /**
     * Test getAll
     */
    @Test
    public void testGetAll()
    {
        final int size = 2;
        StateMap<String, String> stateMap = setUpStateMap("TestGetAll");
        stateMap.put(TEST_KEY1, TEST_VALUE1);
        stateMap.put(TEST_KEY2, TEST_VALUE2);
        Map states = stateMap.getAll();
        assertEquals(states.size(), size);
        assertEquals(states.get(TEST_KEY1), TEST_VALUE1);
        assertEquals(states.get(TEST_KEY2), TEST_VALUE2);
    }

    /**
     * Test getAllMatching
     */
    @Test
    public void testGetAllMatching()
    {
        StateMap<String, String> stateMap = setUpStateMap("TestGetAllMatching");
        stateMap.put(TEST_KEY1, TEST_VALUE1);
        stateMap.put(TEST_KEY2, TEST_VALUE2);
        Map states = stateMap.getAll(ImmutableSet.of(TEST_KEY1, NOT_EXIST));
        assertEquals(states.size(), 1);
        assertEquals(states.get(TEST_KEY1), TEST_VALUE1);
        assertNull(states.get(NOT_EXIST));
    }

    /**
     * Test putIfAbsent
     */
    @Test
    public void testPutIfAbsent()
    {
        StateMap<String, String> stateMap = setUpStateMap("TestPutIfAbsent");
        assertNull(stateMap.putIfAbsent(TEST_KEY1, TEST_VALUE1));
        assertEquals(stateMap.putIfAbsent(TEST_KEY1, TEST_VALUE2), TEST_VALUE1);
        assertEquals(stateMap.get(TEST_KEY1), TEST_VALUE1);
    }

    /**
     * Test putAll
     */
    @Test
    public void testPutAll()
    {
        StateMap<String, String> stateMap = setUpStateMap("TestPutAll");
        Map<String, String> map = new HashMap<>(0);
        map.put(TEST_KEY1, TEST_VALUE1);
        map.put(TEST_KEY2, TEST_VALUE2);
        stateMap.putAll(map);
        assertEquals(stateMap.get(TEST_KEY1), TEST_VALUE1);
        assertEquals(stateMap.get(TEST_KEY2), TEST_VALUE2);
        assertNull(stateMap.get(NOT_EXIST));
    }

    /**
     * Test Hazelcast Slice serializer
     */
    @Test
    public void testSliceSerializer()
    {
        Slice s3 = utf8Slice("test3");

        Slice s1 = utf8Slice("test1");
        StateMap<String, Slice> ss = (StateMap<String, Slice>) stateStore.createStateCollection("slicecheck", STATE_COLLECTION_TYPE);
        ss.put("s1", s1);
        Slice s2 = ss.get("s1");
        assertEquals(s1, s2);
    }

    /**
     * Test remove
     */
    @Test
    public void testRemove()
    {
        StateMap<String, String> stateMap = setUpStateMap("TestRemove");
        stateMap.put(TEST_KEY1, TEST_VALUE1);
        assertEquals(stateMap.remove(TEST_KEY1), TEST_VALUE1);
        assertNull(stateMap.get(TEST_KEY1));
    }

    /**
     * Test removeAll
     */
    @Test
    public void testRemoveAll()
    {
        StateMap<String, String> stateMap = setUpStateMap("TestRemoveAll");
        stateMap.put(TEST_KEY1, TEST_VALUE1);
        stateMap.put(TEST_KEY2, TEST_VALUE2);

        stateMap.removeAll(ImmutableSet.of(TEST_KEY1, TEST_KEY2));
        assertNull(stateMap.get(TEST_KEY1));
        assertNull(stateMap.get(TEST_KEY2));
    }

    /**
     * Test replace
     */
    @Test
    public void testReplace()
    {
        StateMap<String, String> stateMap = setUpStateMap("TestReplace");
        stateMap.put(TEST_KEY1, TEST_VALUE1);
        assertEquals(stateMap.get(TEST_KEY1), TEST_VALUE1);
        stateMap.replace(TEST_KEY1, TEST_VALUE2);
        assertEquals(stateMap.get(TEST_KEY1), TEST_VALUE2);
        assertNull(stateMap.replace(TEST_KEY2, TEST_VALUE1));
        assertNull(stateMap.get(TEST_KEY2));
    }

    /**
     * Test clear
     */
    @Test
    public void testClear()
    {
        StateMap<String, String> stateMap = setUpStateMap("TestClear");
        stateMap.put(TEST_KEY1, TEST_VALUE1);
        stateMap.put(TEST_KEY2, TEST_VALUE2);
        assertEquals(stateMap.get(TEST_KEY1), TEST_VALUE1);
        assertEquals(stateMap.get(TEST_KEY2), TEST_VALUE2);

        stateMap.clear();
        assertNull(stateMap.get(TEST_KEY1));
        assertNull(stateMap.get(TEST_KEY2));
    }

    /**
     * Test destroy
     */
    @Test
    public void testDestroy()
    {
        final int size = hzInstance.getDistributedObjects().size();
        StateMap<String, String> stateMap = setUpStateMap("TestDestroy");
        assertEquals(hzInstance.getDistributedObjects().size(), size + 1);
        boolean isContain = hzInstance.getDistributedObjects().stream().map(object -> object.getName()).collect(Collectors.toList()).contains("TestDestroy");
        assertEquals(isContain, true);
        stateMap.destroy();
        assertEquals(hzInstance.getDistributedObjects().size(), size);
        isContain = hzInstance.getDistributedObjects().stream().map(object -> object.getName()).collect(Collectors.toList()).contains("TestDestroy");
        assertEquals(isContain, false);
    }

    /**
     * Test getName
     */
    @Test
    public void testGetName()
    {
        assertEquals(stateStore.getName(), STATE_STORE_NAME);
    }

    @Test
    public void testListeners()
            throws InterruptedException
    {
        final CountDownLatch listenersCount = new CountDownLatch(3);
        EntryAddedListener<String, String> addedListener = event -> {
            if (event.getKey().equals("Key") && event.getValue().equals("Created")) {
                listenersCount.countDown();
            }
        };

        EntryUpdatedListener<String, String> updated = event -> {
            if (event.getKey().equals("Key") && event.getOldValue().equals("Created") && event.getValue().equals("Updated")) {
                listenersCount.countDown();
            }
        };

        EntryRemovedListener<String, String> removedListener = event -> {
            if (event.getKey().equals("Key") && event.getOldValue().equals("Updated")) {
                listenersCount.countDown();
            }
        };

        StateMap<String, String> testMap = stateStore.createStateMap("Test_Listeners_Map", addedListener, removedListener, updated);
        testMap.put("Key", "Created");
        testMap.put("Key", "Updated");
        testMap.remove("Key");

        assertTrue(listenersCount.await(500, TimeUnit.MILLISECONDS), "Events were not triggered or values did not match");
    }

    @Test
    public void testAddAndRemoveListeners()
            throws InterruptedException
    {
        AtomicInteger count = new AtomicInteger();
        EntryAddedListener<String, String> addedListener = event -> {
            if (event.getKey().equals("Key") && event.getValue().equals("Created")) {
                count.getAndIncrement();
            }
        };

        StateMap<String, String> testMap = stateStore.createStateMap("Test_Add_Remove_Listeners_Map");
        testMap.addEntryListener(addedListener);
        testMap.put("Key", "Created");
        TimeUnit.MILLISECONDS.sleep(500);
        assertEquals(count.get(), 1, "Events were not triggered or values did not match");

        testMap.remove("Key");
        testMap.removeEntryListener(addedListener);
        testMap.put("Key", "Created");
        TimeUnit.MILLISECONDS.sleep(500);
        assertEquals(count.get(), 1, "Events triggered again when listener removed");
    }

    /**
     * Create separate StateCollection for each testcase
     */
    private StateMap<String, String> setUpStateMap(String collectionName)
    {
        stateStore.createStateCollection(collectionName, STATE_COLLECTION_TYPE);
        return (StateMap<String, String>) stateStore.getStateCollection(collectionName);
    }
}
