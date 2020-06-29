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
package io.hetu.core.statestore;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.hetu.core.statestore.hazelcast.HazelcastStateMap;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Base64EncodedStateMap unit test
 *
 * @since 2020-03-04
 */
public class TestBase64EncodedStateMap
{
    private static final String TEST_KEY_1 = "key1";
    private static final String TEST_KEY_2 = "key2";
    private static final String TEST_VALUE_1 = "value1";
    private static final String TEST_VALUE_2 = "value2";
    private static final int PORT = 5713;

    private HazelcastInstance hzInstance;

    /**
     * set up before unit test run
     */
    @BeforeSuite
    private void setup()
    {
        Config config = new Config();
        config.setClusterName("cluster-test-encoded-map-" + UUID.randomUUID());
        // Specify ports to make sure different test cases won't connect to same cluster
        NetworkConfig network = config.getNetworkConfig();
        network.setPortAutoIncrement(false);
        network.setPort(PORT);
        hzInstance = Hazelcast.newHazelcastInstance(config);
    }

    /**
     * Tear down after unit test run
     */
    @AfterSuite
    private void tearDown()
    {
        hzInstance.shutdown();
    }

    /**
     * Test base
     */
    @Test
    public void testBase()
    {
        final int size = 1;
        StateMap<String, String> originalMap =
                new HazelcastStateMap(hzInstance, "testGetMap");
        EncryptedStateMap<String, String> encryptedStateMap =
                new EncryptedStateMap(originalMap, new Base64CipherService<String>());
        assertEquals(encryptedStateMap.getName(), "testGetMap");
        assertEquals(encryptedStateMap.getType(), StateCollection.Type.MAP);

        encryptedStateMap.put(TEST_KEY_1, TEST_VALUE_1);
        assertTrue(encryptedStateMap.containsKey(TEST_KEY_1));
        assertEquals(encryptedStateMap.get(TEST_KEY_1), TEST_VALUE_1);
        assertNotEquals(originalMap.get(TEST_KEY_1), TEST_VALUE_1);

        encryptedStateMap.putIfAbsent(TEST_KEY_1, TEST_VALUE_2);
        assertEquals(encryptedStateMap.size(), size);
        assertEquals(encryptedStateMap.get(TEST_KEY_1), TEST_VALUE_1);
    }

    /**
     * Test object
     */
    @Test
    public void testObject()
    {
        final int value = 1;
        StateMap<String, String> originalMap =
                new HazelcastStateMap(hzInstance, "testObject");
        EncryptedStateMap<String, Integer> encryptedStateMap =
                new EncryptedStateMap(originalMap, new Base64CipherService<Integer>());
        encryptedStateMap.put(TEST_KEY_1, new Integer(value));
        assertEquals(encryptedStateMap.get(TEST_KEY_1), new Integer(value));
        assertNotEquals(originalMap.get(TEST_KEY_1), new Integer(value));
    }

    /**
     * Test remove
     */
    @Test
    public void testRemove()
    {
        StateMap<String, String> originalMap =
                new HazelcastStateMap(hzInstance, "testRemove");
        EncryptedStateMap<String, String> encryptedStateMap =
                new EncryptedStateMap(originalMap, new Base64CipherService<String>());
        encryptedStateMap.put(TEST_KEY_1, TEST_VALUE_1);
        String removed = encryptedStateMap.remove(TEST_KEY_1);
        assertEquals(removed, TEST_VALUE_1);
        assertTrue(encryptedStateMap.isEmpty());
        assertTrue(originalMap.isEmpty());
    }

    /**
     * Test removeAll
     */
    @Test
    public void testRemoveAll()
    {
        StateMap<String, String> originalMap =
                new HazelcastStateMap(hzInstance, "testRemoveAll");
        EncryptedStateMap<String, String> encryptedStateMap =
                new EncryptedStateMap(originalMap, new Base64CipherService<String>());
        encryptedStateMap.put(TEST_KEY_1, TEST_VALUE_1);
        encryptedStateMap.put(TEST_KEY_2, TEST_VALUE_2);
        encryptedStateMap.removeAll(ImmutableSet.of(TEST_KEY_1, TEST_KEY_2));
        assertTrue(encryptedStateMap.isEmpty());
        assertTrue(originalMap.isEmpty());
    }

    /**
     * Test clear
     */
    @Test
    public void testClear()
    {
        StateMap<String, String> originalMap =
                new HazelcastStateMap(hzInstance, "testClear");
        EncryptedStateMap<String, String> encryptedStateMap =
                new EncryptedStateMap(originalMap, new Base64CipherService<String>());
        encryptedStateMap.put(TEST_KEY_1, TEST_VALUE_1);
        assertFalse(encryptedStateMap.isEmpty());
        encryptedStateMap.clear();
        assertTrue(encryptedStateMap.isEmpty());
        assertTrue(originalMap.isEmpty());
    }

    /**
     * Test put and getAll
     */
    @Test
    public void testPutAndGetAll()
    {
        final int mapSize = 2;
        final int valueSize = 1;
        StateMap<String, String> originalMap =
                new HazelcastStateMap(hzInstance, "testPutAndGetALL");
        EncryptedStateMap<String, String> encryptedStateMap =
                new EncryptedStateMap(originalMap, new Base64CipherService<String>());
        encryptedStateMap.putAll(ImmutableMap.of(TEST_KEY_1, TEST_VALUE_1, TEST_KEY_2, TEST_VALUE_2));
        assertEquals(encryptedStateMap.size(), mapSize);
        assertEquals(encryptedStateMap.get(TEST_KEY_1), TEST_VALUE_1);
        assertNotEquals(originalMap.get(TEST_KEY_1), TEST_VALUE_1);

        Map<String, String> values = encryptedStateMap.getAll();
        assertEquals(values.get(TEST_KEY_1), TEST_VALUE_1);
        assertNotEquals(originalMap.get(TEST_KEY_1), TEST_VALUE_1);
        assertEquals(values.get(TEST_KEY_2), TEST_VALUE_2);
        assertNotEquals(originalMap.get(TEST_KEY_2), TEST_VALUE_2);

        values = encryptedStateMap.getAll(ImmutableSet.of(TEST_KEY_2));
        assertEquals(values.size(), valueSize);
        assertEquals(values.get(TEST_KEY_2), TEST_VALUE_2);
        assertFalse(values.containsKey(TEST_KEY_1));
    }

    /**
     * Test replace
     */
    @Test
    public void testReplace()
    {
        StateMap<String, String> originalMap =
                new HazelcastStateMap(hzInstance, "testReplace");
        EncryptedStateMap<String, String> encryptedStateMap =
                new EncryptedStateMap(originalMap, new Base64CipherService<String>());
        encryptedStateMap.put(TEST_KEY_1, TEST_VALUE_1);
        String originalValue = encryptedStateMap.replace(TEST_KEY_1, TEST_VALUE_2);
        assertEquals(encryptedStateMap.size(), 1);
        assertEquals(encryptedStateMap.get(TEST_KEY_1), TEST_VALUE_2);
        assertEquals(originalValue, TEST_VALUE_1);
        originalValue = encryptedStateMap.replace(TEST_KEY_2, TEST_VALUE_2);
        assertNull(originalValue);
    }
}
