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

import com.google.common.collect.ImmutableSet;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.hetu.core.statestore.hazelcast.HazelcastStateSet;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateSet;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Base64EncodedStateSet unit test
 *
 * @since 2020-03-04
 */
public class TestBase64EncodedStateSet
{
    private static final String TEST_VALUE_1 = "value1";
    private static final String TEST_VALUE_2 = "value2";
    private static final int PORT = 5712;

    private HazelcastInstance hzInstance;

    /**
     * set up before unit test run
     */
    @BeforeSuite
    private void setup()
    {
        Config config = new Config();
        config.setClusterName("cluster-test-encoded-set-" + UUID.randomUUID());
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
        StateSet<String> originalSet =
                new HazelcastStateSet(hzInstance, "testGetSet", StateCollection.Type.SET);
        EncryptedStateSet<String> encryptedStateSet =
                new EncryptedStateSet(originalSet, new Base64CipherService<String>());
        assertEquals(encryptedStateSet.getName(), "testGetSet");
        assertEquals(encryptedStateSet.getType(), StateCollection.Type.SET);

        encryptedStateSet.add(TEST_VALUE_1);
        assertTrue(encryptedStateSet.contains(TEST_VALUE_1));
        assertFalse(originalSet.contains(TEST_VALUE_1));
        assertEquals(encryptedStateSet.size(), size);
    }

    /**
     * Test object
     */
    @Test
    public void testObject()
    {
        final int value = 1;
        StateSet<Integer> originalSet =
                new HazelcastStateSet(hzInstance, "testObject", StateCollection.Type.SET);
        EncryptedStateSet<Integer> encryptedStateSet =
                new EncryptedStateSet(originalSet, new Base64CipherService<Integer>());
        encryptedStateSet.add(new Integer(value));
        assertTrue(encryptedStateSet.contains(new Integer(value)));
        assertFalse(originalSet.contains(new Integer(value)));
    }

    /**
     * Test remove
     */
    @Test
    public void testRemove()
    {
        StateSet<String> originalSet =
                new HazelcastStateSet(hzInstance, "testRemove", StateCollection.Type.SET);
        EncryptedStateSet<String> encryptedStateSet =
                new EncryptedStateSet(originalSet, new Base64CipherService<String>());
        encryptedStateSet.add(TEST_VALUE_1);
        assertTrue(encryptedStateSet.contains(TEST_VALUE_1));
        boolean isRemoved = encryptedStateSet.remove(TEST_VALUE_1);
        assertTrue(isRemoved);
        assertTrue(encryptedStateSet.isEmpty());
        assertTrue(originalSet.isEmpty());
    }

    /**
     * Test removeAll
     */
    @Test
    public void testRemoveAll()
    {
        StateSet<String> originalSet =
                new HazelcastStateSet(hzInstance, "testRemoveAll", StateCollection.Type.SET);
        EncryptedStateSet<String> encryptedStateSet =
                new EncryptedStateSet(originalSet, new Base64CipherService<String>());
        encryptedStateSet.add(TEST_VALUE_1);
        encryptedStateSet.add(TEST_VALUE_2);
        encryptedStateSet.removeAll(ImmutableSet.of(TEST_VALUE_1, TEST_VALUE_2));
        assertTrue(encryptedStateSet.isEmpty());
        assertTrue(originalSet.isEmpty());
    }

    /**
     * Test clear
     */
    @Test
    public void testClear()
    {
        StateSet<String> originalSet =
                new HazelcastStateSet(hzInstance, "testClear", StateCollection.Type.SET);
        EncryptedStateSet<String> encryptedStateSet =
                new EncryptedStateSet(originalSet, new Base64CipherService<String>());
        encryptedStateSet.add(TEST_VALUE_1);
        assertFalse(encryptedStateSet.isEmpty());
        encryptedStateSet.clear();
        assertTrue(encryptedStateSet.isEmpty());
        assertTrue(originalSet.isEmpty());
    }

    /**
     * Test addAll and getAll
     */
    @Test
    public void testAddAllAndGetAll()
    {
        final int size = 2;
        StateSet<String> originalSet =
                new HazelcastStateSet(hzInstance, "testAddAllAndGetAll", StateCollection.Type.SET);
        EncryptedStateSet<String> encryptedStateSet =
                new EncryptedStateSet(originalSet, new Base64CipherService<String>());
        encryptedStateSet.addAll(ImmutableSet.of(TEST_VALUE_1, TEST_VALUE_2));
        assertEquals(encryptedStateSet.size(), size);
        assertTrue(encryptedStateSet.contains(TEST_VALUE_1));

        Set<String> values = encryptedStateSet.getAll();
        assertEquals(values.size(), size);
        assertTrue(values.contains(TEST_VALUE_1));
        assertTrue(values.contains(TEST_VALUE_2));
    }
}
