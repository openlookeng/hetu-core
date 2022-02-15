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
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateStore;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Test for HazelcastStateStore
 *
 * @since 2019-11-29
 */
public class TestHazelcastStateStore
{
    private static final String STATE_STORE_NAME = "test";
    private static final String STATE_COLLECTION_QUERY = "query";
    private static final String STATE_COLLECTION_RESOURCE_GROUP = "resourceGroup";
    private static final String STATE_COLLECTION_OOM_QUERY = "oom-query";
    private static final int PORT = 5701;
    private static final StateCollection.Type STATE_COLLECTION_TYPE = StateCollection.Type.MAP;
    private HazelcastInstance hzInstance;
    private StateStore stateStore;

    /**
     * set up before unit test run
     */
    @BeforeSuite
    private void setup()
    {
        Config config = new Config();
        config.setClusterName("cluster-" + UUID.randomUUID());
        // Specify ports to make sure different test cases won't connect to same cluster
        NetworkConfig network = config.getNetworkConfig();
        network.setPortAutoIncrement(false);
        network.setPort(PORT);

        hzInstance = Hazelcast.newHazelcastInstance(config);
        stateStore = new HazelcastStateStore(hzInstance, STATE_STORE_NAME);
        stateStore.init();
        stateStore.createStateCollection(STATE_COLLECTION_QUERY, STATE_COLLECTION_TYPE);
        stateStore.createStateCollection(STATE_COLLECTION_RESOURCE_GROUP, STATE_COLLECTION_TYPE);
    }

    /**
     * Tear down after unit test run
     */
    @AfterSuite
    private void tearDown()
            throws InterruptedException
    {
        final long sleep500 = 500L;
        hzInstance.shutdown();
        TimeUnit.MILLISECONDS.sleep(sleep500);
    }

    /**
     * Test getName
     */
    @Test
    public void testGetName()
    {
        assertEquals(stateStore.getName(), STATE_STORE_NAME);
    }

    /**
     * Test getStateCollection
     */
    @Test
    public void testGetStateCollection()
    {
        StateCollection collection = stateStore.getStateCollection(STATE_COLLECTION_QUERY);
        assertNotNull(collection);
        assertEquals(collection.getName(), STATE_COLLECTION_QUERY);
        assertEquals(collection.getType(), STATE_COLLECTION_TYPE);
        assertNull(stateStore.getStateCollection(STATE_COLLECTION_OOM_QUERY));
    }

    /**
     * Test getStateCollections
     */
    @Test
    public void testGetStateCollections()
    {
        Map<String, StateCollection> collections = stateStore.getStateCollections();
        StateCollection resourceGroupCollection = collections.get(STATE_COLLECTION_RESOURCE_GROUP);
        assertEquals(resourceGroupCollection.getName(), STATE_COLLECTION_RESOURCE_GROUP);
        assertEquals(resourceGroupCollection.getType(), STATE_COLLECTION_TYPE);
        StateCollection queryCollection = collections.get(STATE_COLLECTION_QUERY);
        assertEquals(queryCollection.getName(), STATE_COLLECTION_QUERY);
        assertEquals(queryCollection.getType(), STATE_COLLECTION_TYPE);
    }

    @Test
    public void testGetOrCreateCollection()
    {
        String newStateCollection = "new-collection";
        assertNull(stateStore.getStateCollection(newStateCollection));
        StateCollection collection = stateStore.getOrCreateStateCollection(newStateCollection, StateCollection.Type.MAP);
        assertEquals(collection.getName(), newStateCollection);
        assertEquals(collection.getType(), StateCollection.Type.MAP);
        assertEquals(collection, stateStore.getStateCollection(newStateCollection));
        assertEquals(collection, stateStore.getOrCreateStateCollection(newStateCollection, StateCollection.Type.MAP));
    }

    /**
     * Test getLock
     *
     * @throws InterruptedException InterruptedException thrown if thread interrupted
     */
    @Test
    public void testGetLock()
            throws InterruptedException
    {
        final long sleep100 = 100L;
        Lock lock = stateStore.getLock(STATE_STORE_NAME);
        assertTrue(lock.tryLock());

        Thread t1 = new Thread(() -> {
            Lock lock1 = stateStore.getLock(STATE_STORE_NAME);
            assertFalse(lock1.tryLock());
        });
        t1.setName("t1");
        t1.setUncaughtExceptionHandler((tr, ex) -> System.out.println(tr.getName() + " : " + ex.getMessage()));
        t1.start();

        TimeUnit.MILLISECONDS.sleep(sleep100);
        lock.unlock();

        Thread t2 = new Thread(() -> {
            Lock lock1 = stateStore.getLock(STATE_STORE_NAME);
            assertTrue(lock1.tryLock());
        });
        t2.setName("t2");
        t2.setUncaughtExceptionHandler((tr, ex) -> System.out.println(tr.getName() + " : " + ex.getMessage()));
        t2.start();
    }

    /**
     * Test unlock
     */
    @Test(expectedExceptions = IllegalMonitorStateException.class)
    public void testUnlock()
    {
        final long sleep1000 = 1000L;
        final long sleep100 = 100L;
        Thread t1 = new Thread(() -> {
            Lock lock1 = stateStore.getLock("unlock");
            assertTrue(lock1.tryLock());
            try {
                Thread.sleep(sleep1000);
            }
            catch (InterruptedException e) {
                ignored();
            }
        });
        t1.setName("thread-1");
        t1.setUncaughtExceptionHandler((tr, ex) -> System.out.println(tr.getName() + " : " + ex.getMessage()));
        t1.start();
        try {
            Thread.sleep(sleep100);
        }
        catch (InterruptedException e) {
            ignored();
        }
        Lock lock = stateStore.getLock("unlock");
        assertFalse(lock.tryLock());
        lock.unlock();
    }

    /**
     * Test generateId
     */
    @Test
    public void testGenerateId()
    {
        long id1 = stateStore.generateId();
        long id2 = stateStore.generateId();
        assertNotEquals(id1, id2);
    }

    /**
     * Test registerNodeFailureHandler
     */
    @Test
    public void testRegisterNodeFailureHandler()
    {
        stateStore.registerNodeFailureHandler(failedNode -> {});
    }

    /**
     * Ignored function
     *
     * @return true
     */
    private boolean ignored()
    {
        return true;
    }

    /**
     * Test removeStateCollection
     */
    @Test
    public void testRemoveStateCollection()
    {
        String collectionName = "test_remove";
        stateStore.createStateCollection(collectionName, STATE_COLLECTION_TYPE);
        StateCollection collection = stateStore.getStateCollection(collectionName);
        assertNotNull(collection);
        assertEquals(collection.getName(), collectionName);
        assertEquals(collection.getType(), STATE_COLLECTION_TYPE);
        stateStore.removeStateCollection(collectionName);
        StateCollection collection1 = stateStore.getStateCollection(collectionName);
        assertNull(collection1);
    }
}
