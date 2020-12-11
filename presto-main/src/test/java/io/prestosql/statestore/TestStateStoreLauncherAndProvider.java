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
package io.prestosql.statestore;

import com.google.common.collect.ImmutableSet;
import io.airlift.http.server.HttpServerInfo;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.hetu.core.statestore.hazelcast.HazelcastStateStore;
import io.hetu.core.statestore.hazelcast.HazelcastStateStoreBootstrapper;
import io.hetu.core.statestore.hazelcast.HazelcastStateStoreFactory;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.seedstore.SeedStoreManager;
import io.prestosql.server.InternalCommunicationConfig;
import io.prestosql.spi.seedstore.Seed;
import io.prestosql.spi.seedstore.SeedStore;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.statestore.StateStoreBootstrapper;
import io.prestosql.spi.statestore.StateStoreFactory;
import io.prestosql.utils.HetuConfig;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.internal.thread.ThreadTimeoutException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_PORT_CONFIG_NAME;
import static io.prestosql.statestore.StateStoreConstants.STATE_STORE_CONFIGURATION_PATH;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Test for EmbeddedStateStoreLauncher
 *
 * @since 2020-03-25
 */
public class TestStateStoreLauncherAndProvider
{
    private static final String LOCALHOST = "127.0.0.1";
    private static final String PORT1 = "7980";
    private static final String PORT2 = "7981";
    private static final String PORT3 = "5991";
    private static final String DISCOVERY_SERVICE_LOCK = "discovery-service-lock";
    private static final String DISCOVERY_SERVICE = "discovery-service";
    private static final long DISCOVERY_REGISTRY_LOCK_TIMEOUT = 3000L;
    private StateStoreProvider stateStoreProvider;
    private StateStoreFactory factory;

    // Shared between test launcher and test provider
    @BeforeTest
    private void prepareConfigFiles()
            throws IOException
    {
        File launcherConfigFile = new File(STATE_STORE_CONFIGURATION_PATH);
        if (!launcherConfigFile.exists()) {
            launcherConfigFile.createNewFile();
        }
        else {
            launcherConfigFile.delete();
            launcherConfigFile.createNewFile();
        }
        FileWriter configWriter = new FileWriter(STATE_STORE_CONFIGURATION_PATH);
        configWriter.write("state-store.type=hazelcast\n" +
                "state-store.name=test\n" +
                "state-store.cluster=test-cluster\n" +
                "hazelcast.discovery.mode=tcp-ip\n" +
                "hazelcast.discovery.port=" + PORT1 + "\n");
        configWriter.close();
    }

    // setup for test provider
    @BeforeTest
    private void setUp()
            throws IOException
    {
        Set<Seed> seeds = new HashSet<>();
        SeedStore mockSeedStore = mock(SeedStore.class);
        Seed mockSeed = mock(Seed.class);
        seeds.add(mockSeed);

        SeedStoreManager mockSeedStoreManager = mock(SeedStoreManager.class);
        when(mockSeedStoreManager.getSeedStore()).thenReturn(mockSeedStore);

        when(mockSeed.getLocation()).thenReturn(LOCALHOST + ":" + PORT3);
        when(mockSeedStore.get()).thenReturn(seeds);

        factory = new HazelcastStateStoreFactory();
        stateStoreProvider = new LocalStateStoreProvider(mockSeedStoreManager);
        stateStoreProvider.addStateStoreFactory(factory);
    }

    // Test Launcher

    @Test
    public void testLaunchAndFailure()
            throws Exception
    {
        Set<Seed> seeds = new HashSet<>();
        SeedStore mockSeedStore = mock(SeedStore.class);
        Seed mockSeed1 = mock(Seed.class);
        Seed mockSeed2 = mock(Seed.class);
        seeds.add(mockSeed1);
        seeds.add(mockSeed2);
        when(mockSeed1.getLocation()).thenReturn(LOCALHOST + ":" + PORT1);
        when(mockSeed2.getLocation()).thenReturn(LOCALHOST + ":" + PORT2);
        when(mockSeedStore.get()).thenReturn(seeds);

        SeedStoreManager mockSeedStoreManager = mock(SeedStoreManager.class);
        when(mockSeedStoreManager.getSeedStore()).thenReturn(mockSeedStore);
        when(mockSeedStoreManager.addSeed(LOCALHOST, true)).thenReturn(seeds);
        when(mockSeedStoreManager.getFileSystemClient()).thenReturn(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get("/")));

        InternalCommunicationConfig mockInternalCommunicationConfig = mock(InternalCommunicationConfig.class);
        HttpServerInfo mockHttpServerInfo = mock(HttpServerInfo.class);
        when(mockHttpServerInfo.getHttpsUri()).thenReturn(new URI("https://" + LOCALHOST + ":" + PORT1));
        when(mockInternalCommunicationConfig.isHttpsRequired()).thenReturn(true);

        EmbeddedStateStoreLauncher launcher = new EmbeddedStateStoreLauncher(mockSeedStoreManager, mockInternalCommunicationConfig, mockHttpServerInfo, new HetuConfig());

        StateStoreBootstrapper bootstrapper = new HazelcastStateStoreBootstrapper();

        launcher.addStateStoreBootstrapper(bootstrapper);
        launcher.launchStateStore();

        StateStore second = setupSecondInstance();

        // mock "remove" second instance from cluster (delete from seed store)
        seeds.remove(mockSeed2);
        when(mockSeed1.getLocation()).thenReturn(LOCALHOST + ":" + PORT1);
        when(mockSeedStoreManager.addSeed(LOCALHOST, true)).thenReturn(seeds);

        ((HazelcastStateStore) second).shutdown();
        // Allow the first node to handle failure
        Thread.sleep(3000L);
    }

    // A second instance is setup using bootstrapper directly to manually configure port
    // Using launcher to launch will cause it to read from config file (same port as first member)
    private StateStore setupSecondInstance()
    {
        Map<String, String> config = new HashMap<>();
        config.put("hazelcast.discovery.mode", "tcp-ip");
        config.put("state-store.cluster", "test-cluster");
        config.put(DISCOVERY_PORT_CONFIG_NAME, PORT2);

        StateStoreBootstrapper bootstrapper = new HazelcastStateStoreBootstrapper();
        return bootstrapper.bootstrap(ImmutableSet.of(LOCALHOST + ":" + PORT1, LOCALHOST + ":" + PORT2), config);
    }

    @Test(timeOut = 5000, expectedExceptions = ThreadTimeoutException.class)
    public void testRegisterDiscoveryService()
            throws Exception
    {
        String failurehost = "failurehost";
        String otherhost = "otherhost";
        String localHostName = "localhost";
        int port = 8888;
        URI uri = new URI("http://" + localHostName + ":" + port);
        MockStateMap discoveryServiceMap = new MockStateMap(DISCOVERY_SERVICE, new HashMap<>());

        // Mock
        StateStore stateStore = mock(StateStore.class);
        Lock lock = mock(ReentrantLock.class);
        InternalCommunicationConfig internalCommunicationConfig = mock(InternalCommunicationConfig.class);
        HttpServerInfo httpServerInfo = mock(HttpServerInfo.class);
        when(httpServerInfo.getHttpUri()).thenReturn(uri);
        when(internalCommunicationConfig.isHttpsRequired()).thenReturn(false);
        when(stateStore.getStateCollection(DISCOVERY_SERVICE)).thenReturn(discoveryServiceMap);
        when(stateStore.getLock(DISCOVERY_SERVICE_LOCK)).thenReturn(lock);
        EmbeddedStateStoreLauncher launcher = new EmbeddedStateStoreLauncher(new SeedStoreManager(new FileSystemClientManager()), internalCommunicationConfig, httpServerInfo, new HetuConfig());
        launcher.setStateStore(stateStore);

        when(lock.tryLock(DISCOVERY_REGISTRY_LOCK_TIMEOUT, TimeUnit.MILLISECONDS)).thenReturn(true);
        // discoveryServiceMap is empty, so the current coordinator can get the lock and register itself(register=true)
        discoveryServiceMap.clear();
        assertTrue(launcher.registerDiscoveryService(failurehost));
        assertEquals(discoveryServiceMap.size(), 1);
        assertTrue(discoveryServiceMap.getAll().keySet().contains(localHostName));

        // discoveryServiceMap contains the failure host, so the current coordinator can get the lock and register itself(register=true)
        discoveryServiceMap.clear();
        discoveryServiceMap.put(failurehost, String.valueOf(port));
        assertTrue(launcher.registerDiscoveryService(failurehost));
        assertEquals(discoveryServiceMap.size(), 1);
        assertTrue(discoveryServiceMap.getAll().keySet().contains(localHostName));

        // discoveryServiceMap is already updated by other coordinator(otherhosts)
        // the current coordinator can grab the lock but will not register itself(register=false)
        discoveryServiceMap.clear();
        discoveryServiceMap.put(otherhost, String.valueOf(port));
        assertFalse(launcher.registerDiscoveryService(failurehost));
        assertEquals(discoveryServiceMap.size(), 1);
        assertFalse(discoveryServiceMap.containsKey(localHostName));

        when(lock.tryLock(DISCOVERY_REGISTRY_LOCK_TIMEOUT, TimeUnit.MILLISECONDS)).thenReturn(false);
        // discoveryServiceMap is already updated by other coordinator(otherhosts)
        // the current coordinator cannot grab the lock and not register itself
        discoveryServiceMap.clear();
        discoveryServiceMap.put(otherhost, String.valueOf(port));
        assertFalse(launcher.registerDiscoveryService(failurehost));
        assertEquals(discoveryServiceMap.size(), 1);
        assertFalse(discoveryServiceMap.containsKey(localHostName));

        // discoveryServiceMap contains failure host.
        // The current coordinator cannot get the lock and retry will cause timeout exception
        discoveryServiceMap.clear();
        discoveryServiceMap.put(failurehost, String.valueOf(port));
        launcher.registerDiscoveryService(failurehost);
    }

    // Test Provider

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRepeatAddStateStoreFactory()
    {
        stateStoreProvider.addStateStoreFactory(factory);
        stateStoreProvider.addStateStoreFactory(factory);
    }

    @Test
    public void testGetStateStore()
            throws Exception
    {
        // stateStore should be empty at initialization
        createStateStoreCluster(PORT3);
        StateStore stateStore = stateStoreProvider.getStateStore();
        assertNull(stateStore);
        stateStoreProvider.loadStateStore();
        stateStore = stateStoreProvider.getStateStore();
        assertNotNull(stateStore);
    }

    @Test
    public void testHandleClusterDisconnection()
    {
        ((LocalStateStoreProvider) stateStoreProvider).handleClusterDisconnection(new Object());
    }

    private StateStore createStateStoreCluster(String port)
    {
        Map<String, String> config = new HashMap<>();
        config.put("hazelcast.discovery.mode", "tcp-ip");
        config.put("state-store.cluster", "test-cluster");
        config.put(DISCOVERY_PORT_CONFIG_NAME, port);

        StateStoreBootstrapper bootstrapper = new HazelcastStateStoreBootstrapper();
        return bootstrapper.bootstrap(ImmutableSet.of(LOCALHOST + ":" + port), config);
    }

    @AfterTest
    public boolean tearDown()
    {
        File stateStoreConfigFile = new File(STATE_STORE_CONFIGURATION_PATH);
        if (stateStoreConfigFile.exists()) {
            return stateStoreConfigFile.delete();
        }
        return true;
    }
}
