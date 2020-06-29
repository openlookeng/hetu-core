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
package io.prestosql.seedstore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.seedstore.Seed;
import io.prestosql.spi.seedstore.SeedStore;
import io.prestosql.spi.seedstore.SeedStoreFactory;

import java.io.File;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.spi.StandardErrorCode.STATE_STORE_FAILURE;
import static io.prestosql.util.PropertiesUtil.loadProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * SeedStoreManager manages the lifecycle of SeedStores
 *
 * @since 2020-03-06
 */
public class SeedStoreManager
{
    private static final Logger LOG = Logger.get(SeedStoreManager.class);
    // properties name
    private static final File SEED_STORE_CONFIGURATION = new File("etc/seed-store.properties");
    private static final String SEED_STORE_TYPE_PROPERTY_NAME = "seed-store.type";
    private static final String SEED_STORE_SEED_HEARTBEAT_PROPERTY_NAME = "seed-store.seed.heartbeat";
    private static final String SEED_STORE_SEED_HEARTBEAT_TIMEOUT_PROPERTY_NAME = "seed-store.seed.heartbeat.timeout";
    private static final String SEED_STORE_FILESYSTEM_PROFILE = "seed-store.filesystem.profile";
    // properties default value
    private static final String SEED_STORE_TYPE_DEFAULT_VALUE = "filebased";
    private static final String SEED_STORE_SEED_HEARTBEAT_DEFAULT_VALUE = "10000"; // 10 seconds
    private static final String SEED_STORE_SEED_HEARTBEAT_TIMEOUT_DEFAULT_VALUE = "30000"; // 30 seconds
    private static final String SEED_STORE_FILESYSTEM_PROFILE_DEFAULT_VALUE = "hdfs-config-default";
    private static final int SEED_RETRY_TIMES = 5;
    private static final long SEED_RETRY_INTERVAL = 500L;

    private final Map<String, SeedStoreFactory> seedStoreFactories = new ConcurrentHashMap<>();
    private final FileSystemClientManager fileSystemClientManager;
    private ScheduledExecutorService seedRefreshExecutor;
    private SeedStore seedStore;
    private String seedStoreType;
    private String filesystemProfile;
    private long seedHeartBeat;
    private long seedHeartBeatTimeout;

    @Inject
    public SeedStoreManager(FileSystemClientManager fileSystemClientManager)
    {
        this.fileSystemClientManager = requireNonNull(fileSystemClientManager);
    }

    /**
     * Register SeedStoreFactory to SeedStoreManager to create SeedStore
     *
     * @param factory SeedStoreFactory to create SeedStore
     */
    public void addSeedStoreFactory(SeedStoreFactory factory)
    {
        if (seedStoreFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Seed Store '%s' is already registered", factory.getName()));
        }
    }

    /**
     * Use the registered SeedStoreFactory to load SeedStore
     *
     * @throws IOException exception when fail to create SeedStore
     */
    public void loadSeedStore()
            throws IOException
    {
        LOG.info("-- Loading seed store --");
        if (SEED_STORE_CONFIGURATION.exists()) {
            // load configuration
            Map<String, String> config = loadConfiguration(SEED_STORE_CONFIGURATION);
            // create seed store
            SeedStoreFactory seedStoreFactory = seedStoreFactories.get(seedStoreType);
            checkState(seedStoreFactory != null, "SeedStoreFactory %s is not registered", seedStoreFactory);
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(seedStoreFactory.getClass().getClassLoader())) {
                seedStore = seedStoreFactory.create(seedStoreType,
                        fileSystemClientManager.getFileSystemClient(filesystemProfile),
                        ImmutableMap.copyOf(config));
            }

            LOG.info("-- Loaded seed store %s --", seedStoreType);
        }
    }

    /**
     * add seed to seed store
     *
     * @param seedLocation
     * @return a collection of current seeds in the seed store
     * @throws Exception
     */
    public Collection<Seed> addSeedToSeedStore(String seedLocation)
            throws Exception
    {
        Collection<Seed> seeds = new HashSet<>();

        if (seedStore == null) {
            throw new PrestoException(STATE_STORE_FAILURE, "Seed store is null");
        }
        if (seedStoreType.equalsIgnoreCase(SEED_STORE_TYPE_DEFAULT_VALUE)) {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(seedStore.getClass().getClassLoader())) {
                // Clear expired seeds in the seed file
                clearExpiredSeed();
                // Create a seed and add seed to seed store
                Seed seed = seedStore.create(ImmutableMap.of(
                        Seed.LOCATION_PROPERTY_NAME, seedLocation,
                        Seed.TIMESTAMP_PROPERTY_NAME, String.valueOf(System.currentTimeMillis())));
                seeds = addToSeedStore(seed);
                //start seed refresher
                startSeedRefresh(seed);
            }
        }
        return seeds;
    }

    /**
     * remove seed from seed store
     *
     * @param seedLocation
     * @return a collection of current seeds in the seed store
     * @throws Exception
     */

    public Collection removeSeedFromSeedStore(String seedLocation)
            throws Exception
    {
        Collection<Seed> seeds = new HashSet<>();

        if (seedStore == null) {
            throw new PrestoException(STATE_STORE_FAILURE, "Seed store is null");
        }

        if (seedStoreType.equalsIgnoreCase(SEED_STORE_TYPE_DEFAULT_VALUE)) {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(seedStore.getClass().getClassLoader())) {
                Seed seed = seedStore.get().stream().filter(s -> s.getLocation().equals(seedLocation)).findFirst().get();

                seeds.remove(Lists.newArrayList(seed));
            }
        }
        return seeds;
    }

    /**
     * Add seed to seed store
     *
     * @return updated list of seeds
     * @throws Exception
     */
    private Collection<Seed> addToSeedStore(Seed seed)
            throws Exception
    {
        int retryTimes = 0;
        long retryInterval = 0L;
        Collection<Seed> seeds = null;

        do {
            try {
                TimeUnit.MILLISECONDS.sleep(retryInterval);
                seeds = seedStore.add(Lists.newArrayList(seed));
            }
            catch (InterruptedException | RuntimeException e) {
                LOG.warn("addSeedToSeedStore failed: %s, will retry at times: %s", e.getMessage(), retryTimes);
            }
            finally {
                retryTimes++;
                retryInterval += SEED_RETRY_INTERVAL;
            }
        }
        while (retryTimes <= SEED_RETRY_TIMES && (seeds == null || seeds.size() == 0));

        if (seeds == null || seeds.size() == 0) {
            throw new PrestoException(STATE_STORE_FAILURE, "addSeedToSeedStore failed after retry:" + SEED_RETRY_TIMES);
        }

        return seeds;
    }

    @VisibleForTesting
    /**
     * Set SeedStore
     *
     * Set the local SeedStore
     * */
    public void setSeedStore(SeedStore seedStore)
    {
        this.seedStore = seedStore;
    }

    /**
     * Get loaded SeedStore
     *
     * @return loaded SeedStore or null if it's not loaded
     */
    public SeedStore getSeedStore()
    {
        return seedStore;
    }

    /**
     * Start background task to keep refreshing seed's timestamp to SeedStore
     *
     * @param seed Seed node to refresh
     */
    private void startSeedRefresh(Seed seed)
    {
        if (seedRefreshExecutor == null) {
            seedRefreshExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("SeedRefresher"));
            seedRefreshExecutor.scheduleWithFixedDelay(() -> refreshSeed(seed), 0, seedHeartBeat, TimeUnit.MILLISECONDS);
        }
        else {
            LOG.debug("Seed Refresh has been started, ignored");
        }
    }

    private void refreshSeed(Seed seed)
    {
        try {
            Seed newSeed = seedStore.create(ImmutableMap.of(
                    Seed.LOCATION_PROPERTY_NAME, seed.getLocation(),
                    Seed.TIMESTAMP_PROPERTY_NAME, String.valueOf(System.currentTimeMillis())));

            seedStore.add(Lists.newArrayList(newSeed));
        }
        catch (Exception e) {
            LOG.debug("Error refreshSeed: %s, will refresh in next %s milliseconds" + e.getMessage(), seedHeartBeat);
        }
    }

    private void clearExpiredSeed()
            throws IOException
    {
        long retryInterval = 0L;
        for (int retryTimes = 0; retryTimes <= SEED_RETRY_TIMES; retryTimes++) {
            try {
                TimeUnit.MILLISECONDS.sleep(retryInterval);
                Collection<Seed> expiredSeeds = seedStore.get().stream()
                        .filter(s -> (System.currentTimeMillis() - s.getTimestamp() > seedHeartBeatTimeout))
                        .collect(Collectors.toList());
                if (expiredSeeds.size() > 0) {
                    seedStore.remove(expiredSeeds);
                }
                break;
            }
            catch (InterruptedException | RuntimeException e) {
                LOG.debug("clearExpiredSeed failed: %s, will retry at times: %s", e.getMessage(), ++retryTimes);
                retryInterval += SEED_RETRY_INTERVAL;
            }
        }
    }

    private Map<String, String> loadConfiguration(File configFile)
            throws IOException
    {
        Map<String, String> properties = new HashMap<>(loadProperties(configFile));
        seedStoreType = properties.getOrDefault(SEED_STORE_TYPE_PROPERTY_NAME, SEED_STORE_TYPE_DEFAULT_VALUE);
        filesystemProfile = properties.getOrDefault(SEED_STORE_FILESYSTEM_PROFILE, SEED_STORE_FILESYSTEM_PROFILE_DEFAULT_VALUE);
        seedHeartBeat = Long.parseLong(
                properties.getOrDefault(SEED_STORE_SEED_HEARTBEAT_PROPERTY_NAME, SEED_STORE_SEED_HEARTBEAT_DEFAULT_VALUE));
        seedHeartBeatTimeout = Long.parseLong(
                properties.getOrDefault(SEED_STORE_SEED_HEARTBEAT_TIMEOUT_PROPERTY_NAME, SEED_STORE_SEED_HEARTBEAT_TIMEOUT_DEFAULT_VALUE));
        if (seedHeartBeat > seedHeartBeatTimeout) {
            throw new InvalidParameterException(format("The value of %s cannot be greater than the value of %s in the property file",
                    SEED_STORE_SEED_HEARTBEAT_PROPERTY_NAME, SEED_STORE_SEED_HEARTBEAT_TIMEOUT_PROPERTY_NAME));
        }
        return properties;
    }
}
