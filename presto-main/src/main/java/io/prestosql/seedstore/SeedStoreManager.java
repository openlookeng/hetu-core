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
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.seedstore.Seed;
import io.prestosql.spi.seedstore.SeedStore;
import io.prestosql.spi.seedstore.SeedStoreFactory;
import io.prestosql.statestore.StateStoreConstants;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.prestosql.spi.StandardErrorCode.SEED_STORE_FAILURE;
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
    private static final File STATE_STORE_CONFIGURATION = new File("etc/state-store.properties");
    private static final String SEED_STORE_TYPE_PROPERTY_NAME = "seed-store.type";
    private static final String SEED_STORE_SEED_HEARTBEAT_PROPERTY_NAME = "seed-store.seed.heartbeat";
    private static final String SEED_STORE_SEED_HEARTBEAT_TIMEOUT_PROPERTY_NAME = "seed-store.seed.heartbeat.timeout";
    private static final String SEED_STORE_FILESYSTEM_PROFILE = "seed-store.filesystem.profile";
    // properties default value
    private static final String SEED_STORE_TYPE_DEFAULT_VALUE = "filebased";
    private static final String SEED_STORE_SEED_HEARTBEAT_DEFAULT_VALUE = "10000"; // 10 seconds
    private static final String SEED_STORE_SEED_HEARTBEAT_TIMEOUT_DEFAULT_VALUE = "60000"; // 60 seconds
    private static final String SEED_STORE_FILESYSTEM_PROFILE_DEFAULT_VALUE = "hdfs-config-default";
    private static final int SEED_RETRY_TIMES = 5;
    private static final long SEED_RETRY_INTERVAL = 500L;

    private final Map<String, SeedStoreFactory> seedStoreFactories = new ConcurrentHashMap<>();
    private final FileSystemClientManager fileSystemClientManager;
    private ScheduledExecutorService seedRefreshExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("SeedRefresher"));
    private SeedStore seedStore;
    private String seedStoreType;
    private String filesystemProfile;
    private HetuFileSystemClient fileSystemClient;
    private long seedHeartBeat;
    private long seedHeartBeatTimeout;
    private boolean isSeedStoreEnabled;
    private ConcurrentHashMap<String, Seed> refreshableSeedsMap = new ConcurrentHashMap<>();

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
        // load configuration
        Map<String, String> config = loadConfiguration(STATE_STORE_CONFIGURATION, SEED_STORE_CONFIGURATION);

        if (isSeedStoreEnabled) {
            LOG.info("-- Loading seed store --");
            // create seed store
            SeedStoreFactory seedStoreFactory = seedStoreFactories.get(seedStoreType);
            checkState(seedStoreFactory != null, "SeedStoreFactory %s is not registered", seedStoreFactory);
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(seedStoreFactory.getClass().getClassLoader())) {
                fileSystemClient = fileSystemClientManager.getFileSystemClient(filesystemProfile, Paths.get("/"));
                seedStore = seedStoreFactory.create(seedStoreType,
                        fileSystemClient,
                        ImmutableMap.copyOf(config));
            }
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(seedStore.getClass().getClassLoader())) {
                // start seed refresher
                seedRefreshExecutor.scheduleWithFixedDelay(() -> refreshSeeds(), 0, seedHeartBeat, TimeUnit.MILLISECONDS);
            }
            LOG.info("-- Loaded seed store %s --", seedStoreType);
        }
    }

    /**
     * Get file system client
     */

    public HetuFileSystemClient getFileSystemClient()
    {
        return fileSystemClient;
    }

    /**
     * Get all seeds from seed store
     *
     * @return a collection of seeds in the seed store
     * @throws IOException
     */
    public Collection<Seed> getAllSeeds()
            throws IOException
    {
        if (seedStore == null) {
            throw new PrestoException(SEED_STORE_FAILURE, "Seed store is null");
        }

        Collection<Seed> seeds = seedStore.get();

        return seeds;
    }

    /**
     * Add seed to seed store. If refreshable is enabled, seed will be refreshed periodically
     *
     * @param refreshable
     * @return a collection of seeds in the seed store
     * @throws IOException
     */

    public Collection<Seed> addSeed(String seedLocation, boolean refreshable)
            throws IOException
    {
        Collection<Seed> seeds = new HashSet<>();

        if (seedStore == null) {
            throw new PrestoException(SEED_STORE_FAILURE, "Seed store is null");
        }

        Seed seed = seedStore.create(ImmutableMap.of(
                Seed.LOCATION_PROPERTY_NAME, seedLocation,
                Seed.TIMESTAMP_PROPERTY_NAME, String.valueOf(System.currentTimeMillis())));
        seeds = addSeed(seed);

        if (refreshable) {
            refreshableSeedsMap.put(seedLocation, seed);
        }

        LOG.debug("Seed=%s added to seed store", seedLocation);
        return seeds;
    }

    /**
     * remove seed from seed store
     *
     * @param seedLocation
     * @return a collection of seeds in the seed store
     * @throws IOException
     */

    public Collection<Seed> removeSeed(String seedLocation)
            throws IOException
    {
        Collection<Seed> seeds = new HashSet<>();

        if (seedStore == null) {
            throw new PrestoException(SEED_STORE_FAILURE, "Seed store is null");
        }

        refreshableSeedsMap.remove(seedLocation);
        Optional<Seed> seedOptional = seedStore.get().stream().filter(s -> s.getLocation().equals(seedLocation)).findFirst();

        if (seedOptional.isPresent()) {
            seeds = seedStore.remove(Lists.newArrayList(seedOptional.get()));
            LOG.debug("Seed=%s removed from seed store", seedLocation);
        }

        return seeds;
    }

    /**
     * Clear expired seed in the seed store
     *
     * @throws IOException
     */
    public void clearExpiredSeeds()
            throws IOException
    {
        if (seedStore == null) {
            throw new PrestoException(SEED_STORE_FAILURE, "Seed store is null");
        }
        try {
            Collection<Seed> expiredSeeds = seedStore.get()
                    .stream()
                    .filter(s -> (System.currentTimeMillis() - s.getTimestamp() > seedHeartBeatTimeout))
                    .collect(Collectors.toList());
            if (expiredSeeds.size() > 0) {
                LOG.info("Expired seeds=%s will be cleared", expiredSeeds);
                seedStore.remove(expiredSeeds);
            }
        }
        catch (RuntimeException e) {
            LOG.warn("clearExpiredSeed failed with following message: %s", e.getMessage());
        }
    }

    private Collection<Seed> addSeed(Seed seed)
            throws IOException
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
                LOG.warn("add seed=%s failed: %s, will retry at times: %s", seed, e.getMessage(), retryTimes);
            }
            finally {
                retryTimes++;
                retryInterval += SEED_RETRY_INTERVAL;
            }
        }
        while (retryTimes <= SEED_RETRY_TIMES && (seeds == null || seeds.size() == 0));

        if (seeds == null || seeds.size() == 0) {
            throw new PrestoException(SEED_STORE_FAILURE, String.format("add seed=%s to seed store failed after retry:%d",
                    seed.getLocation(), SEED_RETRY_TIMES));
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

    private void refreshSeeds()
    {
        for (Map.Entry<String, Seed> entry : refreshableSeedsMap.entrySet()) {
            long newTime = System.currentTimeMillis();
            LOG.debug("seed=%s refresh with oldTimestamp=%s and newTimestamp=%s", entry.getKey(), entry.getValue().getTimestamp(), newTime);
            Seed newSeed = seedStore.create(ImmutableMap.of(
                    Seed.LOCATION_PROPERTY_NAME, entry.getKey(),
                    Seed.TIMESTAMP_PROPERTY_NAME, String.valueOf(newTime)));
            try {
                seedStore.add(Lists.newArrayList(newSeed));
                entry.setValue(newSeed);
            }
            catch (IOException | RuntimeException e) {
                LOG.warn("Error refresh seed=%s with error message: %s, will refresh in next %s milliseconds",
                        entry.getKey(), e.getMessage(), seedHeartBeat);
                continue;
            }
        }
    }

    private Map<String, String> loadConfiguration(File stateStoreConfig, File seedStoreConfig)
            throws IOException
    {
        Map<String, String> properties = new HashMap<>();

        // initialize variables
        seedStoreType = SEED_STORE_TYPE_DEFAULT_VALUE;
        filesystemProfile = SEED_STORE_FILESYSTEM_PROFILE_DEFAULT_VALUE;
        seedHeartBeat = Long.parseLong(SEED_STORE_SEED_HEARTBEAT_DEFAULT_VALUE);
        seedHeartBeatTimeout = Long.parseLong(SEED_STORE_SEED_HEARTBEAT_TIMEOUT_DEFAULT_VALUE);

        // load state store config if exist
        if (stateStoreConfig.exists()) {
            Map<String, String> stateStoreProperties = new HashMap<>(loadPropertiesFrom(stateStoreConfig.getPath()));
            filesystemProfile = stateStoreProperties.getOrDefault(StateStoreConstants.HAZELCAST_DISCOVERY_TCPIP_PROFILE, filesystemProfile);
            // for now, seed store is started only if tcp-ip mode enabled and tcp-ip.seeds is not set
            String discoveryMode = stateStoreProperties.get(StateStoreConstants.DISCOVERY_MODE_PROPERTY_NAME);
            isSeedStoreEnabled = discoveryMode != null && discoveryMode.equals(StateStoreConstants.DISCOVERY_MODE_TCPIP)
                    && stateStoreProperties.get(StateStoreConstants.HAZELCAST_DISCOVERY_TCPIP_SEEDS) == null;
        }

        // load seed store config if exist
        if (seedStoreConfig.exists()) {
            Map<String, String> seedStoreProperties = new HashMap<>(loadPropertiesFrom(seedStoreConfig.getPath()));
            properties.putAll(seedStoreProperties);
            seedStoreType = properties.getOrDefault(SEED_STORE_TYPE_PROPERTY_NAME, seedStoreType);
            filesystemProfile = properties.getOrDefault(SEED_STORE_FILESYSTEM_PROFILE, filesystemProfile);
            if (properties.get(SEED_STORE_SEED_HEARTBEAT_PROPERTY_NAME) != null) {
                seedHeartBeat = Long.parseLong(properties.get(SEED_STORE_SEED_HEARTBEAT_PROPERTY_NAME));
            }
            if (properties.get(SEED_STORE_SEED_HEARTBEAT_TIMEOUT_PROPERTY_NAME) != null) {
                seedHeartBeatTimeout = Long.parseLong(properties.get(SEED_STORE_SEED_HEARTBEAT_TIMEOUT_PROPERTY_NAME));
            }
            if (seedHeartBeat > seedHeartBeatTimeout) {
                throw new InvalidParameterException(format("The value of %s cannot be greater than the value of %s in the property file",
                        SEED_STORE_SEED_HEARTBEAT_PROPERTY_NAME, SEED_STORE_SEED_HEARTBEAT_TIMEOUT_PROPERTY_NAME));
            }
        }
        return properties;
    }
}
