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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import io.prestosql.seedstore.SeedStoreManager;
import io.prestosql.server.InternalCommunicationConfig;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.filesystem.FileBasedLock;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.statestore.StateStoreBootstrapper;
import io.prestosql.utils.HetuConfig;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.prestosql.spi.StandardErrorCode.STATE_STORE_FAILURE;
import static io.prestosql.statestore.StateStoreConstants.DEFAULT_HAZELCAST_DISCOVERY_PORT;
import static io.prestosql.statestore.StateStoreConstants.HAZELCAST;
import static io.prestosql.statestore.StateStoreConstants.HAZELCAST_DISCOVERY_PORT_PROPERTY_NAME;
import static io.prestosql.statestore.StateStoreConstants.HAZELCAST_DISCOVERY_TCPIP_SEEDS;
import static io.prestosql.statestore.StateStoreConstants.STATE_STORE_CLUSTER_PROPERTY_NAME;
import static io.prestosql.statestore.StateStoreConstants.STATE_STORE_CONFIGURATION_PATH;
import static io.prestosql.statestore.StateStoreConstants.STATE_STORE_TYPE_PROPERTY_NAME;
import static java.util.Objects.requireNonNull;

/**
 * EmbeddedStateStoreLauncher responsible for launching embedded state stores on current node
 * @since 2020-03-04
 */
public class EmbeddedStateStoreLauncher
        implements StateStoreLauncher
{
    private static final Logger LOG = Logger.get(EmbeddedStateStoreLauncher.class);
    private static final File STATE_STORE_LAUNCHER_CONFIGURATION = new File(STATE_STORE_CONFIGURATION_PATH);
    private static final String DISCOVERY_SERVICE_LOCK = "discovery-service-lock";
    private static final String DISCOVERY_SERVICE = "discovery-service";
    private static final String COMMA = ",";
    private static final long DISCOVERY_REGISTRY_LOCK_TIMEOUT = 3000L;
    private static final int DISCOVERY_REGISTRY_RETRY_TIMES = 60;
    private static final long DISCOVERY_REGISTRY_RETRY_INTERVAL = 5000L; //5 seconds
    private static final String LAUNCHER_LOCK_FILE_PATH = "/tmp/state-store-launcher";

    private final SeedStoreManager seedStoreManager;
    private final HttpServerInfo httpServerInfo;
    private final InternalCommunicationConfig internalCommunicationConfig;
    private final HetuConfig hetuConfig;
    private StateStoreBootstrapper bootstrapper;
    private StateStore stateStore;

    @Inject
    public EmbeddedStateStoreLauncher(
            SeedStoreManager seedStoreManager,
            InternalCommunicationConfig internalCommunicationConfig,
            HttpServerInfo httpServerInfo,
            HetuConfig hetuConfig)
    {
        this.seedStoreManager = requireNonNull(seedStoreManager, "seedStoreManager is null");
        this.httpServerInfo = requireNonNull(httpServerInfo, "httpServerInfo is null");
        this.internalCommunicationConfig = requireNonNull(internalCommunicationConfig, "internalCommunicationConfig is null");
        this.hetuConfig = requireNonNull(hetuConfig, "hetuConfig is null");
    }

    @Override
    public void addStateStoreBootstrapper(StateStoreBootstrapper bootstrapper)
    {
        this.bootstrapper = bootstrapper;
    }

    @Override
    public void launchStateStore()
            throws Exception
    {
        if (bootstrapper == null) {
            LOG.info("No available bootstrapper, skip launching state store");
            return;
        }

        if (STATE_STORE_LAUNCHER_CONFIGURATION.exists()) {
            Map<String, String> properties = new HashMap<>(loadPropertiesFrom(STATE_STORE_LAUNCHER_CONFIGURATION.getPath()));
            Set<String> staticSeeds = getStateStoreStaticSeeds(properties);
            if (staticSeeds.size() > 0) {
                launchStateStore(staticSeeds, properties);
            }
            else if (seedStoreManager.getSeedStore() != null) {
                // Set seed store name
                seedStoreManager.getSeedStore().setName(properties.get(STATE_STORE_CLUSTER_PROPERTY_NAME));
                // Clear expired seeds
                seedStoreManager.clearExpiredSeeds();
                // Use lock to control synchronization of state store launch among all coordinators
                Lock launcherLock = new FileBasedLock(seedStoreManager.getFileSystemClient(), Paths.get(LAUNCHER_LOCK_FILE_PATH));
                try {
                    launcherLock.lock();
                    launchStateStoreFromSeedStore(properties);
                }
                finally {
                    launcherLock.unlock();
                }
            }
            else {
                launchStateStore(new HashSet<>(), properties);
            }

            if (stateStore == null) {
                throw new PrestoException(STATE_STORE_FAILURE, "Unable to launch state store, please check your configuration");
            }
        }
        else {
            LOG.info("No configuration file found, skip launching state store");
        }
    }

    private void launchStateStoreFromSeedStore(Map<String, String> properties) throws IOException
    {
        // Get all seeds
        Set<String> locations = seedStoreManager.getAllSeeds()
                .stream()
                .map(x -> x.getLocation())
                .collect(Collectors.toSet());
        String launcherPort = getStateStoreLauncherPort(properties);
        requireNonNull(launcherPort, "The launcher port is null");
        // Launch state store
        String currentLocation = getNodeUri().getHost() + ":" + launcherPort;
        locations.add(currentLocation);
        if (launchStateStore(locations, properties) != null) {
            // Add seed to seed store if and only if state store launched successfully
            seedStoreManager.addSeed(currentLocation, true);
        }
    }

    @VisibleForTesting
    public StateStore launchStateStore(Collection<String> ips, Map<String, String> properties)
    {
        if (bootstrapper == null) {
            LOG.info("No available bootstrapper, skip launching state store");
            return null;
        }

        stateStore = bootstrapper.bootstrap(ips, properties);

        boolean isDiscoveryNode = false;
        if (hetuConfig.isMultipleCoordinatorEnabled()) {
            // Try registering as discovery node when there are multiple coordinators
            stateStore.createStateCollection(DISCOVERY_SERVICE, StateCollection.Type.MAP);
            isDiscoveryNode = registerDiscoveryService("");
        }

        stateStore.registerNodeFailureHandler(this::handleNodeFailure);
        LOG.info("State store node launched"
                + ", current seed nodes: " + String.join(",", ips)
                + ", local node: " + getNodeUri().toString()
                + ", is discovery node: " + isDiscoveryNode);

        return stateStore;
    }

    @VisibleForTesting
    public void setStateStore(StateStore stateStore)
    {
        this.stateStore = stateStore;
    }

    /**
     * Register discovery service to the state store
     * while launching state store or after node failure
     *
     * @return true if register successfully
     */
    @VisibleForTesting
    boolean registerDiscoveryService(String failureNodeHost)
    {
        // Check node host and port
        URI nodeUri = getNodeUri();
        String nodeHost = nodeUri.getHost();
        int nodePort = nodeUri.getPort();
        if (nodeHost == null || nodePort == -1) {
            throw new PrestoException(STATE_STORE_FAILURE, "node ip and node port cannot be null");
        }

        // Add Jitter under 1000 milliseonds
        try {
            Thread.sleep((long) (new SecureRandom().nextDouble() * 1000));
        }
        catch (InterruptedException e) {
        }

        boolean registered = false;
        boolean locked = false;
        Lock lock = null;
        StateMap<String, String> discoveryServiceMap;
        // Add retry mechanism to avoid (1) and (2)
        for (int i = 1; i <= DISCOVERY_REGISTRY_RETRY_TIMES; i++) {
            try {
                LOG.debug("Trying to register Discovery Service at time: %s", i);
                lock = stateStore.getLock(DISCOVERY_SERVICE_LOCK);
                locked = lock.tryLock(DISCOVERY_REGISTRY_LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
                if (locked) {
                    discoveryServiceMap = (StateMap<String, String>) stateStore.getStateCollection(DISCOVERY_SERVICE);
                    if (discoveryServiceMap.isEmpty() || discoveryServiceMap.containsKey(failureNodeHost)) {
                        // Note coordinators can be crashed in 3 conditions while registering discovery service
                        // (1) before discoveryServiceMap.clear() -> old failureNodeHost still in the discoveryServiceMap
                        // (2) after discoveryServiceMap.clear() but before new node registered -> discoveryServiceMap is empty
                        // (3) after new node registered -> discoveryServiceMap has new node
                        if (discoveryServiceMap.containsKey(failureNodeHost)) {
                            // If failure host exists
                            // old discovery node info needs to be cleared first
                            discoveryServiceMap.clear();
                        }
                        discoveryServiceMap.put(nodeHost, String.valueOf(nodePort));
                        LOG.info("Discovery service node set with host=%s and port=%d", nodeHost, nodePort);
                        registered = true;
                    }
                    break;
                }
                else {
                    discoveryServiceMap = (StateMap<String, String>) stateStore.getStateCollection(DISCOVERY_SERVICE);

                    if (!discoveryServiceMap.isEmpty() && !discoveryServiceMap.containsKey(failureNodeHost)) {
                        // Discovery service is updated by other coordinator
                        break;
                    }

                    if (i == DISCOVERY_REGISTRY_RETRY_TIMES) {
                        LOG.error("Discovery Service: %s is not available, either discovery service is not found or discovery service is down, please take a look",
                                discoveryServiceMap);
                        break;
                    }
                    Thread.sleep(DISCOVERY_REGISTRY_RETRY_INTERVAL);
                }
            }
            catch (InterruptedException | RuntimeException e) {
                LOG.warn("registerDiscoveryService failed with following exception: %s at try times: %s", e.getMessage(), i);
            }
            finally {
                if (locked) {
                    lock.unlock();
                }
            }
        }
        return registered;
    }

    private void handleNodeFailure(Object failureMember)
    {
        if (hetuConfig.isMultipleCoordinatorEnabled()) {
            // failureMember format host:port
            String failureMemberHost = ((String) failureMember).split(":")[0];
            registerDiscoveryService(failureMemberHost);
        }

        if (seedStoreManager.getSeedStore() != null) {
            try {
                seedStoreManager.removeSeed((String) failureMember);
            }
            catch (Exception e) {
                LOG.error("Cannot remove failure node %s from seed store: %s", failureMember, e.getMessage());
            }
        }
    }

    private URI getNodeUri()
    {
        if (internalCommunicationConfig.isHttpsRequired()) {
            return httpServerInfo.getHttpsUri();
        }
        else {
            return httpServerInfo.getHttpUri();
        }
    }

    private String getStateStoreLauncherPort(Map<String, String> properties)
    {
        String port = null;
        String stateStoreType = properties.get(STATE_STORE_TYPE_PROPERTY_NAME);
        if (stateStoreType != null) {
            if (stateStoreType.trim().equals(HAZELCAST)) {
                port = properties.get(HAZELCAST_DISCOVERY_PORT_PROPERTY_NAME);
                if (port == null || port.trim().isEmpty()) {
                    port = DEFAULT_HAZELCAST_DISCOVERY_PORT;
                }
            }
        }
        return port;
    }

    // This function will get static seeds manually configured by client
    private Set<String> getStateStoreStaticSeeds(Map<String, String> properties)
    {
        Set<String> members = new HashSet<>();
        // now only support hazelcast tcp-ip seeds
        String memberString = properties.get(HAZELCAST_DISCOVERY_TCPIP_SEEDS);
        if (memberString != null) {
            for (String m : memberString.split(COMMA)) {
                members.add(m.trim());
            }
        }
        return members;
    }
}
