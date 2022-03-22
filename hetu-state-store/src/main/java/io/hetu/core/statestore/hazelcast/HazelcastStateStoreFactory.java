/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.hetu.core.security.authentication.kerberos.KerberosConfig;
import io.hetu.core.security.networking.ssl.SslConfig;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;
import io.prestosql.spi.seedstore.Seed;
import io.prestosql.spi.seedstore.SeedStore;
import io.prestosql.spi.statestore.CipherService;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.statestore.StateStoreFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.hetu.core.statestore.Constants.STATE_STORE_CLUSTER_CONFIG_NAME;
import static io.hetu.core.statestore.StateStoreUtils.getEncryptionTypeFromConfig;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DEFAULT_CLUSTER_ID;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_ENABLED;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_MODE_CONFIG_NAME;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_MODE_MULTICAST;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_MODE_TCPIP;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_MULTICAST_STRATEGY_CLASS_NAME;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_TCPIP_SEEDS;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.HAZELCAST_SSL_ENABLED;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.HEARTBEAT_INTERVAL_SECONDS;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.HEARTBEAT_TIMEOUT_SECONDS;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.JAAS_CONFIG_FILE;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.KERBEROS_ENABLED;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.KERBEROS_LOGIN_CONTEXT_NAME;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.KERBEROS_SERVICE_PRINCIPAL;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.KRB5_CONFIG_FILE;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.SSL_CIPHER_SUITES;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.SSL_KEYSTORE_PASSWORD;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.SSL_KEYSTORE_PATH;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.SSL_PROTOCOLS;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.SSL_TRUSTSTORE_PASSWORD;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.SSL_TRUSTSTORE_PATH;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.prestosql.spi.StandardErrorCode.STATE_STORE_FAILURE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * State store factory for Hazelcast to create client connecting to Hazelcast cluster
 *
 * @since 2020-03-06
 */
public class HazelcastStateStoreFactory
        implements StateStoreFactory
{
    private static final Logger log = Logger.get(HazelcastStateStoreFactory.class);
    // Retry 30 seconds to grab the seeds from seed store
    private static final int SEED_IP_FETCHING_RETRY_TIMES = 10;
    private static final long SEED_IP_FETCHING_INITIAL_RETRY_INTERVAL = 500L;
    private static final String COMMA = ",";
    private static final String CLIENT_HEARTBEAT_TIMEOUT = "hazelcast.client.heartbeat.timeout";
    private static final String CLIENT_HEARTBEAT_INTERVAL = "hazelcast.client.heartbeat.interval";
    private String name = "hazelcast";

    private final Map<String, StateStoreFactory> stateStoreFactories = new ConcurrentHashMap<>(0);

    @Override
    public StateStore create(String stateStoreName, SeedStore seedStore, Map<String, String> properties)
    {
        if (properties == null) {
            throw new IllegalArgumentException(format("found no state store config"));
        }
        this.name = stateStoreName;

        requireNonNull(properties, "properties is null");

        log.info("-- Starting new state store client --");

        String clusterId = properties.get(STATE_STORE_CLUSTER_CONFIG_NAME);
        if (clusterId == null) {
            log.info("cluster name not provided, using default cluster name: %s", DEFAULT_CLUSTER_ID);
            clusterId = DEFAULT_CLUSTER_ID;
        }

        ClientConfig clientConfig = new ClientConfig();
        ClientConnectionStrategyConfig connectionStrategy = clientConfig.getConnectionStrategyConfig();
        connectionStrategy.setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.OFF);

        // Add serialization for Slice
        SerializerConfig sc = new SerializerConfig().setImplementation(new HazelCastSliceSerializer()).setTypeClass(Slice.class);
        clientConfig.getSerializationConfig().addSerializerConfig(sc);

        SerializerConfig catalogEntity = new SerializerConfig().setImplementation(new HazelcastCatalogSerializer()).setTypeClass(CatalogEntity.class);
        clientConfig.getSerializationConfig().addSerializerConfig(catalogEntity);

        SerializerConfig dataEntity = new SerializerConfig().setImplementation(new HazelcastDatabaseEntitySerializer()).setTypeClass(DatabaseEntity.class);
        clientConfig.getSerializationConfig().addSerializerConfig(dataEntity);

        SerializerConfig tableEntity = new SerializerConfig().setImplementation(new HazelcastTableEntitySerializer()).setTypeClass(TableEntity.class);
        clientConfig.getSerializationConfig().addSerializerConfig(tableEntity);

        clientConfig.setClusterName(clusterId);

        // set security config
        if (Boolean.parseBoolean(properties.get(KERBEROS_ENABLED))) {
            KerberosConfig.setKerberosEnabled(true);
            KerberosConfig.setLoginContextName(properties.get(KERBEROS_LOGIN_CONTEXT_NAME));
            KerberosConfig.setServicePrincipalName(properties.get(KERBEROS_SERVICE_PRINCIPAL));
            System.setProperty("java.security.krb5.conf", properties.get(KRB5_CONFIG_FILE));
            System.setProperty("java.security.auth.login.config", properties.get(JAAS_CONFIG_FILE));
        }

        // Set hazelcast SSL config
        if (Boolean.parseBoolean(properties.get(HAZELCAST_SSL_ENABLED))) {
            SslConfig.setSslEnabled(true);
            SslConfig.setKeyStorePath(properties.get(SSL_KEYSTORE_PATH));
            SslConfig.setKeyStorePassword(properties.get(SSL_KEYSTORE_PASSWORD));
            SslConfig.setTrustStorePath(properties.get(SSL_TRUSTSTORE_PATH));
            SslConfig.setTrustStorePassword(properties.get(SSL_TRUSTSTORE_PASSWORD));
            SslConfig.setCipherSuites(properties.get(SSL_CIPHER_SUITES));
            SslConfig.setProtocols(properties.get(SSL_PROTOCOLS));
        }

        // Set heartbeat config
        final String heartbeatInterval = properties.get(HEARTBEAT_INTERVAL_SECONDS);
        final String heartbeatTimeout = properties.get(HEARTBEAT_TIMEOUT_SECONDS);
        if (heartbeatInterval != null) {
            clientConfig.setProperty(CLIENT_HEARTBEAT_INTERVAL, String.valueOf(Integer.parseInt(heartbeatInterval) * 1000));
        }
        if (heartbeatTimeout != null) {
            clientConfig.setProperty(CLIENT_HEARTBEAT_TIMEOUT, String.valueOf(Integer.parseInt(heartbeatTimeout) * 1000));
        }

        final String discoveryMode = properties.get(DISCOVERY_MODE_CONFIG_NAME);

        if (discoveryMode == null || discoveryMode.equalsIgnoreCase(DISCOVERY_MODE_MULTICAST)) {
            log.info("Using Multicast discovery for Hazelcast");
            clientConfig.setProperty(DISCOVERY_ENABLED, "true");
            DiscoveryStrategyConfig strategy = new DiscoveryStrategyConfig(DISCOVERY_MULTICAST_STRATEGY_CLASS_NAME);
            clientConfig.getNetworkConfig().getDiscoveryConfig().addDiscoveryStrategyConfig(strategy);
        }
        else if (discoveryMode.equalsIgnoreCase(DISCOVERY_MODE_TCPIP)) {
            String tcpipSeeds = properties.get(DISCOVERY_TCPIP_SEEDS);
            Collection<String> seedLocation = new HashSet<>();
            if (tcpipSeeds != null && !tcpipSeeds.trim().isEmpty()) {
                for (String seed : tcpipSeeds.split(COMMA)) {
                    seedLocation.add(seed);
                }
            }
            else {
                seedStore.setName(clusterId);
                seedLocation = getSeedLocation(seedStore);
            }

            log.info("Using TCP-IP discovery for Hazelcast, seed nodes are: %s", String.join(",", seedLocation));
            seedLocation.stream().forEach(ip -> {
                clientConfig.getNetworkConfig().addAddress(ip);
            });
        }
        else {
            throw new PrestoException(CONFIGURATION_INVALID, "Discovery mode not supported: " + discoveryMode);
        }

        HazelcastInstance hzClient = HazelcastClient.newHazelcastClient(clientConfig);

        CipherService.Type encryptionType = getEncryptionTypeFromConfig(properties);

        return new HazelcastStateStore(hzClient, stateStoreName, encryptionType);
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    private Collection<String> getSeedLocation(SeedStore seedStore)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(seedStore.getClass().getClassLoader())) {
            int retryTimes = 0;
            long retryInterval = 0L;
            Set<String> ips = null;
            do {
                try {
                    log.debug("getSeedLocation at retry times: %s, retryInterval: %s", retryTimes, retryInterval);

                    TimeUnit.MILLISECONDS.sleep(retryInterval);
                    Collection<Seed> seeds = seedStore.get();
                    ips = seeds.stream().map(x -> x.getLocation()).collect(Collectors.toSet());
                }
                catch (IOException e) {
                    throw new RuntimeException("Error getSeedLocation: " + e.getMessage());
                }
                catch (UncheckedExecutionException | IllegalStateException | InterruptedException e) {
                    log.warn("getSeedLocation failed with following exception : %s", e.getMessage());
                }
                finally {
                    retryTimes++;
                    retryInterval += SEED_IP_FETCHING_INITIAL_RETRY_INTERVAL;
                }
            }
            while (retryTimes <= SEED_IP_FETCHING_RETRY_TIMES && (ips == null || ips.size() == 0));

            if (ips == null || ips.size() == 0) {
                throw new PrestoException(STATE_STORE_FAILURE, "Using TCP-IP discovery but can not find seeds");
            }
            return ips;
        }
    }
}
