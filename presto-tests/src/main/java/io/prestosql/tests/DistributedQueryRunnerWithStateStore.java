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
package io.prestosql.tests;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.inject.Key;
import io.airlift.log.Logger;
import io.hetu.core.common.util.SslSocketUtil;
import io.hetu.core.seedstore.filebased.FileBasedSeed;
import io.hetu.core.statestore.StateStoreManagerPlugin;
import io.hetu.core.statestore.hazelcast.HazelcastConstants;
import io.hetu.core.statestore.hazelcast.HazelcastStateStore;
import io.prestosql.Session;
import io.prestosql.seedstore.SeedStoreManager;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.seedstore.Seed;
import io.prestosql.spi.seedstore.SeedStore;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.statestore.EmbeddedStateStoreLauncher;
import io.prestosql.statestore.LocalStateStoreProvider;
import io.prestosql.statestore.StateStoreLauncher;
import io.prestosql.statestore.StateStoreProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_PORT_CONFIG_NAME;
import static java.util.Objects.requireNonNull;

/**
 * This class is a DistributedQueryRunner that also runs StateStore.
 *
 * The DistributedQueryRunner does not includes a state store module.
 * This class however, starts a DistributedQueryRunner and also runs the
 * StateStore module on the coordinator and the servers.
 *
 */

public class DistributedQueryRunnerWithStateStore
        extends DistributedQueryRunner
{
    private static final Logger log = Logger.get(DistributedQueryRunnerWithStateStore.class);
    private List<StateStore> stateStores = new ArrayList<>();
    private List<StateStoreProvider> providers = new ArrayList<>();

    @Deprecated
    public DistributedQueryRunnerWithStateStore(Session defaultSession, int nodeCount, Map<String, String> extraProperties)
            throws Exception
    {
        this(defaultSession, nodeCount, extraProperties, ImmutableMap.of(), DEFAULT_SQL_PARSER_OPTIONS, ENVIRONMENT, Optional.empty());
    }

    protected DistributedQueryRunnerWithStateStore(
            Session defaultSession,
            int nodeCount,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            SqlParserOptions parserOptions,
            String environment,
            Optional<Path> baseDataDir)
            throws Exception
    {
        super(defaultSession, nodeCount, extraProperties, coordinatorProperties, parserOptions, environment, baseDataDir);
        requireNonNull(defaultSession, "defaultSession is null");

        setupStateStore();
    }

    public void setupStateStore()
            throws Exception
    {
        int port = SslSocketUtil.getAvailablePort();
        for (TestingPrestoServer server : servers) {
            server.installPlugin(new StateStoreManagerPlugin());
            // State Store
            StateStoreLauncher launcher = server.getInstance(Key.get(StateStoreLauncher.class));
            if (launcher instanceof EmbeddedStateStoreLauncher) {
                Set<String> ips = Sets.newHashSet(Arrays.asList("127.0.0.1"));
                Map<String, String> stateStoreProperties = new HashMap<>();
                stateStoreProperties.put(DISCOVERY_PORT_CONFIG_NAME, port + "");
                stateStoreProperties.putIfAbsent(HazelcastConstants.DISCOVERY_MODE_CONFIG_NAME, HazelcastConstants.DISCOVERY_MODE_TCPIP);
                this.stateStores.add(((EmbeddedStateStoreLauncher) launcher).launchStateStore(ips, stateStoreProperties));
            }
            launcher.launchStateStore();

            StateStoreProvider provider = server.getInstance(Key.get(StateStoreProvider.class));
            Seed seed = new FileBasedSeed.FileBasedSeedBuilder("127.0.0.1:" + port).build();
            SeedStore seedStore = new SeedStore()
            {
                @Override
                public Collection<Seed> add(Collection<Seed> seeds)
                        throws IOException
                {
                    return null;
                }

                @Override
                public Collection<Seed> get()
                        throws IOException
                {
                    return new ArrayList<>((Arrays.asList(seed)));
                }

                @Override
                public Collection<Seed> remove(Collection<Seed> seeds)
                        throws IOException
                {
                    return null;
                }

                @Override
                public Seed create(Map<String, String> properties)
                {
                    return null;
                }

                @Override
                public String getName()
                {
                    return null;
                }

                @Override
                public void setName(String name)
                {
                }
            };
            server.getInstance(Key.get(SeedStoreManager.class)).setSeedStore(seedStore);
            if (provider instanceof LocalStateStoreProvider) {
                Map<String, String> stateStoreProperties = new HashMap<>();
                stateStoreProperties.putIfAbsent(HazelcastConstants.DISCOVERY_MODE_CONFIG_NAME, HazelcastConstants.DISCOVERY_MODE_TCPIP);
                stateStoreProperties.put(DISCOVERY_PORT_CONFIG_NAME, port + "");
                ((LocalStateStoreProvider) provider).setStateStore("hazelcast", stateStoreProperties);
                ((LocalStateStoreProvider) provider).createStateCollections();
            }
            provider.loadStateStore();
            this.providers.add(provider);
        }
    }

    @Override
    public void close()
    {
        cancelAllQueries();
        try {
            closer.close();
            for (StateStoreProvider provider : this.providers) {
                if (provider != null && provider.getStateStore() != null && provider.getStateStore() instanceof HazelcastStateStore) {
                    ((HazelcastStateStore) provider.getStateStore()).terminate();
                }
            }
            for (StateStore stateStore : this.stateStores) {
                if (stateStore != null) {
                    ((HazelcastStateStore) stateStore).terminate();
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
