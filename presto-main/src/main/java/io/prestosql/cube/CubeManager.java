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

package io.prestosql.cube;

import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.spi.cube.CubeProvider;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.sql.analyzer.FeaturesConfig;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class CubeManager
{
    public static final String STAR_TREE = "star-tree";

    private final Map<String, CubeProvider> cubeProviders = new HashMap<>();
    private final Map<String, CubeMetaStore> cubeMetaStores = new HashMap<>();
    private final HetuMetaStoreManager metaStoreManager;
    private final Properties properties = new Properties();

    @Inject
    public CubeManager(FeaturesConfig config, HetuMetaStoreManager metaStoreManager)
    {
        this.metaStoreManager = requireNonNull(metaStoreManager, "metaStoreManager is null");
        properties.setProperty("cache-ttl", Long.toString(config.getCubeMetadataCacheTtl().toMillis()));
        properties.setProperty("cache-size", Long.toString(config.getCubeMetadataCacheSize()));
    }

    private CubeManager(HetuMetaStoreManager hetuMetaStoreManager)
    {
        this.metaStoreManager = requireNonNull(hetuMetaStoreManager, "metaStoreManager is null");
    }

    public static CubeManager getNoOpCubeManager()
    {
        return new CubeManager(new HetuMetaStoreManager());
    }

    public synchronized void addCubeProvider(CubeProvider cubeProvider)
    {
        String name = cubeProvider.getName();
        checkState(!cubeProviders.containsKey(name), "A cube %s already exists", name);
        cubeProviders.put(name, cubeProvider);
    }

    public synchronized Optional<CubeProvider> getCubeProvider(String name)
    {
        return Optional.ofNullable(this.cubeProviders.get(name));
    }

    public synchronized Optional<CubeMetaStore> getMetaStore(String name)
    {
        CubeMetaStore cubeMetaStore = this.cubeMetaStores.get(name);
        if (cubeMetaStore != null) {
            return Optional.of(cubeMetaStore);
        }

        CubeProvider cubeProvider = this.cubeProviders.get(name);
        if (cubeProvider != null) {
            HetuMetastore hetuMetastore = metaStoreManager.getHetuMetastore();
            if (hetuMetastore == null) {
                return Optional.empty();
            }
            cubeMetaStore = cubeProvider.getCubeMetaStore(metaStoreManager.getHetuMetastore(), properties);
            this.cubeMetaStores.put(name, cubeMetaStore);
            return Optional.of(cubeMetaStore);
        }
        return Optional.empty();
    }
}
