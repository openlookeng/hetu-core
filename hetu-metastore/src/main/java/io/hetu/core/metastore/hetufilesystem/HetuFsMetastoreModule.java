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
package io.hetu.core.metastore.hetufilesystem;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.hetu.core.metastore.ForHetuMetastoreCache;
import io.hetu.core.metastore.HetuMetastoreCache;
import io.hetu.core.metastore.HetuMetastoreCacheConfig;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.metastore.HetuMetastore;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HetuFsMetastoreModule
        implements Module
{
    private final HetuFileSystemClient client;

    public HetuFsMetastoreModule(HetuFileSystemClient client)
    {
        this.client = client;
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(HetuFsMetastoreConfig.class);
        configBinder(binder).bindConfig(HetuMetastoreCacheConfig.class);
        binder.bind(HetuMetastore.class).annotatedWith(ForHetuMetastoreCache.class)
                .to(HetuFsMetastore.class).in(Scopes.SINGLETON);
        binder.bind(HetuMetastore.class).to(HetuMetastoreCache.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HetuMetastore.class)
                .as(generator -> generator.generatedNameOf(HetuMetastoreCache.class));
    }

    @Provides
    @HetuFsMetadataClient
    public HetuFileSystemClient getHetuFileSystemClient()
    {
        return client;
    }
}
