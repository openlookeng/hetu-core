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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.metastore.HetuMetaStoreFactory;
import io.prestosql.spi.metastore.HetuMetastore;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class HetuFsMetastoreFactory
        implements HetuMetaStoreFactory
{
    private static final String FACTORY_TYPE = "hetufilesystem";
    private final ClassLoader classLoader;

    @Override
    public String getName()
    {
        return FACTORY_TYPE;
    }

    public HetuFsMetastoreFactory(ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public HetuMetastore create(String name, Map<String, String> config, HetuFileSystemClient client)
    {
        requireNonNull(config, "config is null");
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(new HetuFsMetastoreModule(client));
            Injector injector =
                    app.strictConfig().doNotInitializeLogging().setRequiredConfigurationProperties(config).initialize();

            return injector.getInstance(HetuMetastore.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new ExceptionInInitializerError();
        }
    }
}
