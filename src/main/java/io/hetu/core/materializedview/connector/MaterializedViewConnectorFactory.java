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

package io.hetu.core.materializedview.connector;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.hetu.core.materializedview.MaterializedViewModule;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/**
 * MV connector factory for create MV connector
 *
 * @since 2020-03-25
 */
public class MaterializedViewConnectorFactory
        implements ConnectorFactory
{
    private final String name = "mv";
    private final ClassLoader classLoader;

    /**
     * Constructor of connector factory
     *
     * @param classLoader classLoader
     */

    public MaterializedViewConnectorFactory(ClassLoader classLoader)
    {
        this.classLoader = classLoader;
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new MaterializedViewConnectorHandlerResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(new MaterializedViewModule(context.getTypeManager(), context.getHetuMetastore()));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(MaterializedViewConnector.class);
            // CHECKSTYLE:OFF:IllegalCatch
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            // CHECKSTYLE:OFF:IllegalCatch
            throw new RuntimeException(e);
        }
    }
}
