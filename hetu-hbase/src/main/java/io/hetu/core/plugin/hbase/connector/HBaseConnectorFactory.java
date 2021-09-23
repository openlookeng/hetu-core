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
package io.hetu.core.plugin.hbase.connector;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.hetu.core.plugin.hbase.HBaseModule;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.metastore.HetuMetastore;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/**
 * HBaseConnectorFactory
 *
 * @since 2020-03-30
 */
public class HBaseConnectorFactory
        implements ConnectorFactory
{
    private static final Logger LOG = Logger.get(HBaseConnectorFactory.class);
    private static HetuMetastore hetuMetastore;
    private final String name;
    private final ClassLoader classLoader;
    private final Module module;

    /**
     * constructor
     *
     * @param name name
     * @param module module
     * @param classLoader classLoader
     */
    public HBaseConnectorFactory(String name, Module module, ClassLoader classLoader)
    {
        this.name = requireNonNull(name, "name is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.module = requireNonNull(module, "module is null");
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new HBaseConnectorHandlerResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");
        HBaseConnectorId.setConnectorId(catalogName);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            hetuMetastore = context.getHetuMetastore();
            Bootstrap app = new Bootstrap(binder ->
                    binder.bind(HetuMetastore.class).toInstance(context.getHetuMetastore()),
                    new HBaseModule(), this.module);
            Injector injector =
                    app.strictConfig().doNotInitializeLogging().quiet().setRequiredConfigurationProperties(config).initialize();
            return injector.getInstance(HBaseConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            LOG.error("create HBaseConnector failed... cause by %s", e.getMessage());
            throw new ExceptionInInitializerError();
        }
    }

    public static HetuMetastore getHetuMetastore()
    {
        return hetuMetastore;
    }
}
