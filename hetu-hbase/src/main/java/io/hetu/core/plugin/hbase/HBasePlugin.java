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
package io.hetu.core.plugin.hbase;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.hetu.core.plugin.hbase.connector.HBaseConnectorFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

/**
 * HBasePlugin
 *
 * @since 2020-03-18
 */
public class HBasePlugin
        implements Plugin
{
    private String connectorId;

    private Module module;

    public HBasePlugin()
    {
        this("hbase-connector", new HBaseModule());
    }

    public HBasePlugin(String connectorId, Module module)
    {
        this.connectorId = connectorId;
        this.module = module;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        // connector.name
        return ImmutableList.of(new HBaseConnectorFactory(this.connectorId, module, getClassLoader()));
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = HBasePlugin.class.getClassLoader();
        }
        return classLoader;
    }
}
