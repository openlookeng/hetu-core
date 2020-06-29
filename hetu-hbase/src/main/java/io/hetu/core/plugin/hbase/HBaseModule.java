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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.conf.HBaseTableProperties;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.connector.HBaseConnector;
import io.hetu.core.plugin.hbase.connector.HBaseConnectorHandlerResolver;
import io.hetu.core.plugin.hbase.connector.HBaseConnectorMetadataFactory;
import io.hetu.core.plugin.hbase.query.HBasePageSinkProvider;
import io.hetu.core.plugin.hbase.query.HBasePageSourceProvider;
import io.hetu.core.plugin.hbase.query.HBaseRecordSetProvider;
import io.hetu.core.plugin.hbase.split.HBaseSplitManager;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.procedure.Procedure;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * HBaseModule
 *
 * @since 2020-03-18
 */
public class HBaseModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        newOptionalBinder(binder, ConnectorAccessControl.class);
        newSetBinder(binder, Procedure.class);

        binder.bind(HBaseConnection.class).in(Scopes.SINGLETON);
        binder.bind(HBaseConnector.class).in(Scopes.SINGLETON);
        binder.bind(HBaseTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(HBaseConnectorHandlerResolver.class).in(Scopes.SINGLETON);
        binder.bind(HBaseConnectorMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(HBaseSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HBaseRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(HBasePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(HBaseTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(HBasePageSourceProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(HBaseConfig.class);
    }
}
