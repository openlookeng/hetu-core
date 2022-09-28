/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.singledata;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.hetu.core.plugin.opengauss.OpenGaussClientConfig;
import io.hetu.core.plugin.singledata.optimization.SingleDataPlanOptimizer;
import io.hetu.core.plugin.singledata.optimization.SingleDataPlanOptimizerProvider;
import io.hetu.core.plugin.singledata.shardingsphere.ShardingSphereModule;
import io.hetu.core.plugin.singledata.tidrange.TidRangeModule;
import io.prestosql.plugin.jdbc.InternalBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcMetadataConfig;
import io.prestosql.plugin.jdbc.JdbcPageSourceProvider;
import io.prestosql.plugin.jdbc.JdbcRecordSetProvider;
import io.prestosql.plugin.jdbc.jmx.StatisticsAwareJdbcClient;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.procedure.Procedure;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.hetu.core.plugin.singledata.SingleDataConnectorFactory.CONNECTOR_NAME;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class SingleDataModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger LOGGER = Logger.get(SingleDataModule.class);

    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, ConnectorAccessControl.class);
        newSetBinder(binder, Procedure.class);

        binder.bind(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(SingleDataSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(SingleDataConnector.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPlanOptimizer.class).to(SingleDataPlanOptimizer.class).in(Scopes.SINGLETON);
        binder.bind(SingleDataPlanOptimizerProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        configBinder(binder).bindConfig(OpenGaussClientConfig.class);

        switch (buildConfigObject(SingleDataModeConfig.class).getSingleDataMode()) {
            case TID_RANGE:
                LOGGER.info("SingleData will start with TID_RANGE mode");
                binder.install(new TidRangeModule());
                break;
            case SHARDING_SPHERE:
                LOGGER.info("SingleData will start with SHARDING_SPHERE mode");
                binder.install(new ShardingSphereModule());
                break;
            default:
                throw new UnsupportedOperationException("Only support mode TID_RANGE or SHARDING_SPHERE.");
        }

        newExporter(binder).export(Key.get(JdbcClient.class, InternalBaseJdbc.class))
                .as(generator -> generator.generatedNameOf(JdbcClient.class, CONNECTOR_NAME));
    }

    @Provides
    @Singleton
    @InternalBaseJdbc
    public static JdbcClient createJdbcClientWithStats(JdbcClient client)
    {
        return new StatisticsAwareJdbcClient(client);
    }
}
