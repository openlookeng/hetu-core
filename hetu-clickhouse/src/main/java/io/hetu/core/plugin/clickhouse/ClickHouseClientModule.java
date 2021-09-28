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
package io.hetu.core.plugin.clickhouse;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.hetu.core.plugin.clickhouse.optimization.externalfunc.ClickHouseExternalFunctionHub;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.spi.function.ExternalFunctionHub;
import ru.yandex.clickhouse.ClickHouseDriver;
import ru.yandex.clickhouse.settings.ClickHouseConnectionSettings;

import java.util.Optional;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;

public class ClickHouseClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).to(ClickHouseClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        binder.bind(ExternalFunctionHub.class).to(ClickHouseExternalFunctionHub.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ClickHouseConfig.class);
    }

    @Provides
    @Singleton
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, ClickHouseConfig clickHouseConfig)
    {
        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty(ClickHouseConnectionSettings.SOCKET_TIMEOUT.getKey(),
                String.valueOf(clickHouseConfig.getSocketTimeout()));
        return new DriverConnectionFactory(new ClickHouseDriver(), config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties);
    }
}
