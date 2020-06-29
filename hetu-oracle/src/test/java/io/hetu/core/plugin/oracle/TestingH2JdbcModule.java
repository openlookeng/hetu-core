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

package io.hetu.core.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import org.h2.Driver;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static java.lang.String.format;

/**
 * TestingH2JdbcModule
 *
 * @since 2019-07-08
 */
public class TestingH2JdbcModule
        implements Module
{
    /**
     * createProperties
     *
     * @return Map
     */
    public static Map<String, String> createProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", format("jdbc:h2:mem:test%s;DB_CLOSE_DELAY=-1", System.nanoTime()))
                .build();
    }

    /**
     * getConnectionFactory
     *
     * @param config config
     * @return ConnectionFactory
     */
    @Provides
    @Singleton
    public static ConnectionFactory getConnectionFactory(BaseJdbcConfig config)
    {
        Properties connectionProperties = basicConnectionProperties(config);
        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties);
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
    }

    /**
     * provideJdbcClient
     *
     * @param config config
     * @return JdbcClient
     */
    @Provides
    public JdbcClient provideJdbcClient(BaseJdbcConfig config)
    {
        return new BaseJdbcClient(config, "\"", new DriverConnectionFactory(new Driver(), config));
    }
}
