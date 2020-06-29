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
package io.hetu.core.plugin.hana;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.spi.PrestoException;

import java.lang.reflect.InvocationTargetException;
import java.sql.Driver;
import java.util.Optional;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

/**
 * Guice implementation to create the correct DI and binds
 *
 * @since 2019-07-10
 */
public class HanaClientModule
        implements Module
{
    private static final Logger log = Logger.get(HanaClientModule.class);

    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).to(HanaClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(HanaConfig.class);
    }

    /**
     * createConnectionFactory
     *
     * @param config BaseJdbcConfig
     * @param hanaConfig HanaConfig
     * @return ConnectionFactory
     */
    @Provides
    @Singleton
    public static ConnectionFactory getConnectionFactory(BaseJdbcConfig config, HanaConfig hanaConfig)
    {
        Properties connectionProperties = basicConnectionProperties(config);

        connectionProperties.setProperty("autocommit", String.valueOf(hanaConfig.isAutoCommit()));
        connectionProperties.setProperty("communicationTimeout", String.valueOf(hanaConfig.getCommunicationTimeout()));
        connectionProperties.setProperty("encrypt", String.valueOf(hanaConfig.isEncrypt()));
        connectionProperties.setProperty("packetsize", String.valueOf(hanaConfig.getPacketSize()));
        connectionProperties.setProperty("readOnly", String.valueOf(hanaConfig.isReadOnly()));
        connectionProperties.setProperty("reconnect", String.valueOf(hanaConfig.isReconnect()));

        log.info("Hana connection properties: %s", connectionProperties);
        Driver driver = null;
        try {
            driver = (Driver) Class.forName(HanaConstants.SAP_HANA_JDBC_DRIVER_CLASS_NAME).getConstructor(((Class<?>[]) null)).newInstance();
        }
        catch (InstantiationException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        catch (IllegalAccessException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        catch (InvocationTargetException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        catch (NoSuchMethodException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        return new DriverConnectionFactory(driver, config.getConnectionUrl(), Optional.ofNullable(config.getUserCredentialName()), Optional.ofNullable(config.getPasswordCredentialName()), connectionProperties);
    }
}
