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

package io.hetu.core.plugin.singledata.tidrange;

import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.spi.PrestoException;

import javax.inject.Inject;

import java.lang.reflect.InvocationTargetException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.hetu.core.plugin.singledata.SingleDataUtils.OPENGAUSS_DRIVER_NAME;
import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

public class TidRangeConnectionFactory
        implements ConnectionFactory
{
    private static final Logger LOGGER = Logger.get(TidRangeConnectionFactory.class);

    private final List<ConnectionFactory> connectionFactories;

    @Inject
    public TidRangeConnectionFactory(BaseJdbcConfig baseJdbcConfig)
    {
        Properties connectionProperties = basicConnectionProperties(baseJdbcConfig);
        Driver driver;
        try {
            driver = (Driver) Class.forName(OPENGAUSS_DRIVER_NAME).getConstructor(((Class<?>[]) null)).newInstance();
        }
        catch (InstantiationException | ClassNotFoundException | IllegalAccessException | NoSuchMethodException |
                InvocationTargetException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        String[] connectionUrls = baseJdbcConfig.getConnectionUrl().split(";");
        checkState(connectionUrls.length > 0, "At least one url is required");
        this.connectionFactories = Arrays.stream(connectionUrls).map(url ->
                new DriverConnectionFactory(driver, url,
                        Optional.ofNullable(baseJdbcConfig.getUserCredentialName()),
                        Optional.ofNullable(baseJdbcConfig.getPasswordCredentialName()),
                        connectionProperties))
                .collect(Collectors.toList());
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        if (connectionFactories.size() == 1) {
            return connectionFactories.get(0).openConnection(identity);
        }
        else {
            // Get a connection randomly
            int index = new SecureRandom().nextInt(connectionFactories.size());
            LOGGER.debug("Tid range connector will connect datasource %s", index);
            ConnectionFactory connectionFactory = connectionFactories.get(index);
            return connectionFactory.openConnection(identity);
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        if (connectionFactories != null && connectionFactories.size() > 0) {
            for (ConnectionFactory connectionFactory : connectionFactories) {
                connectionFactory.close();
            }
        }
    }
}
