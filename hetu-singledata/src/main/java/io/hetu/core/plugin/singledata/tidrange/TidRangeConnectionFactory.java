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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.spi.PrestoException;

import javax.inject.Inject;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.hetu.core.plugin.singledata.SingleDataUtils.OPENGAUSS_DRIVER_NAME;
import static io.hetu.core.plugin.singledata.tidrange.TidRangeUtils.getMaxConnectionsSql;
import static io.hetu.core.plugin.singledata.tidrange.TidRangeUtils.getPluginStatusSql;
import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class TidRangeConnectionFactory
        implements ConnectionFactory
{
    private static final Logger LOGGER = Logger.get(TidRangeConnectionFactory.class);
    private static final int FIRST_COLUMN_INDEX = 1;
    private static final long IDLE_TIME = 10000L;
    private static final Driver DRIVER;

    private final ListeningExecutorService executorService = listeningDecorator(
            newFixedThreadPool(Runtime.getRuntime().availableProcessors(), threadsNamed("tidrange-connection-%s")));

    // get metadata don't use connection pool
    private final List<ConnectionFactory> factories;
    private final List<HikariDataSource> dataSources;
    private final AtomicReference<Integer> lastFactoryId = new AtomicReference<>();

    static {
        try {
            DRIVER = (Driver) Class.forName(OPENGAUSS_DRIVER_NAME).getConstructor(((Class<?>[]) null)).newInstance();
        }
        catch (InstantiationException | ClassNotFoundException | IllegalAccessException | NoSuchMethodException |
                InvocationTargetException e) {
            throw new PrestoException(JDBC_ERROR, "Failed to load openGauss JDBC driver");
        }
    }

    @Inject
    public TidRangeConnectionFactory(BaseJdbcConfig baseJdbcConfig, TidRangeConfig tidRangeConfig)
    {
        this.dataSources = this.checkAndGetDataSources(baseJdbcConfig, tidRangeConfig);
        this.factories = this.getConnectionFactories(baseJdbcConfig);
        this.lastFactoryId.set(0);
        LOGGER.info("Create TidRangeConnectionFactory with %s data sources", dataSources.size());
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
    {
        return openConnectionSync(identity);
    }

    public int getLastFactoryId()
    {
        return lastFactoryId.get();
    }

    private synchronized Connection openConnectionSync(JdbcIdentity identity)
    {
        if (lastFactoryId.get() != null) {
            try {
                return factories.get(lastFactoryId.get()).openConnection(identity);
            }
            catch (SQLException e) {
                LOGGER.warn("Get connection from dataSource [%s] failed, will try next", lastFactoryId.get());
            }
        }

        return findFirstAvailableConnection(identity);
    }

    public Connection findFirstAvailableConnection(JdbcIdentity identity)
    {
        for (int i = 0; i < factories.size(); i++) {
            try {
                Connection connection = factories.get(i).openConnection(identity);
                if (connection != null) {
                    lastFactoryId.set(i);
                    return connection;
                }
            }
            catch (SQLException e) {
                LOGGER.warn("Get connection from dataSource [%s]:[%s] failed, will try next", i, dataSources.get(i).getJdbcUrl());
            }
        }

        throw new PrestoException(JDBC_ERROR, "No available openGauss dataSource");
    }

    public List<Integer> getAvailableConnections(JdbcIdentity identity)
    {
        List<Integer> availableDataSources = new ArrayList<>();
        for (int i = 0; i < factories.size(); i++) {
            try (Connection connection = factories.get(i).openConnection(identity)) {
                availableDataSources.add(i);
            }
            catch (SQLException e) {
                LOGGER.warn("Connect to dataSource [%s] failed, %s, skip it", i, e.getMessage());
            }
        }
        if (availableDataSources.isEmpty()) {
            throw new PrestoException(JDBC_ERROR, "No available data source");
        }
        LOGGER.info("Available dataSource count: %s", availableDataSources.size());
        return availableDataSources;
    }

    public ListenableFuture<Connection> openConnectionSync(int id)
    {
        return executorService.submit(() -> openConnectionById(id));
    }

    public Connection openConnectionById(int id)
    {
        try {
            return dataSources.get(id).getConnection();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, format("Connect to dataSource [%s] failed, %s", dataSources.get(id).getJdbcUrl(), e.getMessage()));
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        executorService.shutdown();
        for (HikariDataSource dataSource : dataSources) {
            dataSource.close();
        }
        for (ConnectionFactory factory : factories) {
            factory.close();
        }
    }

    private List<ConnectionFactory> getConnectionFactories(BaseJdbcConfig baseJdbcConfig)
    {
        Properties connectionProperties = basicConnectionProperties(baseJdbcConfig);
        String[] connectionUrls = baseJdbcConfig.getConnectionUrl().split(";");
        checkState(connectionUrls.length > 0, "At least one url is required");
        return Arrays.stream(connectionUrls).map(url ->
                        new DriverConnectionFactory(DRIVER, url,
                                Optional.ofNullable(baseJdbcConfig.getUserCredentialName()),
                                Optional.ofNullable(baseJdbcConfig.getPasswordCredentialName()),
                                connectionProperties))
                .collect(Collectors.toList());
    }

    private List<HikariDataSource> checkAndGetDataSources(BaseJdbcConfig baseJdbcConfig, TidRangeConfig tidRangeConfig)
    {
        int maxConnectionCountPerNode = tidRangeConfig.getMaxConnectionCountPerNode();
        long connectionTimeout = tidRangeConfig.getConnectionTimeout();
        if (connectionTimeout != 0) {
            // connection timeout must greater than 250ms or equal to 0 (no timeout)
            checkState(connectionTimeout >= 250L, "connectionTimeout cannot be less than 250ms");
        }

        String[] connectionUrls = baseJdbcConfig.getConnectionUrl().split(";");
        checkState(connectionUrls.length > 0, "At least one url is required");

        ImmutableList.Builder<HikariDataSource> dataSourceBuilder = new ImmutableList.Builder<>();

        for (String connectionUrl : connectionUrls) {
            String url = connectionUrl.trim();

            HikariConfig config = new HikariConfig();
            config.setDriverClassName(OPENGAUSS_DRIVER_NAME);
            config.setJdbcUrl(url);
            config.setUsername(baseJdbcConfig.getConnectionUser());
            config.setPassword(baseJdbcConfig.getConnectionPassword());
            config.setMaximumPoolSize(maxConnectionCountPerNode);
            config.setMinimumIdle(0);
            config.setIdleTimeout(IDLE_TIME);
            config.setConnectionTimeout(connectionTimeout);
            config.setMaxLifetime(tidRangeConfig.getMaxLifetime());
            HikariDataSource dataSource = new HikariDataSource(config);
            checkDataSource(dataSource, maxConnectionCountPerNode);

            dataSourceBuilder.add(dataSource);
        }

        return dataSourceBuilder.build();
    }

    private static void checkDataSource(HikariDataSource dataSource, int maxConnectionSize)
    {
        try (Connection connection = dataSource.getConnection()) {
            // check tidrangescan plugin is installed and enabled
            try (PreparedStatement statement = connection.prepareStatement(getPluginStatusSql());
                    ResultSet resultSet = statement.executeQuery()) {
                if (!(resultSet.next() && resultSet.getString(FIRST_COLUMN_INDEX).equals("on"))) {
                    throw new PrestoException(JDBC_ERROR, format("tidrangescan plugin is not enabled in dataSource [%s]", dataSource.getJdbcUrl()));
                }
            }
            catch (SQLException e) {
                throw new PrestoException(JDBC_ERROR, format("tidrangescan plugin is not installed in dataSource [%S]", dataSource.getJdbcUrl()));
            }

            // check tidrangescan.maxConnectionCountPerNode cannot greater than max_connections in openGauss
            try (PreparedStatement statement = connection.prepareStatement(getMaxConnectionsSql());
                    ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    int maxConnections = resultSet.getInt(FIRST_COLUMN_INDEX);
                    checkState(maxConnections >= maxConnectionSize,
                            format("tidrange.maxConnectionCountPerNode cannot greater than max_connections [%s] to the datasource", maxConnections));
                }
                else {
                    throw new PrestoException(JDBC_ERROR, format("Failed to get max_connections from datasource [%s]", dataSource.getJdbcUrl()));
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e.getMessage());
        }
    }
}
