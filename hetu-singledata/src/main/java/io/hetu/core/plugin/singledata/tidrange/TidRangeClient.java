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
import io.airlift.log.Logger;
import io.hetu.core.plugin.opengauss.OpenGaussClientConfig;
import io.hetu.core.plugin.singledata.BaseSingleDataClient;
import io.hetu.core.plugin.singledata.SingleDataSplit;
import io.hetu.core.plugin.singledata.optimization.SingleDataPushDownContext;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.hetu.core.plugin.singledata.SingleDataUtils.buildPushDownSql;
import static io.hetu.core.plugin.singledata.SingleDataUtils.buildQueryByTableHandle;
import static io.hetu.core.plugin.singledata.SingleDataUtils.getSingleDataSplit;
import static io.hetu.core.plugin.singledata.SingleDataUtils.rewriteQueryWithColumns;
import static io.hetu.core.plugin.singledata.tidrange.TidRangeUtils.getIndexSql;
import static io.hetu.core.plugin.singledata.tidrange.TidRangeUtils.getRelationSizeSql;
import static io.hetu.core.plugin.singledata.tidrange.TidRangeUtils.getSplitSqlFromContext;
import static io.hetu.core.plugin.singledata.tidrange.TidRangeUtils.getTidRangeFilter;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;

public class TidRangeClient
        extends BaseSingleDataClient
{
    private static final Logger LOGGER = Logger.get(TidRangeClient.class);
    private static final String ENABLE_TIDRANGESCAN = "set enable_tidrangescan=on";

    private final long pageSize;
    private final long maxSplitCount;
    private final long defaultSplitSize;

    @Inject
    public TidRangeClient(BaseJdbcConfig config, OpenGaussClientConfig openGaussConfig, TidRangeConfig tidRangeConfig, ConnectionFactory connectionFactory, TypeManager typeManager)
    {
        super(config, openGaussConfig, connectionFactory, typeManager);
        pageSize = tidRangeConfig.getPageSize().toBytes();
        maxSplitCount = tidRangeConfig.getMaxSplitCount();
        defaultSplitSize = tidRangeConfig.getDefaultSplitSize().toBytes();
    }

    @Override
    public Optional<List<SingleDataSplit>> tryGetSplits(ConnectorSession session, SingleDataPushDownContext context)
    {
        List<String> tidRangeFilters = getTidRangeFilters(context.getTableHandle().getTableName(), JdbcIdentity.from(session));
        if (tidRangeFilters.isEmpty()) {
            return Optional.of(ImmutableList.of(getSingleDataSplit(null, buildPushDownSql(context))));
        }
        else {
            ImmutableList.Builder<SingleDataSplit> splits = new ImmutableList.Builder<>();
            for (String filter : tidRangeFilters) {
                splits.add(getSingleDataSplit(null, getSplitSqlFromContext(context, filter)));
            }
            return Optional.of(splits.build());
        }
    }

    @Override
    public ConnectorSplitSource getSplits(JdbcIdentity identity, JdbcTableHandle tableHandle)
    {
        List<String> tidRangeFilters = getTidRangeFilters(tableHandle.getTableName(), identity);
        String query = buildQueryByTableHandle(tableHandle);
        if (tidRangeFilters.isEmpty()) {
            return new FixedSplitSource(ImmutableList.of(getSingleDataSplit(null, query)));
        }
        else {
            ImmutableList.Builder<SingleDataSplit> splits = new ImmutableList.Builder<>();
            for (String filter : tidRangeFilters) {
                splits.add(getSingleDataSplit(null, addTidRangeFilter(query, filter)));
            }
            return new FixedSplitSource(splits.build());
        }
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split) throws SQLException
    {
        return connectionFactory.openConnection(identity);
    }

    @Override
    public PreparedStatement buildSql(
            ConnectorSession session,
            Connection connection,
            JdbcSplit split,
            JdbcTableHandle tableHandle,
            List<JdbcColumnHandle> columns) throws SQLException
    {
        checkState(split instanceof SingleDataSplit, "Need SingleDataSplit");
        try {
            Statement statement = connection.createStatement();
            statement.execute(ENABLE_TIDRANGESCAN);
        }
        catch (Exception e) {
            LOGGER.warn("TidRangeScan plugin is unavailable. The performance may deteriorate.");
        }
        String query = rewriteQueryWithColumns(((SingleDataSplit) split).getSql(), columns);
        LOGGER.debug("Actual SQL: [%s]", query);
        return connection.prepareStatement(query);
    }

    private List<String> getTidRangeFilters(String tableName, JdbcIdentity identity)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            // check whether index exists. If yes, do not use tid_range
            try (PreparedStatement statement = connection.prepareStatement(getIndexSql(tableName));
                    ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    LOGGER.info("Table %s has an index, tidRange is not used", tableName);
                    return ImmutableList.of();
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(getRelationSizeSql(tableName));
                    ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    long relationSize = resultSet.getLong(1);
                    long defaultSplitCount = getSplitSize(relationSize, defaultSplitSize);
                    long splitCount = Long.min(defaultSplitCount, maxSplitCount);
                    long lastPage = getSplitSize(relationSize, pageSize);
                    long pageCountPerSplit = getSplitSize(lastPage, splitCount);

                    LOGGER.debug("Table %s relation size is %s, will be divided into %s splits, %s pages per split",
                            tableName, relationSize, splitCount, pageCountPerSplit);
                    List<String> tidRangeFilters = new ArrayList<>();
                    for (int i = 0; i < splitCount; i++) {
                        long start = i * pageCountPerSplit;
                        long end = Long.min((i + 1) * pageCountPerSplit, lastPage);
                        tidRangeFilters.add(getTidRangeFilter(start, end));
                    }
                    return tidRangeFilters;
                }
                else {
                    throw new PrestoException(JDBC_ERROR, format("Failed to get relation size : %s", tableName));
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e.getMessage());
        }
    }

    private long getSplitSize(long dividend, long divisor)
    {
        long result = dividend / divisor;
        if (dividend % divisor != 0) {
            result++;
        }
        return result;
    }

    private String addTidRangeFilter(String query, String tidRangeFilter)
    {
        return query + " WHERE " + tidRangeFilter;
    }
}
