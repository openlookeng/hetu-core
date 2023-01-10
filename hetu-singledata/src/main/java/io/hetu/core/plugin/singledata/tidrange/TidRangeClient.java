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
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.hetu.core.plugin.opengauss.OpenGaussClientConfig;
import io.hetu.core.plugin.singledata.BaseSingleDataClient;
import io.hetu.core.plugin.singledata.SingleDataSplit;
import io.hetu.core.plugin.singledata.optimization.SingleDataPushDownContext;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.hetu.core.plugin.singledata.SingleDataUtils.buildPushDownSql;
import static io.hetu.core.plugin.singledata.SingleDataUtils.buildQueryByTableHandle;
import static io.hetu.core.plugin.singledata.SingleDataUtils.getSingleDataSplit;
import static io.hetu.core.plugin.singledata.tidrange.TidRangeUtils.getBlockSizeSql;
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

    private static final DataSize DEFAULT_SPLIT_SIZE = new DataSize(64, DataSize.Unit.MEGABYTE);

    private final int maxTableSplitCountPerNode;
    private final TidRangeConnectionFactory tidRangeConnectionFactory;

    @Inject
    public TidRangeClient(BaseJdbcConfig config, OpenGaussClientConfig openGaussConfig, TidRangeConfig tidRangeConfig, ConnectionFactory connectionFactory, TypeManager typeManager)
    {
        super(config, openGaussConfig, connectionFactory, typeManager);
        this.maxTableSplitCountPerNode = tidRangeConfig.getMaxTableSplitCountPerNode();
        this.tidRangeConnectionFactory = (TidRangeConnectionFactory) connectionFactory;
    }

    @Override
    public Optional<List<SingleDataSplit>> tryGetSplits(ConnectorSession session, SingleDataPushDownContext context)
    {
        List<TidRangeSplit> tidRangeSplits = getTidRangeSplits(context.getTableHandle().getTableName(), JdbcIdentity.from(session));
        if (tidRangeSplits.isEmpty()) {
            return Optional.of(getSingleSplit(buildPushDownSql(context)));
        }
        else {
            ImmutableList.Builder<SingleDataSplit> splits = new ImmutableList.Builder<>();
            for (TidRangeSplit split : tidRangeSplits) {
                splits.add(getSingleDataSplit(split.dataSourceId, getSplitSqlFromContext(context, split.splitFilter)));
            }
            return Optional.of(splits.build());
        }
    }

    @Override
    public ConnectorSplitSource getSplits(JdbcIdentity identity, JdbcTableHandle tableHandle)
    {
        List<TidRangeSplit> tidRangeSplits = getTidRangeSplits(tableHandle.getTableName(), identity);
        String query = buildQueryByTableHandle(tableHandle);
        if (tidRangeSplits.isEmpty()) {
            return new FixedSplitSource(getSingleSplit(query));
        }
        else {
            ImmutableList.Builder<SingleDataSplit> splits = new ImmutableList.Builder<>();
            for (TidRangeSplit split : tidRangeSplits) {
                splits.add(getSingleDataSplit(split.dataSourceId, addTidRangeFilter(query, split.splitFilter)));
            }
            return new FixedSplitSource(splits.build());
        }
    }

    private List<SingleDataSplit> getSingleSplit(String query)
    {
        String dataSourceId = String.valueOf(tidRangeConnectionFactory.getLastFactoryId());
        return ImmutableList.of(getSingleDataSplit(dataSourceId, query));
    }

    public ListenableFuture<Connection> openConnectionSync(int id)
    {
        return tidRangeConnectionFactory.openConnectionSync(id);
    }

    private List<TidRangeSplit> getTidRangeSplits(String tableName, JdbcIdentity identity)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            // check whether index exists. If yes, do not use tid_range
            try (PreparedStatement statement = connection.prepareStatement(getIndexSql(tableName));
                     ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    LOGGER.info("Table [%s] has an index, tidRange will not be enabled", tableName);
                    return ImmutableList.of();
                }
            }

            long blockSize;
            try (PreparedStatement statement = connection.prepareStatement(getBlockSizeSql());
                    ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    blockSize = resultSet.getLong(1);
                }
                else {
                    throw new PrestoException(JDBC_ERROR, "Get block_size from openGauss failed.");
                }
            }

            long relationSize;
            try (PreparedStatement statement = connection.prepareStatement(getRelationSizeSql(tableName));
                     ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    relationSize = resultSet.getLong(1);
                    if (relationSize == 0) {
                        return ImmutableList.of();
                    }
                }
                else {
                    throw new PrestoException(JDBC_ERROR, format("Failed to get relation size : %s", tableName));
                }
            }

            List<Integer> availableFactoryIds = tidRangeConnectionFactory.getAvailableConnections(identity);
            int nodeCount = availableFactoryIds.size();
            long maxSplitCount = (long) maxTableSplitCountPerNode * nodeCount;
            long defaultSplitCount = getSplitSize(relationSize, DEFAULT_SPLIT_SIZE.toBytes());
            long splitCount = Long.min(maxSplitCount, defaultSplitCount);
            checkState(splitCount > 0, "splitCount must greater than 0");

            long lastPage = getSplitSize(relationSize, blockSize);
            long pageCountPerSplit = getSplitSize(lastPage, splitCount);

            LOGGER.debug("Table %s relation size is %s, will be divided into %s splits, %s pages per split",
                    tableName, relationSize, splitCount, pageCountPerSplit);

            List<TidRangeSplit> tidRangeSplits = new ArrayList<>();
            for (int i = 0; i < splitCount; i++) {
                long start = i * pageCountPerSplit;
                long end = Long.min((i + 1) * pageCountPerSplit, lastPage);
                String dataSourceId = String.valueOf(availableFactoryIds.get(i % nodeCount));
                tidRangeSplits.add(new TidRangeSplit(dataSourceId, getTidRangeFilter(start, end)));
            }
            return tidRangeSplits;
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

    private static class TidRangeSplit
    {
        private final String dataSourceId;
        private final String splitFilter;

        public TidRangeSplit(String id, String filter)
        {
            this.dataSourceId = id;
            this.splitFilter = filter;
        }
    }
}
