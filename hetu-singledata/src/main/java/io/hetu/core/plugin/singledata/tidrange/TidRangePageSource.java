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

import com.google.common.base.VerifyException;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.singledata.SingleDataSplit;
import io.hetu.core.plugin.singledata.SingleDataUtils;
import io.prestosql.plugin.jdbc.BlockReadFunction;
import io.prestosql.plugin.jdbc.BooleanReadFunction;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.DoubleReadFunction;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.LongReadFunction;
import io.prestosql.plugin.jdbc.ReadFunction;
import io.prestosql.plugin.jdbc.SliceReadFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTime;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class TidRangePageSource
            implements ConnectorPageSource
{
    private static final Logger LOGGER = Logger.get(TidRangePageSource.class);

    private static final int MAX_ROWS_PER_PAGE = 4096;

    private final int dataSourceId;
    private final PageBuilder pageBuilder;
    private final ConnectorSession session;
    private final List<JdbcColumnHandle> columnHandles;
    private final TidRangeClient client;
    private final String query;
    private final List<Type> types;
    private final AtomicLong readTimeNanos = new AtomicLong(0);

    private boolean closed;
    private boolean connected;
    private CompletableFuture<Connection> future;
    private Connection connection;
    private PreparedStatement statement;
    private ReadFunction[] readFunctions;
    private ResultSet resultSet;
    // use for statistics
    private DateTime startTime;
    private DateTime connectedTime;
    private DateTime executedTime;
    private long rows;

    public TidRangePageSource(TidRangeClient client, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        startTime = DateTime.now();
        this.client = client;
        this.columnHandles = new ArrayList<>();
        this.types = new ArrayList<>();
        this.session = session;
        for (ColumnHandle column : columns) {
            JdbcColumnHandle jdbcColumnHandle = (JdbcColumnHandle) column;
            columnHandles.add(jdbcColumnHandle);
            types.add(jdbcColumnHandle.getColumnType());
        }
        this.pageBuilder = new PageBuilder(types);
        SingleDataSplit singleDataSplit = (SingleDataSplit) split;
        this.dataSourceId = Integer.parseInt(singleDataSplit.getDataSourceName());
        this.query = SingleDataUtils.rewriteQueryWithColumns(singleDataSplit.getSql(), columnHandles);
        LOGGER.debug("Create pageSource with datasource: [%s] query: [%s]", dataSourceId, query);
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return future == null ? NOT_BLOCKED : future;
    }

    @Override
    public Page getNextPage()
    {
        if (!connected) {
            if (future == null) {
                future = openConnectionInternal();
                return null;
            }

            if (!future.isDone()) {
                // connection is waiting
                return null;
            }

            connect();
            return null;
        }
        else {
            return processNextPage();
        }
    }

    private PrestoException handleException(Exception e)
    {
        try {
            close();
        }
        catch (Exception closeException) {
            // ignore exception in close
            LOGGER.error(closeException);
        }
        return new PrestoException(JDBC_ERROR, e);
    }

    private CompletableFuture<Connection> openConnectionInternal()
    {
        long start = System.nanoTime();
        ListenableFuture<Connection> connectionFuture = client.openConnectionSync(dataSourceId);
        connectionFuture = catchingSqlException(connectionFuture);
        connectionFuture.addListener(() -> readTimeNanos.addAndGet(System.nanoTime() - start), directExecutor());
        return toCompletableFuture(nonCancellationPropagating(connectionFuture));
    }

    private static PrestoException throwPrestoException(Exception e)
    {
        if (e instanceof SQLException) {
            throw new PrestoException(JDBC_ERROR, e.getMessage());
        }
        throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
    }

    private static ListenableFuture<Connection> catchingSqlException(ListenableFuture<Connection> future)
    {
        return catchingAsync(future, Exception.class, e -> immediateFailedFuture(throwPrestoException(e)), directExecutor());
    }

    private Page processNextPage()
    {
        if (!closed) {
            for (int i = 0; i < MAX_ROWS_PER_PAGE; i++) {
                if (pageBuilder.isFull()) {
                    break;
                }
                try {
                    if (resultSet.next()) {
                        rows++;
                        pageBuilder.declarePosition();
                        for (int column = 0; column < types.size(); column++) {
                            BlockBuilder output = pageBuilder.getBlockBuilder(column);
                            if (isNull(column)) {
                                output.appendNull();
                            }
                            else {
                                writeValue(output, resultSet, types.get(column), column);
                            }
                        }
                    }
                    else {
                        close();
                        break;
                    }
                }
                catch (RuntimeException | SQLException e) {
                    throw handleException(e);
                }
            }
        }

        if (pageBuilder.isEmpty() || (!closed && !pageBuilder.isFull())) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();

        return page;
    }

    private void connect()
    {
        connectedTime = DateTime.now();
        try {
            this.connection = getFutureValue(future);
            this.statement = connection.prepareStatement(query);
            this.resultSet = statement.executeQuery();
            this.executedTime = DateTime.now();
            this.readFunctions = new ReadFunction[columnHandles.size()];
            for (int i = 0; i < columnHandles.size(); i++) {
                JdbcColumnHandle columnHandle = columnHandles.get(i);
                ColumnMapping columnMapping = client.toPrestoType(session, connection, columnHandle.getJdbcTypeHandle())
                        .orElseThrow(() -> new VerifyException("Unsupported column type"));
                readFunctions[i] = columnMapping.getReadFunction();
            }
            connected = true;
            future = null;
        }
        catch (RuntimeException | SQLException e) {
            throw handleException(e);
        }
    }

    private boolean isNull(int field)
    {
        try {
            resultSet.getObject(field + 1);
            return resultSet.wasNull();
        }
        catch (SQLException | RuntimeException e) {
            throw handleException(e);
        }
    }

    private void writeValue(BlockBuilder output, ResultSet resultSet, Type type, int column)
            throws SQLException
    {
        int columnIndex = column + 1;
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            BooleanReadFunction function = (BooleanReadFunction) readFunctions[column];
            type.writeBoolean(output, function.readBoolean(resultSet, columnIndex));
        }
        else if (javaType == long.class) {
            LongReadFunction function = (LongReadFunction) readFunctions[column];
            type.writeLong(output, function.readLong(resultSet, columnIndex));
        }
        else if (javaType == double.class) {
            DoubleReadFunction function = (DoubleReadFunction) readFunctions[column];
            type.writeDouble(output, function.readDouble(resultSet, columnIndex));
        }
        else if (javaType == Slice.class) {
            SliceReadFunction function = (SliceReadFunction) readFunctions[column];
            Slice slice = function.readSlice(resultSet, columnIndex);
            type.writeSlice(output, slice, 0, slice.length());
        }
        else {
            BlockReadFunction function = (BlockReadFunction) readFunctions[column];
            type.writeObject(output, function.readBlock(resultSet, columnIndex));
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return pageBuilder.getSizeInBytes();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }

        closed = true;
        DateTime toCloseTime = DateTime.now();

        if (future != null) {
            if (future.isDone()) {
                try (Connection con = getFutureValue(future)) {
                    checkArgument(true, "Just for close connection");
                }
                catch (SQLException e) {
                    // ignore exception in close
                    LOGGER.error(e);
                }
            }
            future.cancel(true);
        }

        if (resultSet != null) {
            try (ResultSet rs = resultSet) {
                checkArgument(true, "Just for close resultSet");
            }
            catch (SQLException e) {
                // ignore exception in close
                LOGGER.error(e);
            }
        }

        if (statement != null) {
            try (PreparedStatement stat = statement) {
                checkArgument(true, "Just for close statement");
            }
            catch (SQLException e) {
                // ignore exception in close
                LOGGER.error(e);
            }
        }

        if (connection != null) {
            try (Connection con = connection) {
                checkArgument(true, "Just for close connection");
            }
            catch (SQLException e) {
                // ignore exception in close
                LOGGER.error(e);
            }
        }

        DateTime closedTime = DateTime.now();
        if (connection == null || executedTime == null) {
            LOGGER.debug("This pageSource does not connect to dataSource before close, total cost %dms", closedTime.getMillis() - startTime.getMillis());
        }
        else {
            LOGGER.debug("open connection cost %dms, execute cost %dms, fetch data cost %dms, connect close cost %dms, total cost %dms, fetch rows %d.",
                    connectedTime.getMillis() - startTime.getMillis(),
                    executedTime.getMillis() - connectedTime.getMillis(),
                    toCloseTime.getMillis() - executedTime.getMillis(),
                    closedTime.getMillis() - toCloseTime.getMillis(),
                    closedTime.getMillis() - startTime.getMillis(),
                    rows);
        }
    }
}
