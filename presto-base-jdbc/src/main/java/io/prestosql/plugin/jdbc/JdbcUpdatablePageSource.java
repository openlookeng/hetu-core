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
package io.prestosql.plugin.jdbc;

import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.connector.UpdatablePageSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

public class JdbcUpdatablePageSource
        implements UpdatablePageSource
{
    private final ConnectorSession session;
    private final RecordPageSource inner;
    private final ConnectorTableHandle handle;
    private final JdbcClient jdbcClient;
    private final JdbcSplit jdbcSplit;
    private final Connection connection;
    private final int batchSizeLimit = 1000;
    private boolean dmlStatementsCommittedAsAnTransaction;

    public JdbcUpdatablePageSource(RecordSet recordSet, ConnectorSession session,
                                   ConnectorTableHandle handle,
                                   JdbcClient jdbcClient,
                                   BaseJdbcConfig config,
                                   JdbcSplit jdbcSplit)
    {
        this.inner = new RecordPageSource(recordSet);
        this.session = session;
        this.handle = handle;
        this.jdbcClient = jdbcClient;
        this.jdbcSplit = jdbcSplit;
        this.dmlStatementsCommittedAsAnTransaction = config.isDmlStatementsCommitInATransaction();
        try {
            this.connection = jdbcClient.getConnection(JdbcIdentity.from(session), jdbcSplit);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        try {
            int batchSize = 0;
            boolean autoCommitFlag = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(jdbcClient.buildDeleteSql(handle))) {
                for (int position = 0; position < rowIds.getPositionCount(); position++) {
                    jdbcClient.setDeleteSql(statement, rowIds, position);
                    statement.addBatch();
                    batchSize++;

                    if (batchSize >= batchSizeLimit) {
                        statement.executeBatch();
                        batchSize = 0;
                        if (!dmlStatementsCommittedAsAnTransaction) {
                            connection.commit();
                            connection.setAutoCommit(false);
                        }
                    }
                }
                if (batchSize > 0) {
                    statement.executeBatch();
                }
                if (dmlStatementsCommittedAsAnTransaction || batchSize > 0) {
                    connection.commit();
                }
                connection.setAutoCommit(autoCommitFlag);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels, List<String> updatedColumns)
    {
        try {
            List<Block> columnValueAndRowIdBlock = columnValueAndRowIdChannels
                    .stream()
                    .map(index -> page.getBlock(index))
                    .collect(Collectors.toList());
            Block rowIds = columnValueAndRowIdBlock.get(columnValueAndRowIdBlock.size() - 1);

            boolean autoCommitFlag = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(jdbcClient.buildUpdateSql(this.session, handle, updatedColumns.size(), updatedColumns))) {
                int batchSize = 0;
                for (int position = 0; position < rowIds.getPositionCount(); position++) {
                    jdbcClient.setUpdateSql(this.session, this.handle, statement, columnValueAndRowIdBlock, position, updatedColumns);
                    statement.addBatch();
                    batchSize++;

                    if (batchSize >= 1000) {
                        statement.executeBatch();
                        batchSize = 0;
                        if (!dmlStatementsCommittedAsAnTransaction) {
                            connection.commit();
                            connection.setAutoCommit(false);
                        }
                    }
                }
                if (batchSize > 0) {
                    statement.executeBatch();
                }
                if (dmlStatementsCommittedAsAnTransaction || batchSize > 0) {
                    connection.commit();
                }
                connection.setAutoCommit(autoCommitFlag);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return inner.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return inner.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return inner.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return inner.getNextPage();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return inner.getSystemMemoryUsage();
    }

    @Override
    public void close() throws IOException
    {
        inner.close();
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        CompletableFuture<Collection<Slice>> cf = new CompletableFuture<>();
        cf.complete(Collections.emptyList());
        return cf;
    }

    @Override
    public void abort()
    {
        // rollback and close
        try (Connection connection = this.connection) {
            if (!connection.isClosed()) {
                connection.rollback();
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
}
