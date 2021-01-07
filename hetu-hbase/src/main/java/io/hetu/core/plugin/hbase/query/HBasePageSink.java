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
package io.hetu.core.plugin.hbase.query;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.hetu.core.plugin.hbase.utils.Constants;
import io.hetu.core.plugin.hbase.utils.HBaseErrorCode;
import io.hetu.core.plugin.hbase.utils.serializers.HBaseRowSerializer;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.QUERY_REJECTED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * HBasePageSink
 *
 * @since 2020-03-18
 */
public class HBasePageSink
        implements ConnectorPageSink
{
    private static final Logger LOG = Logger.get(HBasePageSink.class);

    private final HBaseRowSerializer serializer;

    private final HBaseConnection hbaseConn;

    private final List<HBaseColumnHandle> columns;

    private final String tablename;

    private final int rowIdOrdinal;

    /**
     * constructor
     *
     * @param hbaseConn hbaseConn
     * @param table table
     */
    public HBasePageSink(HBaseConnection hbaseConn, HBaseTableHandle table)
    {
        requireNonNull(table, "table is null");

        this.columns = table.getColumns();
        this.hbaseConn = hbaseConn;
        this.serializer = HBaseRowSerializer.getSerializerInstance(table.getSerializerClassName());
        this.tablename = table.getHbaseTableName().get();
        this.rowIdOrdinal = table.getRowIdOrdinal();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        // For each position within the page
        List<Put> puts = new ArrayList<>();

        try {
            for (int position = 0; position < page.getPositionCount(); ++position) {
                // Convert Page to a Put, writing and indexing it
                Put put = pageToPut(page, position);
                puts.add(put);
                if (puts.size() >= Constants.PUT_BATCH_SIZE) {
                    hbaseConn.getConn().getTable(TableName.valueOf(tablename)).put(puts);
                    puts.clear();
                }
            }
            if (!puts.isEmpty()) {
                hbaseConn.getConn().getTable(TableName.valueOf(tablename)).put(puts);
            }
        }
        catch (IOException e) {
            LOG.error("appendPage PUT rejected by server... cause by %s", e.getMessage());
            throw new PrestoException(HBaseErrorCode.UNEXPECTED_HBASE_ERROR, "Insert into table error", e);
        }

        return NOT_BLOCKED;
    }

    private Put pageToPut(Page page, int position)
    {
        // Set value to the row ID
        if (rowIdOrdinal < 0 || rowIdOrdinal >= page.getChannelCount()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Column mapped as the HBase row ID is illegal");
        }

        Type rowIdType = columns.get(rowIdOrdinal).getType();
        Object rowIdValue = TypeUtils.readNativeValue(rowIdType, page.getBlock(rowIdOrdinal), position);
        if (rowIdValue == null) {
            throw new PrestoException(
                    QUERY_REJECTED,
                    "When inserting data into table, you need to specify the rowKey and at least one another column");
        }

        byte[] rowIdByte = this.serializer.setObjectBytes(rowIdType, rowIdValue);

        Put put = new Put(rowIdByte);

        // For each channel within the page, i.e. column, setting the Put's columns
        for (int channel = 0; channel < page.getChannelCount(); ++channel) {
            // Skip the row ID ordinal
            if (channel == rowIdOrdinal) {
                continue;
            }

            // Get the type for this channel
            Type type = columns.get(channel).getType();
            Object value = TypeUtils.readNativeValue(type, page.getBlock(channel), position);
            // If the value of the field is not null
            if (value != null) {
                byte[] valueBytes = this.serializer.setObjectBytes(type, value);

                // And add the bytes to the Put
                put.addColumn(
                        columns.get(channel).getFamily().get().getBytes(UTF_8),
                        columns.get(channel).getQualifier().get().getBytes(UTF_8),
                        valueBytes);
            }
        }

        if (put.getFamilyCellMap().size() < 1) {
            throw new PrestoException(
                    QUERY_REJECTED,
                    "When inserting data into table, you need to specify the rowKey and at least one another column");
        }

        return put;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        getFutureValue(finish());
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }
}
