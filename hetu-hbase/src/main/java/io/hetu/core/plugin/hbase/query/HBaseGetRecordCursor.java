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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.split.HBaseSplit;
import io.hetu.core.plugin.hbase.utils.HBaseErrorCode;
import io.hetu.core.plugin.hbase.utils.serializers.HBaseRowSerializer;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * HBaseGetRecordCursor
 *
 * @since 2020-03-18
 */
public class HBaseGetRecordCursor
        extends HBaseRecordCursor
{
    private static final Logger LOG = Logger.get(HBaseGetRecordCursor.class);

    private Connection conn;

    private Result[] results;

    private int currentRecordIndex;

    public HBaseGetRecordCursor(
            List<HBaseColumnHandle> columnHandles,
            HBaseSplit hBaseSplit,
            Connection connection,
            HBaseRowSerializer serializer,
            List<Type> columnTypes,
            String rowIdName,
            String[] fieldToColumnName,
            String defaultValue)
    {
        super(columnHandles, columnTypes, serializer, rowIdName, fieldToColumnName, defaultValue);
        startTime = System.currentTimeMillis();
        this.columnHandles = columnHandles;

        this.rowIdName =
                requireNonNull(hBaseSplit.getRowKeyName(), "RowKeyName cannot be null if you want to query by RowKey");

        this.split = hBaseSplit;
        this.conn = connection;
        try (Table table =
                connection.getTable(TableName.valueOf(hBaseSplit.getTableHandle().getHbaseTableName().get()))) {
            List<String> rowKeys = new ArrayList<>();

            if (hBaseSplit.getRanges().containsKey(hBaseSplit.getTableHandle().getRowIdOrdinal())) {
                for (Range range : hBaseSplit.getRanges().get(hBaseSplit.getTableHandle().getRowIdOrdinal())) {
                    Object object = range.getSingleValue();
                    if (object instanceof Slice) {
                        rowKeys.add(((Slice) object).toStringUtf8());
                    }
                    else {
                        rowKeys.add(range.getSingleValue().toString());
                    }
                }
            }

            this.results = getResults(rowKeys, table);
        }
        catch (IOException e) {
            LOG.error(e, e.getMessage());
            this.close();
        }
        this.bytesRead = 0L;
    }

    private Result[] getResults(List<String> rowKeys, Table table)
    {
        List<Get> gets =
                rowKeys.stream()
                        .map(
                                rowKey -> {
                                    Get get = new Get(Bytes.toBytes(rowKey));
                                    for (ColumnHandle ch : columnHandles) {
                                        if (ch instanceof HBaseColumnHandle) {
                                            HBaseColumnHandle hch = (HBaseColumnHandle) ch;
                                            if (!this.rowIdName.equals(hch.getColumnName())) {
                                                get.addColumn(
                                                        Bytes.toBytes(hch.getFamily().get()),
                                                        Bytes.toBytes(hch.getQualifier().get()));
                                            }
                                        }
                                    }
                                    return get;
                                })
                        .collect(Collectors.toList());

        try {
            return table.get(gets);
        }
        catch (IOException e) {
            LOG.error(e.getMessage(), e);
            return new Result[0];
        }
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            if (this.currentRecordIndex >= this.results.length) {
                return false;
            }
            else {
                Result record = this.results[this.currentRecordIndex];
                serializer.reset();
                if (record.getRow() != null) {
                    serializer.deserialize(record, this.defaultValue);
                }
                this.currentRecordIndex++;
                return true;
            }
        }
        catch (Exception e) {
            this.close();
            throw new PrestoException(HBaseErrorCode.IO_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        try (Connection connection = this.conn) {
            if (connection != null) {
                connection.close();
            }
        }
        catch (IOException e) {
            // ignore exception from close
        }
    }
}
