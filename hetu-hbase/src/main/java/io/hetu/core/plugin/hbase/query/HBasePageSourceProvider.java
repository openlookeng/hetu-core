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
package io.hetu.core.plugin.hbase.query;

import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.hetu.core.plugin.hbase.utils.Constants;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * HBasePageSourceProvider
 *
 * @since 2020-03-18
 */
public class HBasePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private HBaseRecordSetProvider recordSetProvider;
    private HBaseConnection hbaseConnection;

    @Inject
    public HBasePageSourceProvider(HBaseRecordSetProvider recordSetProvider, HBaseConnection hbaseConnection)
    {
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.hbaseConnection = hbaseConnection;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns)
    {
        // if delete rows, we should replace $rowId -> real rowkey name
        List<ColumnHandle> columnsReplaceRowKey = new ArrayList<>();
        columns.forEach(ch -> {
            if (Constants.HBASE_ROWID_NAME.equals(ch.getColumnName())) {
                if (ch instanceof HBaseColumnHandle && table instanceof HBaseTableHandle) {
                    HBaseColumnHandle rowColumnHandle = (HBaseColumnHandle) ch;
                    columnsReplaceRowKey.add(new HBaseColumnHandle(
                            ((HBaseTableHandle) table).getRowId(),
                            rowColumnHandle.getFamily(),
                            rowColumnHandle.getQualifier(),
                            rowColumnHandle.getType(),
                            rowColumnHandle.getOrdinal(),
                            rowColumnHandle.getComment(),
                            rowColumnHandle.isIndexed()));
                }
            }
            else {
                columnsReplaceRowKey.add(ch);
            }
        });

        RecordSet recordSet = recordSetProvider.getRecordSet(transactionHandle, session, split, table, columnsReplaceRowKey);
        HBaseRecordSet hbaseRecordSet = null;
        if (recordSet instanceof HBaseRecordSet) {
            hbaseRecordSet = (HBaseRecordSet) recordSet;
        }
        if (columnsReplaceRowKey.stream()
                .anyMatch(
                        ch -> (ch instanceof HBaseColumnHandle)
                                && (table instanceof HBaseTableHandle)
                                && ((HBaseColumnHandle) ch).getOrdinal()
                                == ((HBaseTableHandle) table).getRowIdOrdinal())) {
            return new HBaseUpdatablePageSource(hbaseRecordSet, hbaseConnection);
        }
        else {
            return new RecordPageSource(recordSet);
        }
    }
}
