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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.hetu.core.plugin.hbase.split.HBaseSplit;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * HBaseRecordSetProvider
 *
 * @since 2020-03-18
 */
public class HBaseRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger LOG = Logger.get(HBaseRecordSetProvider.class);

    private final HBaseConnection hbaseConn;

    /**
     * constructor
     *
     * @param hbaseConn hbaseConn
     */
    @Inject
    public HBaseRecordSetProvider(HBaseConnection hbaseConn)
    {
        this.hbaseConn = hbaseConn;
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        List<HBaseColumnHandle> accColumns =
                columns.stream().map(HBaseColumnHandle.class::cast).collect(toImmutableList());
        return new HBaseRecordSet(hbaseConn, session, (HBaseSplit) split, (HBaseTableHandle) table, accColumns);
    }
}
