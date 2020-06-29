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
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

/**
 * HBasePageSinkProvider
 *
 * @since 2020-03-18
 */
public class HBasePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final HBaseConnection hbaseConn;

    /**
     * constructor
     *
     * @param hbaseConn hbaseConn
     */
    @Inject
    public HBasePageSinkProvider(HBaseConnection hbaseConn)
    {
        this.hbaseConn = hbaseConn;
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle)
    {
        HBaseTableHandle tableHandle = (HBaseTableHandle) outputTableHandle;
        return new HBasePageSink(hbaseConn, tableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle)
    {
        return createPageSink(transactionHandle, session, (ConnectorOutputTableHandle) insertTableHandle);
    }
}
