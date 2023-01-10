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

package io.hetu.core.plugin.singledata;

import io.hetu.core.plugin.singledata.tidrange.TidRangeClient;
import io.hetu.core.plugin.singledata.tidrange.TidRangePageSource;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcRecordSetProvider;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SingleDataPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ConnectorRecordSetProvider recordSetProvider;
    private final SingleDataModeConfig.SingleDataMode mode;
    private final JdbcClient client;

    @Inject
    public SingleDataPageSourceProvider(JdbcRecordSetProvider recordSetProvider, SingleDataModeConfig modeConfig, JdbcClient client)
    {
        this.client = requireNonNull(client, "client is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "RecordSetProvider is null");
        this.mode = modeConfig.getSingleDataMode();
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle tableHandle, List<ColumnHandle> columns)
    {
        if (mode == SingleDataModeConfig.SingleDataMode.TID_RANGE) {
            return new TidRangePageSource((TidRangeClient) client, session, split, columns);
        }
        else {
            RecordSet recordSet = recordSetProvider.getRecordSet(transaction, session, split, tableHandle, columns);
            return new RecordPageSource(recordSet);
        }
    }
}
