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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class JdbcPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private JdbcRecordSetProvider recordSetProvider;
    private final JdbcClient jdbcClient;
    private final BaseJdbcConfig config;

    @Inject
    public JdbcPageSourceProvider(@InternalBaseJdbc JdbcClient jdbcClient, BaseJdbcConfig config, JdbcRecordSetProvider recordSetProvider)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.config = requireNonNull(config, "config is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns)
    {
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) table;
        RecordSet recordSet = recordSetProvider.getRecordSet(transaction, session, split, table, columns);
        if (jdbcTableHandle.getDeleteOrUpdate()) {
            return new JdbcUpdatablePageSource(recordSet, session, table, jdbcClient, config, (JdbcSplit) split);
        }
        else {
            return new RecordPageSource(recordSet);
        }
    }
}
