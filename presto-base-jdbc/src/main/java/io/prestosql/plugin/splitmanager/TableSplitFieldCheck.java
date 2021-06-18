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
package io.prestosql.plugin.splitmanager;

import io.prestosql.plugin.jdbc.ForwardingJdbcClient;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class TableSplitFieldCheck
        extends ForwardingJdbcClient
{
    private final JdbcClient delegate;

    private final DataSourceTableSplitManager tableSplitManager;

    private static final List<Integer> SPLITFIELD_SUPPORT_JDBC_TYPE = Arrays.asList(Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT);

    public TableSplitFieldCheck(JdbcClient delegate, DataSourceTableSplitManager tableSplitManager)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.tableSplitManager = requireNonNull(tableSplitManager, "tableSplitManager is null");
    }

    @Override
    protected JdbcClient getDelegate()
    {
        return delegate;
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        Optional<JdbcTableHandle> tableHandle = getDelegate().getTableHandle(identity, schemaTableName);
        if (tableHandle.isPresent()) {
            TableSplitConfig config = tableSplitManager.getTableSplitConfig(tableHandle.get());
            if (null != config) {
                tableHandle.get().setTableSplitField(config.getSplitField());
            }
        }
        return tableHandle;
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        List<JdbcColumnHandle> columnHandleList = getDelegate().getColumns(session, tableHandle);
        String tableSplitField = tableHandle.getTableSplitField();
        if (!isNullOrEmpty(tableSplitField)) {
            long matchCoutn = columnHandleList.stream().filter(columnHandle ->
            {
                int jdbcType = columnHandle.getJdbcTypeHandle().getJdbcType();
                if (!SPLITFIELD_SUPPORT_JDBC_TYPE.contains(jdbcType)) {
                    return false;
                }
                return columnHandle.getColumnName().equals(tableSplitField);
            }).count();

            if (1 == matchCoutn) {
                tableHandle.setTableSplitFieldValidated(true);
            }
        }
        return columnHandleList;
    }
}
