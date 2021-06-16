/*
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

import io.prestosql.plugin.splitmanager.DataSourceTableSplitManager;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class JdbcSplitManager
        implements ConnectorSplitManager
{
    private final JdbcClient jdbcClient;

    private boolean tableSplitEnable;
    private DataSourceTableSplitManager tableSplitManager;

    @Inject
    public JdbcSplitManager(@InternalBaseJdbc JdbcClient jdbcClient, BaseJdbcConfig config, DataSourceTableSplitManager tableSplitManager)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "client is null");
        this.tableSplitEnable = requireNonNull(config, "config is null").getTableSplitEnable();
        this.tableSplitManager = requireNonNull(tableSplitManager, "tableSplitManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        //table split eanble and no pushdown operator
        if (tableSplitEnable && !tableHandle.getGeneratedSql().isPresent()) {
            return tableSplitManager.getSplits(JdbcIdentity.from(session), tableHandle);
        }
        return jdbcClient.getSplits(JdbcIdentity.from(session), tableHandle);
    }
}
