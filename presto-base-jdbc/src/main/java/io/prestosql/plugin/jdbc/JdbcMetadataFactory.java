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
import io.prestosql.plugin.splitmanager.TableSplitFieldCheck;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class JdbcMetadataFactory
{
    private final JdbcClient jdbcClient;
    private final boolean allowDropTable;
    private final DataSourceTableSplitManager tableSplitManager;

    @Inject
    public JdbcMetadataFactory(@InternalBaseJdbc JdbcClient jdbcClient, JdbcMetadataConfig config, DataSourceTableSplitManager tableSplitManager)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.tableSplitManager = requireNonNull(tableSplitManager, "tableSplitManager is null");
        requireNonNull(config, "config is null");
        this.allowDropTable = config.isAllowDropTable();
    }

    /**
     * Hetu's sub-query push down optimizer needs to access the JDBC client via {@link JdbcConnector}.
     * This getter method is introduced to provide the access to jdbc client.
     *
     * @return the jdbc client
     */
    public JdbcClient getJdbcClient()
    {
        return jdbcClient;
    }

    public JdbcMetadata create()
    {
        return new JdbcMetadata(new TableSplitFieldCheck(new TransactionScopeCachingJdbcClient(jdbcClient), tableSplitManager), allowDropTable);
    }
}
