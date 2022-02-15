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
package io.hetu.core.plugin.greenplum;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.hetu.core.plugin.greenplum.GreenPlumConstants.GREENPLUM_CONNECTOR_NAME;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.tests.QueryAssertions.copyTpchTables;

public final class GreenPlumQueryRunner
{
    private GreenPlumQueryRunner()
    {
    }

    private static final String TPCH_SCHEMA = "tpch";

    public static QueryRunner createGreenPlumQueryRunner(TestingPostgreSqlServer server, TpchTable<?>... tables)
            throws Exception
    {
        return createGreenPlumQueryRunner(server, ImmutableMap.of(), ImmutableList.copyOf(tables));
    }

    public static QueryRunner createGreenPlumQueryRunner(TestingPostgreSqlServer server, Map<String, String> connectorProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = new DistributedQueryRunner(createSession(), 3);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Map<String, String> connectorPropertiesMap = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorPropertiesMap.putIfAbsent("connection-url", server.getJdbcUrl());
            connectorPropertiesMap.putIfAbsent("allow-drop-table", "true");
            connectorPropertiesMap.putIfAbsent("jdbc.pushdown-enabled", "false");

            createSchema(server.getJdbcUrl(), "tpch");

            queryRunner.installPlugin(new GreenPlumSqlPlugin());
            queryRunner.createCatalog(GREENPLUM_CONNECTOR_NAME, GREENPLUM_CONNECTOR_NAME, connectorPropertiesMap);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, server);
            throw e;
        }
    }

    private static void createSchema(String url, String schema)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(url);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(GREENPLUM_CONNECTOR_NAME)
                .setSchema(TPCH_SCHEMA)
                .build();
    }
}
