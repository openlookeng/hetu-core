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
package io.hetu.core.plugin.hana;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.hetu.core.plugin.hana.TestHanaSqlUtil.getHandledSql;
import static io.hetu.core.plugin.hana.TestingHanaServer.getActualTable;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class HanaQueryRunner
        extends DistributedQueryRunner
{
    private static final Logger LOG = Logger.get(HanaQueryRunner.class);
    private static final String TPCH = "tpch";
    private static final String HANA = "hana";

    private HanaQueryRunner(Session session, int workers) throws Exception
    {
        super(session, workers);
    }

    public static HanaQueryRunner createHanaQueryRunner(TestingHanaServer server)
            throws Exception
    {
        if (!server.isHanaServerAvailable()) {
            LOG.info("please set correct hana data base info!");
            throw new SkipException("skip the test");
        }
        HanaQueryRunner queryRunner = null;

        try {
            String schema = server.getSchema();
            queryRunner = new HanaQueryRunner(createSession(schema), 3);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog(TPCH, TPCH);

            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("connection-url", server.getJdbcUrl())
                    .put("connection-user", server.getUser())
                    .put("connection-password", server.getPassword())
                    .put("allow-drop-table", "true")
                    .put("hana.table-types", "TABLE,VIEW")
                    .put("hana.schema-pattern", server.getSchema())
                    .put("hana.query.pushdown.enabled", "false")
                    .build();
            queryRunner.installPlugin(new HanaPlugin());
            queryRunner.createCatalog(HANA, HANA, properties);

            if (!server.isTpchLoaded()) {
                copyTpchTables(queryRunner, TPCH, TINY_SCHEMA_NAME, createSession(schema), TpchTable.getTables());
                server.setTpchLoaded();
            }
            else {
                server.waitTpchLoaded();
            }

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session createSession(String schema)
    {
        return testSessionBuilder().setCatalog(HANA).setSchema(schema).build();
    }

    private static void copyTpchTables(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, Session session, Iterable<TpchTable<?>> tables)
    {
        LOG.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH), session);
        }
        LOG.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTable(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, String sourceTable, Session session)
    {
        QualifiedObjectName table = new QualifiedObjectName(sourceCatalog, sourceSchema, sourceTable);
        String tableName = getActualTable(table.getObjectName());

        long start = System.nanoTime();
        LOG.info("Running import for %s", tableName);
        @Language("SQL") String sql = format("CREATE TABLE %s AS SELECT * FROM %s", tableName, table);
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        LOG.info("Executed success for SQL: %s", sql);
        LOG.info("Imported %s rows for %s in %s", rows, tableName, nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    @Override
    public MaterializedResult execute(Session session, @Language("SQL") String sql)
    {
        String newSql = getHandledSql(sql);
        return super.execute(session, newSql);
    }
}
