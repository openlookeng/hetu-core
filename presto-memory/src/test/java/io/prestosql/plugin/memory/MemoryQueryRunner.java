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
package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.testing.mysql.TestingMySqlServer;
import io.airlift.tpch.TpchTable;
import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.metastore.HetuMetastorePlugin;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.tests.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.tests.QueryAssertions.copyTpchTables;

public final class MemoryQueryRunner
{
    private static final String CATALOG = "memory";

    public static final long PROCESSING_DELAY = 1000;

    private MemoryQueryRunner()
    {
    }

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema("default")
                .build();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, 2, extraProperties);

        try {
            queryRunner.installPlugin(new HetuMetastorePlugin());
            TestingMySqlServer mysqlServer = new TestingMySqlServer("test", "mysql", "metastore");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                mysqlServer.close();
            }));
            Map<String, String> metastoreConfig = new HashMap<>();
            metastoreConfig.put("hetu.metastore.type", "jdbc");
            metastoreConfig.put("hetu.metastore.db.url", mysqlServer.getJdbcUrl("metastore"));
            metastoreConfig.put("hetu.metastore.db.user", mysqlServer.getUser());
            metastoreConfig.put("hetu.metastore.db.password", mysqlServer.getPassword());
            queryRunner.registerCloseable(mysqlServer);
            queryRunner.getServers().forEach(server -> {
                try {
                    server.loadMetastore(metastoreConfig);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            TempFolder folder = new TempFolder();
            folder.create();
            Runtime.getRuntime().addShutdownHook(new Thread(folder::close));

            queryRunner.installPlugin(new MemoryPlugin());
            queryRunner.createCatalog(CATALOG, "memory",
                    ImmutableMap.of("memory.max-data-per-node", "1GB",
                            "memory.splits-per-node", "2",
                            "memory.spill-path", folder.newFolder("memory-connector").getAbsolutePath(),
                            "memory.logical-part-processing-delay", PROCESSING_DELAY + "ms",
                            "memory.max-page-size", "50kB"));

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, session, TpchTable.getTables());

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of("http-server.http.port", "8080"));
        Thread.sleep(10);
        Logger log = Logger.get(MemoryQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
