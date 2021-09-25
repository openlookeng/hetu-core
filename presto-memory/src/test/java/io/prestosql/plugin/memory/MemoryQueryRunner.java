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
import io.airlift.tpch.TpchTable;
import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.filesystem.HetuFileSystemClientPlugin;
import io.hetu.core.metastore.HetuMetastorePlugin;
import io.prestosql.Session;
import io.prestosql.plugin.tpcds.TpcdsPlugin;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.tests.DistributedQueryRunner;

import java.io.IOException;
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

    private static final Map<String, String> DEFAULT_MEMORY_PROPERTIES = ImmutableMap.of("memory.max-data-per-node", "1GB",
            "memory.logical-part-processing-delay", PROCESSING_DELAY + "ms",
            "memory.max-page-size", "50kB");

    private static final String FOLDER_PROPERTY_KEY = "memory.spill-path";

    private static Map<String, String> createConfig(TempFolder folder, Map<String, String> newConfigs)
            throws IOException
    {
        Map<String, String> memoryProperties = new HashMap(DEFAULT_MEMORY_PROPERTIES);

        memoryProperties.put(FOLDER_PROPERTY_KEY, folder.newFolder("memory-connector").getAbsolutePath());
        for (Map.Entry<String, String> entry : newConfigs.entrySet()) {
            memoryProperties.put(entry.getKey(), entry.getValue());
        }

        return memoryProperties;
    }

    private MemoryQueryRunner()
    {
    }

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(2, ImmutableMap.of(), ImmutableMap.of(), true);
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        return createQueryRunner(2, extraProperties, ImmutableMap.of(), true);
    }

    public static DistributedQueryRunner createQueryRunner(int nodes, Map<String, String> extraProperties, Map<String, String> memoryProperties, Boolean createTpchTables)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema("default")
                .build();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, nodes, extraProperties);
        Runtime.getRuntime().addShutdownHook(new Thread(queryRunner::close));

        try {
            queryRunner.installPlugin(new HetuFileSystemClientPlugin());
            queryRunner.installPlugin(new HetuMetastorePlugin());
            TempFolder metastoreFolder = new TempFolder().create();
            Runtime.getRuntime().addShutdownHook(new Thread(metastoreFolder::close));

            Map<String, String> metastoreConfig = new HashMap<>();
            metastoreConfig.put("hetu.metastore.type", "hetufilesystem");
            metastoreConfig.put("hetu.metastore.hetufilesystem.profile-name", "default");
            metastoreConfig.put("hetu.metastore.hetufilesystem.path", metastoreFolder.newFolder("metastore").getAbsolutePath());

            queryRunner.getServers().forEach(server -> {
                try {
                    server.loadMetastore(new HashMap<>(metastoreConfig));
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            queryRunner.installPlugin(new MemoryPlugin());

            queryRunner.getServers().forEach(server -> {
                try {
                    TempFolder folder = new TempFolder();
                    folder.create();
                    Runtime.getRuntime().addShutdownHook(new Thread(folder::close));

                    //create memory catalog with custom memory properties
                    server.createCatalog(CATALOG, "memory", createConfig(folder, memoryProperties));
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
            queryRunner.installPlugin(new TpcdsPlugin());
            queryRunner.createCatalog("tpcds", "tpcds", ImmutableMap.of());

            if (createTpchTables) {
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, session, TpchTable.getTables());
            }

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
        DistributedQueryRunner queryRunner = createQueryRunner(2, ImmutableMap.of(), ImmutableMap.of("http-server.http.port", "8080"), true);
        Thread.sleep(10);
        Logger log = Logger.get(MemoryQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
