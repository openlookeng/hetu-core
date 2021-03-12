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
package io.hetu.core.heuristicindex;

import com.google.common.io.Files;
import io.airlift.testing.mysql.TestingMySqlServer;
import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.common.util.SslSocketUtil;
import io.hetu.core.filesystem.HetuFileSystemClientPlugin;
import io.hetu.core.metastore.HetuMetastorePlugin;
import io.prestosql.Session;
import io.prestosql.plugin.hive.HiveHadoop2Plugin;
import io.prestosql.plugin.hive.HivePlugin;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.tests.DistributedQueryRunner;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public final class HindexQueryRunner
{
    private HindexQueryRunner() {}

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        TempFolder folder = new TempFolder();
        TestingMySqlServer mysqlServer = new TestingMySqlServer("test", "mysql", "metastore1");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            folder.close();
            mysqlServer.close();
        }));

        DistributedQueryRunner queryRunner; // Use this to return if no exceptions

        // Try to get a free port and start up the host
        for (int i = 0; i < 10; i++) {
            int port = -1;
            try {
                port = SslSocketUtil.getAvailablePort();
                folder.create();
                Map<String, String> configs = new HashMap<>();
                configs.put("http-server.http.port", Integer.toString(port));
                configs.put("hetu.heuristicindex.filter.enabled", "true");
                configs.put("hetu.heuristicindex.filter.cache.max-memory", "1GB");
                configs.put("hetu.heuristicindex.filter.cache.loading-delay", "100ms");
                configs.put("hetu.heuristicindex.indexstore.uri", folder.getRoot().getAbsolutePath());
                configs.put("hetu.heuristicindex.indexstore.filesystem.profile", "__test__hdfs__");
                Map<String, String> metastoreConfig = new HashMap<>();
                metastoreConfig.put("hetu.metastore.type", "jdbc");
                metastoreConfig.put("hetu.metastore.db.url", mysqlServer.getJdbcUrl("metastore1"));
                metastoreConfig.put("hetu.metastore.db.user", mysqlServer.getUser());
                metastoreConfig.put("hetu.metastore.db.password", mysqlServer.getPassword());

                queryRunner = createQueryRunner(configs, metastoreConfig, Collections.emptyMap());
                queryRunner.registerCloseable(mysqlServer);
                return queryRunner;
            }
            catch (Exception portException) {
                System.out.printf("Failed to start testing server with port %d. Retrying.%n", port);
            }
        }
        throw new Exception("Reached maximum attempt to start testing server.");
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties,
            Map<String, String> metastoreProperties,
            Map<String, String> coordinatorProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setSource("test")
                .setCatalog("hive")
                .setSchema("test")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .setExtraProperties(extraProperties)
                .setCoordinatorProperties(coordinatorProperties)
                .build();

        try {
            File tempDir = Files.createTempDir();
            File hiveDir = new File(tempDir, "hive_data");
            HiveMetastore metastore = createTestingFileHiveMetastore(hiveDir);
            HiveIdentity identity = new HiveIdentity(SESSION);
            metastore.createDatabase(identity,
                    Database.builder()
                            .setDatabaseName("test")
                            .setOwnerName("public")
                            .setOwnerType(PrincipalType.ROLE)
                            .build());

            queryRunner.installPlugin(new HetuFileSystemClientPlugin());
            queryRunner.installPlugin(new HetuMetastorePlugin());
            queryRunner.installPlugin(new HiveHadoop2Plugin());
            queryRunner.installPlugin(new HeuristicIndexPlugin());
            queryRunner.installPlugin(new HivePlugin("Hive", Optional.of(metastore)));
            queryRunner.getServers().forEach(server -> {
                try {
                    server.loadMetastore(metastoreProperties);
                    server.getHeuristicIndexerManager().buildIndexClient();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            queryRunner.createCatalog("hive", "Hive");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        createQueryRunner();
    }
}
