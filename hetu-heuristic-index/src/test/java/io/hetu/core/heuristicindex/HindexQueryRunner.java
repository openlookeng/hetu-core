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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.filesystem.HetuFileSystemClientPlugin;
import io.prestosql.Session;
import io.prestosql.plugin.hive.HiveHadoop2Plugin;
import io.prestosql.plugin.hive.HivePlugin;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.tests.DistributedQueryRunner;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
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
        try (TempFolder folder = new TempFolder()) {
            DistributedQueryRunner queryRunner = null; // Use this to return if no exceptions

            // Try to get a free port and start up the host
            for (int port = 8080; port <= 65535; port++) {
                // Use starting host at port 8080, max 2^16-1
                try {
                    folder.create();
                    Map<String, String> queryRunnerMap = new HashMap<>();

                    queryRunnerMap.put("http-server.http.port", Integer.toString(port));
                    queryRunnerMap.put("hetu.heuristicindex.filter.enabled", "true");
                    queryRunnerMap.put("hetu.heuristicindex.filter.cache.max-memory", "1GB");
                    queryRunnerMap.put("hetu.heuristicindex.filter.cache.loading-delay", "100ms");
                    queryRunnerMap.put("hetu.heuristicindex.indexstore.uri", folder.getRoot().getAbsolutePath().toString());
                    queryRunnerMap.put("hetu.heuristicindex.indexstore.filesystem.profile", "default");
                    queryRunner = createQueryRunner(queryRunnerMap);
                    break;
                }
                catch (Exception portException) {
                    if (port >= 65535) {
                        // No free ports
                        throw new Exception("No more free ports for hosting server.");
                    }
                    continue;
                }
            }
            return queryRunner;
        }
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        return createQueryRunner(extraProperties, ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties,
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
            queryRunner.installPlugin(new HiveHadoop2Plugin());
            queryRunner.installPlugin(new HeuristicIndexPlugin());
            queryRunner.installPlugin(new HivePlugin("Hive", Optional.of(metastore)));
            queryRunner.getServers().forEach(server -> {
                try {
                    server.getHeuristicIndexerManager().buildIndexClient();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
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
