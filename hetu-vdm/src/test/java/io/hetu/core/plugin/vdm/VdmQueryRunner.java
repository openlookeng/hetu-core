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
package io.hetu.core.plugin.vdm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.mysql.TestingMySqlServer;
import io.airlift.tpch.TpchTable;
import io.hetu.core.metastore.HetuMetastorePlugin;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public final class VdmQueryRunner
{
    protected static final SqlParserOptions DEFAULT_SQL_PARSER_OPTIONS = new SqlParserOptions();

    private VdmQueryRunner()
    {
    }

    public static QueryRunner createVdmQueryRunner(TestingMySqlServer server, TpchTable<?>... tables)
            throws Exception
    {
        return createVdmQueryRunner(server.getJdbcUrl("vdm"), ImmutableMap.of(), ImmutableList.copyOf(tables));
    }

    public static QueryRunner createVdmQueryRunner(TestingMySqlServer server, Map<String, String> connectorProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        try {
            return createVdmQueryRunner(server.getJdbcUrl("vdm"), connectorProperties, tables);
        }
        catch (Throwable e) {
            closeAllSuppress(e, server);
            throw e;
        }
    }

    public static QueryRunner createVdmQueryRunner(String jdbcUrl, Map<String, String> connectorProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            File directory = new File("");
            String courseFile = directory.getCanonicalPath();
            System.setProperty("config", courseFile + "/etc/");
            String configDir = System.getProperty("config");
            String hetumetastoreConfig = configDir + "hetu-metastore.properties";
            File file = new File(configDir);
            if (!file.exists()) {
                file.mkdirs();
            }
            File file2 = new File(configDir, "hetu-metastore.properties");
            if (!file2.exists()) {
                try {
                    file2.createNewFile();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(hetumetastoreConfig))) {
                bufferedWriter.write("hetu.metastore.db.url = " + jdbcUrl);
                bufferedWriter.write("\n");
                bufferedWriter.write("hetu.metastore.type = jdbc\n");
                bufferedWriter.write("hetu.metastore.db.user = user\n");
                bufferedWriter.write("hetu.metastore.db.password = testpass\n");
            }
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setNodeCount(1)
                    .setExtraProperties(ImmutableMap.of())
                    .setCoordinatorProperties(ImmutableMap.of())
                    .setParserOptions(DEFAULT_SQL_PARSER_OPTIONS)
                    .setEnvironment("vdmtest")
                    .setBaseDataDir(Optional.empty())
                    .build();
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.installPlugin(new VdmPlugin());
            queryRunner.installPlugin(new HetuMetastorePlugin());
            queryRunner.getCoordinator().loadMetastore();

            queryRunner.createCatalog("tpch", "tpch");
            queryRunner.createCatalog("vdm", "vdm", ImmutableMap.of("vdm.metadata-cache-enabled", "false"));
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("vdm")
                .build();
    }
}
