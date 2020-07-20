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

package io.hetu.core.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.tests.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.tests.QueryAssertions.copyTpchTables;

/**
 * OracleQueryRunner
 *
 * @since 2019-08-28
 */
public class OracleQueryRunner
{
    private static final String CATALOG = "oracle";

    private static final String TPCH_SCHEMA = "test";

    private static final String TPCH_CATALOG = "tpch";

    private OracleQueryRunner()
    { }

    /**
     * createOracleQueryRunner
     *
     * @param oracleServer oracleServer
     * @param tables tables
     * @return DistributedQueryRunner
     * @throws Exception Exception
     */
    public static DistributedQueryRunner createOracleQueryRunner(TestingOracleServer oracleServer,
            TpchTable<?>... tables)
            throws Exception
    {
        return createOracleQueryRunner(oracleServer, ImmutableMap.of(), ImmutableList.copyOf(tables));
    }

    /**
     * createOracleQueryRunner
     *
     * @param testingOracleServer testingOracleServer
     * @param connectorProperties connectorProperties
     * @param tables tables
     * @return DistributedQueryRunner
     * @throws Exception Exception
     */
    public static DistributedQueryRunner createOracleQueryRunner(TestingOracleServer testingOracleServer,
            Map<String, String> connectorProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession()).build();
        try {
            queryRunner = new DistributedQueryRunner(createSession(), 3);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog(TPCH_CATALOG, TPCH_CATALOG);
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", testingOracleServer.getJdbcUrl());
            connectorProperties.putIfAbsent("connection-user", testingOracleServer.getUser());
            connectorProperties.putIfAbsent("connection-password", testingOracleServer.getPassWd());
            connectorProperties.putIfAbsent("allow-drop-table", "true");

            queryRunner.installPlugin(new OraclePlugin());
            queryRunner.createCatalog(CATALOG, "oracle", connectorProperties);

            copyTpchTables(queryRunner, TPCH_CATALOG, TINY_SCHEMA_NAME, createSession(), tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    /**
     * createSession
     *
     * @return Session
     */
    public static Session createSession()
    {
        return testSessionBuilder().setCatalog(CATALOG).setSchema(TPCH_SCHEMA).build();
    }
}
