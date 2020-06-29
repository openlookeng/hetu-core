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

package io.hetu.core.plugin.datacenter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

final class DataCenterQueryRunner
{
    private DataCenterQueryRunner()
    {
        // Utility class
    }

    static QueryRunner createDCQueryRunner(TestingPrestoServer server, TpchTable<?>... tables)
            throws Exception
    {
        return createDCQueryRunner(server, ImmutableMap.of(), ImmutableList.copyOf(tables));
    }

    static QueryRunner createDCQueryRunner(TestingPrestoServer hetuServer, Map<String, String> properties,
                                           Iterable<TpchTable<?>> tables)
            throws Exception
    {
        hetuServer.installPlugin(new TpchPlugin());
        hetuServer.createCatalog("tpch", "tpch");

        DistributedQueryRunner queryRunner = null;
        try {
            DistributedQueryRunner.Builder builder = DistributedQueryRunner.builder(createSession());
            queryRunner = builder.build();

            Map<String, String> connectorProperties = new HashMap<>(properties);
            connectorProperties.putIfAbsent("connection-url", hetuServer.getBaseUrl().toString());
            connectorProperties.putIfAbsent("connection-user", "root");
            queryRunner.installPlugin(new DataCenterPlugin());
            queryRunner.createDCCatalog("dc", "dc", connectorProperties);
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", properties);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session createSession()
    {
        return testSessionBuilder().setCatalog("dc.tpch").setSchema("tiny").build();
    }
}
