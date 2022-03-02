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
package io.prestosql.tests;

import io.hetu.core.filesystem.HetuFileSystemClientPlugin;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.plugin.tpch.TpchPlugin;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestDistributedSpilledQueries
        extends AbstractTestQueries
{
    public TestDistributedSpilledQueries()
    {
        super(TestDistributedSpilledQueries::createQueryRunner);
    }

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.TASK_CONCURRENCY, "2")
                .setSystemProperty(SystemSessionProperties.SPILL_ENABLED, "true")
                .setSystemProperty(SystemSessionProperties.SPILL_ORDER_BY, "true")
                .setSystemProperty(SystemSessionProperties.AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT, "128kB")
                .build();

        Map<String, String> extraProperties = new HashMap<String, String>();
        extraProperties.put("experimental.spiller-spill-path", Paths.get(System.getProperty("java.io.tmpdir"), "presto", "spills").toString());
        extraProperties.put("experimental.spiller-max-used-space-threshold", "1.0");
        extraProperties.put("experimental.memory-revoking-threshold", "0.0"); // revoke always
        extraProperties.put("experimental.memory-revoking-target", "0.0");
        extraProperties.put("experimental.spiller-spill-to-hdfs", "false");

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(defaultSession, 2, extraProperties);

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.installPlugin(new HetuFileSystemClientPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    @Override
    public void testAssignUniqueId()
    {
        //       due to long running query test created many spill files on disk.
    }
}
