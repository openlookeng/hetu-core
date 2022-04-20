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
package io.prestosql.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.tpch.TpchTable;
import io.prestosql.plugin.redis.description.RedisTableDescription;
import io.prestosql.plugin.redis.util.RedisServer;
import io.prestosql.plugin.redis.util.TestUtils;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.tests.DistributedQueryRunner;
import redis.clients.jedis.HostAndPort;

import java.util.Map;
import java.util.Optional;

import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class RedisQueryRunner
{
    private RedisQueryRunner(){}

    private static final Logger LOG = Logger.get(RedisQueryRunner.class);

    public static void main(final String[] args) throws InterruptedException
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createRedisQueryRunner(
                new RedisServer(new HostAndPort("localhost", 6379)),
                "string",
                TpchTable.getTables());

        Thread.sleep(10);
        LOG.info("======== SERVER STARTED ========");
        LOG.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    private static DistributedQueryRunner createRedisQueryRunner(
            final RedisServer redisServer,
            final String dataformat,
            final Iterable<TpchTable<?>> tables)
    {
        try {
            DistributedQueryRunner queryRunner = new DistributedQueryRunner(
                    testSessionBuilder().setCatalog("redis").setSchema("tpch").build(),
                    1,
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    new SqlParserOptions(),
                    "testing",
                    Optional.empty());

            LOG.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
            Map<SchemaTableName, RedisTableDescription> tpchTableDescriptions = TestTableDescription.createTpchTableDescriptions(queryRunner.getMetadata(), tables, dataformat);

            TestRedisPlugin.installRedisPlugin(redisServer, queryRunner, tpchTableDescriptions);
            System.out.println("loading data ....");
            for (TpchTable<?> tpchTable : tables) {
                TestUtils.writeToRedis(queryRunner, redisServer, tpchTable, dataformat);
            }
            return queryRunner;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
