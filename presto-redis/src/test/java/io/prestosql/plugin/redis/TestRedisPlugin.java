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

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.redis.description.RedisTableDescription;
import io.prestosql.plugin.redis.util.RedisServer;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.testing.QueryRunner;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TestRedisPlugin
{
    private TestRedisPlugin(){}

    public static void installRedisPlugin(
            final RedisServer redisServer,
            final QueryRunner queryRunner,
            final Map<SchemaTableName, RedisTableDescription> tableDescriptionMap)
    {
        Map<SchemaTableName, RedisTableDescription> description = ImmutableMap.copyOf(requireNonNull(tableDescriptionMap, "tableDescription is null"));
        RedisPlugin redisPlugin = new RedisPlugin();
        Supplier<Map<SchemaTableName, RedisTableDescription>> mapSupplier = () -> description;
        redisPlugin.setTableDescriptionSupplier(mapSupplier);
        queryRunner.installPlugin(redisPlugin);
        Map<String, String> redisConfig = ImmutableMap.of(
                "redis.nodes", redisServer.getHostAndPort().toString(),
                "redis.table-names", Joiner.on(",").join(description.keySet()),
                "redis.default-schema", "default",
                "redis.hide-internal-columns", "true",
                "redis.key-prefix-schema-table", "true");

        queryRunner.createCatalog("redis", "redis", redisConfig);
    }
}
