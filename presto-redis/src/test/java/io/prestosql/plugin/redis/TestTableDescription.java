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
import io.airlift.json.JsonCodec;
import io.airlift.tpch.TpchTable;
import io.prestosql.metadata.Metadata;
import io.prestosql.plugin.redis.description.RedisTableDescription;
import io.prestosql.plugin.redis.util.CodecSupplier;
import io.prestosql.plugin.redis.util.TestUtils;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.Map;

public class TestTableDescription
{
    private TestTableDescription(){}

    protected static Map<SchemaTableName, RedisTableDescription> createTpchTableDescriptions(
            final Metadata metadata,
            final Iterable<TpchTable<?>> tables,
            final String dataFormat)
    {
        JsonCodec<RedisTableDescription> redisTableDescriptionJsonCodec = new CodecSupplier<>(metadata, RedisTableDescription.class).get();
        ImmutableMap.Builder<SchemaTableName, RedisTableDescription> builder = ImmutableMap.builder();
        for (TpchTable<?> table : tables) {
            SchemaTableName tableName = new SchemaTableName("tpch", table.getTableName());
            Map.Entry<SchemaTableName, RedisTableDescription> redisTableDescriptionEntry = TestUtils.loadTpchTableDescription(redisTableDescriptionJsonCodec, tableName, dataFormat);

            builder.put(redisTableDescriptionEntry);
        }
        return builder.build();
    }
}
