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
package io.prestosql.plugin.redis.record;

import com.google.common.collect.ImmutableList;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.plugin.redis.RedisJedisManager;
import io.prestosql.plugin.redis.handle.RedisColumnHandle;
import io.prestosql.plugin.redis.split.RedisSplit;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class RedisRecordSet
        implements RecordSet
{
    private final RedisSplit split;
    private final RedisJedisManager jedisManager;

    private final RowDecoder keyDecoder;
    private final RowDecoder valueDecoder;

    private final List<RedisColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    public RedisRecordSet(
            final RedisSplit redisSplit,
            final RedisJedisManager jedisManager,
            final ImmutableList<RedisColumnHandle> redisColumnHandles,
            final RowDecoder keyDecoder,
            final RowDecoder valueDecoder)
    {
        this.split = requireNonNull(redisSplit, "split is null");
        this.jedisManager = requireNonNull(jedisManager, "jedisManager is null");
        this.keyDecoder = requireNonNull(keyDecoder, "keyDecoder is null");
        this.valueDecoder = requireNonNull(valueDecoder, "valueDecoder is null");
        this.columnHandles = requireNonNull(redisColumnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
        for (DecoderColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }
        this.columnTypes = typeBuilder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new RedisRecordCursor(keyDecoder, valueDecoder, split, columnHandles, jedisManager);
    }
}
