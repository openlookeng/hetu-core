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
import io.prestosql.decoder.DispatchingRowDecoderFactory;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.plugin.redis.RedisJedisManager;
import io.prestosql.plugin.redis.handle.RedisColumnHandle;
import io.prestosql.plugin.redis.split.RedisSplit;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * this class be injected by RedisModule
 */
public class RedisRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final DispatchingRowDecoderFactory decoderFactory;
    private final RedisJedisManager jedisManager;

    @Inject
    public RedisRecordSetProvider(
            final DispatchingRowDecoderFactory decoderFactory,
            final RedisJedisManager jedisManager)
    {
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.jedisManager = requireNonNull(jedisManager, "jedisManager is null");
    }

    public RecordSet getRecordSet(
            final ConnectorTransactionHandle transaction,
            final ConnectorSession session,
            final ConnectorSplit split,
            final ConnectorTableHandle table,
            final List<? extends ColumnHandle> columns)
    {
        RedisSplit redisSplit = (RedisSplit) split;
        ImmutableList<RedisColumnHandle> redisColumnHandles = columns.stream()
                .map(RedisColumnHandle.class::cast)
                .collect(toImmutableList());

        RowDecoder keyDecoder = decoderFactory.create(
                redisSplit.getKeyDataFormat(),
                Collections.emptyMap(),
                redisColumnHandles.stream()
                        .filter(colhandle -> !colhandle.isInternal() && colhandle.isKeyDecoder())
                        .collect(toImmutableSet()));
        RowDecoder valueDecoder = decoderFactory.create(
                redisSplit.getValueDataFormat(),
                Collections.emptyMap(),
                redisColumnHandles.stream()
                        .filter(colhandle -> !colhandle.isInternal() && !colhandle.isKeyDecoder())
                        .collect(toImmutableSet()));
        return new RedisRecordSet(redisSplit, jedisManager, redisColumnHandles, keyDecoder, valueDecoder);
    }
}
