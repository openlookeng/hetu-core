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
package io.prestosql.plugin.redis.decoder;

import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.decoder.RowDecoderFactory;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class HashRedisRowDecoderFactory
        implements RowDecoderFactory
{
    @Override
    public RowDecoder create(final Map<String, String> decoderParams, final Set<DecoderColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");
        return new HashRedisRowDecoder(chooseFieldDecoders(columns));
    }

    private Map<DecoderColumnHandle, RedisFieldDecoder<String>> chooseFieldDecoders(final Set<DecoderColumnHandle> columns)
    {
        return columns.stream()
                .collect(toImmutableMap(identity(), this::chooseFieldDecoder));
    }

    private RedisFieldDecoder<String> chooseFieldDecoder(final DecoderColumnHandle column)
    {
        checkArgument(!column.isInternal(), "unexpected internal column '%s'", column.getName());
        if (column.getDataFormat() == null) {
            return new HashRedisFieldDecoder();
        }
        if (column.getType().getJavaType() == long.class && "iso8601".equals(column.getDataFormat())) {
            return new ISO8601HashRedisFieldDecoder();
        }
        throw new IllegalArgumentException(format("unknown data format '%s' for column '%s'", column.getDataFormat(), column.getName()));
    }
}
