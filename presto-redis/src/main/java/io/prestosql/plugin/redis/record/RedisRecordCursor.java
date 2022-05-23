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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.plugin.redis.RedisJedisManager;
import io.prestosql.plugin.redis.description.RedisInternalFieldDescription;
import io.prestosql.plugin.redis.handle.RedisColumnHandle;
import io.prestosql.plugin.redis.split.RedisSplit;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.Type;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.decoder.FieldValueProviders.booleanValueProvider;
import static io.prestosql.decoder.FieldValueProviders.bytesValueProvider;
import static io.prestosql.decoder.FieldValueProviders.longValueProvider;
import static java.lang.String.format;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

public class RedisRecordCursor
        implements RecordCursor
{
    private static final Logger LOG = Logger.get(RedisRecordCursor.class);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final RowDecoder keyDecoder;
    private final RowDecoder valueDecoder;

    private final RedisSplit split;
    private final List<RedisColumnHandle> columnHandles;

    private final JedisPool jedisPool;

    private ScanResult<String> redisCursor;
    private final ScanParams scanParams;
    private Iterator<String> keysIterator;
    private final String keyDataFormat;
    private final String valueDataFormat;

    private final AtomicBoolean reported = new AtomicBoolean();

    private String valueString;
    private Map<String, String> valueMap;

    private long totalBytes;
    private long totalValues;

    private final FieldValueProvider[] currentRowValues;

    public RedisRecordCursor(
            final RowDecoder keyDecoder,
            final RowDecoder valueDecoder,
            final RedisSplit split,
            final List<RedisColumnHandle> columnHandles,
            final RedisJedisManager jedisManager)
    {
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.split = split;
        this.columnHandles = columnHandles;
        this.jedisPool = jedisManager.getJedisPool(split.getNodes().get(0));
        this.currentRowValues = new FieldValueProvider[columnHandles.size()];
        this.keyDataFormat = split.getKeyDataFormat();
        this.valueDataFormat = split.getValueDataFormat();
        if (!keyDataFormat.equals("zset")) {
            this.scanParams = new ScanParams();
            scanParams.count(jedisManager.getRedisConfig().getRedisScanCount());
            if (jedisManager.getRedisConfig().isKeyPrefixSchemaTable()) {
                String keyMatch = "";
                if (!split.getSchemaName().equals("default")) {
                    keyMatch = split.getSchemaName() + jedisManager.getRedisConfig().getRedisKeyDelimiter();
                }
                keyMatch = keyMatch + split.getTableName() + jedisManager.getRedisConfig().getRedisKeyDelimiter() + '*';
                scanParams.match(keyMatch);
            }
        }
        else {
            this.scanParams = null;
        }
        nextKeys();
    }

    private boolean nextKeys()
    {
        Jedis jedis = jedisPool.getResource();
        if (keyDataFormat.equals("zset")) {
            keysIterator = jedis.zrange(split.getKeyName(), split.getStart(), split.getEnd()).iterator();
        }
        else {
            String cursor = SCAN_POINTER_START;
            if (redisCursor != null) {
                cursor = redisCursor.getCursor();
            }
            LOG.debug("Scanning new Redis keys from cursor %s . %d values read so far", cursor, totalValues);
            redisCursor = jedis.scan(cursor, scanParams);
            List<String> keys = redisCursor.getResult();
            keysIterator = keys.iterator();
        }
        jedis.close();
        return true;
    }

    private boolean nextdata(final String key)
    {
        valueString = null;
        valueMap = null;
        Jedis jedis = jedisPool.getResource();
        if (valueDataFormat.equals("hash")) {
            valueMap = jedis.hgetAll(key);
            if (valueMap == null) {
                LOG.warn("Redis data modified while query was running, hash value at key %s deleted", key);
                return false;
            }
        }
        else {
            valueString = jedis.get(key);
            if (valueString == null) {
                LOG.warn("Redis data modified while query was running, string value at key %s deleted", key);
                return false;
            }
        }
        jedis.close();
        return true;
    }

    private boolean nextRow(final String nextkey)
    {
        nextdata(nextkey);
        byte[] keydata = nextkey.getBytes(StandardCharsets.UTF_8);
        byte[] valuedata = EMPTY_BYTE_ARRAY;
        if (valueString != null) {
            valuedata = valueString.getBytes(StandardCharsets.UTF_8);
        }
        totalBytes += valuedata.length;
        totalValues++;
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey = keyDecoder.decodeRow(keydata, null);
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = valueDecoder.decodeRow(valuedata, valueMap);
        Map<ColumnHandle, FieldValueProvider> rowValuesMap = new HashMap<>();
        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                RedisInternalFieldDescription internalFieldDescription = RedisInternalFieldDescription.forColumnName(columnHandle.getName());
                switch (internalFieldDescription) {
                    case KEY_FIELD:
                        rowValuesMap.put(columnHandle, bytesValueProvider(keydata));
                        break;
                    case VALUE_FIELD:
                        rowValuesMap.put(columnHandle, bytesValueProvider(valuedata));
                        break;
                    case KEY_LENGTH_FIELD:
                        rowValuesMap.put(columnHandle, longValueProvider(keydata.length));
                        break;
                    case VALUE_LENGTH_FIELD:
                        rowValuesMap.put(columnHandle, longValueProvider(valuedata.length));
                        break;
                    case KEY_CORRUPT_FIELD:
                        rowValuesMap.put(columnHandle, booleanValueProvider(!decodedKey.isPresent()));
                        break;
                    case VALUE_CORRUPT_FIELD:
                        rowValuesMap.put(columnHandle, booleanValueProvider(!decodedValue.isPresent()));
                        break;
                    default:
                        throw new IllegalArgumentException("unknown internal field " + internalFieldDescription);
                }
            }
        }
        decodedKey.ifPresent(rowValuesMap::putAll);
        decodedValue.ifPresent(rowValuesMap::putAll);

        for (int i = 0; i < columnHandles.size(); i++) {
            ColumnHandle columnHandle = columnHandles.get(i);
            currentRowValues[i] = rowValuesMap.get(columnHandle);
        }
        return true;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(final int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        while (!keysIterator.hasNext()) {
            if (redisCursor == null || redisCursor.getCursor().equals("0")) {
                if (!reported.getAndSet(true)) {
                    LOG.debug("Read a total of %d values with %d bytes.", totalValues, totalBytes);
                }
                return false;
            }
            nextKeys();
        }
        return nextRow(keysIterator.next());
    }

    @Override
    public boolean getBoolean(final int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        Class<?> javaType = columnHandles.get(field).getType().getJavaType();
        checkArgument(javaType == boolean.class, "Expected field %s to be type boolean but is %s", field, javaType);
        return currentRowValues[field].getBoolean();
    }

    @Override
    public long getLong(final int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        Class<?> javaType = columnHandles.get(field).getType().getJavaType();
        checkArgument(javaType == long.class, "Expected field %s to be type long but is %s", field, javaType);
        return currentRowValues[field].getLong();
    }

    @Override
    public double getDouble(final int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        Class<?> javaType = columnHandles.get(field).getType().getJavaType();
        checkArgument(javaType == double.class, "Expected field %s to be type Double but is %s", field, javaType);
        return currentRowValues[field].getDouble();
    }

    @Override
    public Slice getSlice(final int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        Class<?> javaType = columnHandles.get(field).getType().getJavaType();
        checkArgument(javaType == Slice.class, "Expected field %s to be type Slice but is %s", field, javaType);
        return currentRowValues[field].getSlice();
    }

    @Override
    public Object getObject(final int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        throw new IllegalArgumentException(format("Type %s is not supported", getType(field)));
    }

    @Override
    public boolean isNull(final int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return currentRowValues == null || currentRowValues[field].isNull();
    }

    @Override
    public void close()
    {
    }
}
