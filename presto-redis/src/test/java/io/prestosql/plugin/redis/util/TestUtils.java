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
package io.prestosql.plugin.redis.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.airlift.json.JsonCodec;
import io.airlift.tpch.TpchTable;
import io.prestosql.plugin.redis.description.RedisTableDescription;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.Varchars;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.QueryRunner;
import org.joda.time.format.ISODateTimeFormat;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.util.DateTimeUtils.parseTimeLiteral;
import static io.prestosql.spi.util.DateTimeUtils.parseTimestampWithTimeZone;
import static io.prestosql.spi.util.DateTimeUtils.parseTimestampWithoutTimeZone;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestUtils
{
    private TestUtils(){}

    public static Map.Entry<SchemaTableName, RedisTableDescription> loadTpchTableDescription(
            final JsonCodec<RedisTableDescription> tableDescriptionJsonCodec,
            final SchemaTableName schemaTableName,
            final String dataFormat)
    {
        RedisTableDescription redisTableDescriptionTemplate;

        InputStream resourceAsStream = TestUtils.class.getResourceAsStream(format("/tpch/%s/%s.json", dataFormat, schemaTableName.getTableName()));
        try {
            redisTableDescriptionTemplate = tableDescriptionJsonCodec.fromJson(ByteStreams.toByteArray(resourceAsStream));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        RedisTableDescription redisTableDescription = new RedisTableDescription(
                schemaTableName.getTableName(),
                schemaTableName.getSchemaName(),
                redisTableDescriptionTemplate.getKey(),
                redisTableDescriptionTemplate.getValue());

        String schemaName = schemaTableName.getSchemaName();
        SchemaTableName schemaTableName1 = new SchemaTableName(schemaName, schemaTableName.getTableName());

        return new AbstractMap.SimpleEntry<>(schemaTableName1, redisTableDescription);
    }

    public static void writeToRedis(
            final QueryRunner queryRunner,
            final RedisServer redisServer,
            final TpchTable table,
            final String dataformat) throws JsonProcessingException
    {
        QualifiedObjectName tpch = new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH));
        MaterializedResult columns = queryRunner.execute(format("SHOW COLUMNS from %s", tpch));
        ImmutableList.Builder<String> builder = ImmutableList.builder();

        for (MaterializedRow materializedRow : columns) {
            builder.add((String) materializedRow.getField(0));
        }

        ImmutableList<String> columnsName = builder.build();

        MaterializedResult execute = queryRunner.execute(format("SELECT * from %s", tpch));

        List<MaterializedRow> materializedRows = execute.getMaterializedRows();
        List<Type> types = execute.getTypes();
        Iterator<MaterializedRow> iterator = execute.iterator();
        AtomicLong count = new AtomicLong();

        JedisPool jedisPool = redisServer.getJedisPool();
        ObjectMapper objectMapper = new ObjectMapper();
        Jedis jedis = jedisPool.getResource();

        while (iterator.hasNext()) {
            String redisKey = "tpch:" + table.getTableName() + ":" + count.getAndIncrement();
            MaterializedRow next = iterator.next();
            System.out.println(redisKey);
            List<Object> fields = next.getFields();
            switch (dataformat) {
                case "string":
                    ImmutableMap.Builder<String, Object> builder1 = ImmutableMap.builder();
                    for (int i = 0; i < fields.size(); i++) {
                        Type type = types.get(i);
                        Object value = TestUtils.convertValue(fields.get(i), type, queryRunner.getDefaultSession().getTimeZoneKey());
                        if (value != null) {
                            builder1.put(columnsName.get(i), value);
                        }
                    }
                    String s = objectMapper.writeValueAsString(builder1.build());
                    jedis.setnx(redisKey, s);
                    break;
                case "hash":
                    // add keys to zset
                    String redisZset = "keyset:" + table.getTableName();
                    jedis.zadd(redisZset, count.doubleValue(), redisKey);
                    System.out.println(redisKey);
                    // add values to Hash
                    for (int i = 0; i < fields.size(); i++) {
                        jedis.hset(redisKey, columnsName.get(i), fields.get(i).toString());
                    }
                    break;
                default:
                    throw new AssertionError("unhandled value type: " + dataformat);
            }
        }
    }

    public static Object convertValue(final Object value, final Type type, final TimeZoneKey timeZoneKey)
    {
        if (value == null) {
            return null;
        }

        if (BOOLEAN.equals(type) || Varchars.isVarcharType(type)) {
            return value;
        }

        if (BIGINT.equals(type)) {
            return ((Number) value).longValue();
        }

        if (INTEGER.equals(type)) {
            return ((Number) value).intValue();
        }

        if (DOUBLE.equals(type)) {
            return ((Number) value).doubleValue();
        }

        if (DATE.equals(type)) {
            return value;
        }

        if (TIME.equals(type)) {
            return ISODateTimeFormat.dateTime().print(parseTimeLiteral(timeZoneKey, (String) value));
        }

        if (TIMESTAMP.equals(type)) {
            return ISODateTimeFormat.dateTime().print(parseTimestampWithoutTimeZone(timeZoneKey, (String) value));
        }

        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return ISODateTimeFormat.dateTime().print(unpackMillisUtc(parseTimestampWithTimeZone(timeZoneKey, (String) value)));
        }

        throw new AssertionError("unhandled type: " + type);
    }

    public static Map.Entry<SchemaTableName, RedisTableDescription> createEmptyTableDescription(final SchemaTableName schemaTableName)
    {
        RedisTableDescription tableDescription = new RedisTableDescription(
                schemaTableName.getTableName(),
                schemaTableName.getSchemaName(),
                null,
                null);

        return new AbstractMap.SimpleImmutableEntry<>(schemaTableName, tableDescription);
    }
}
