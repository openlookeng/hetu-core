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
package io.hetu.core.plugin.hbase.utils.serializers;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.hbase.client.Result;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.DAYS;

/**
 * StringRowSerializer
 *
 * @since 2020-03-30
 */
public class StringRowSerializer
        implements HBaseRowSerializer
{
    private static final Logger LOG = Logger.get(StringRowSerializer.class);

    private final Map<String, Map<String, String>> familyQualifierColumnMap = new HashMap<>();

    private final Map<String, String> columnValues = new HashMap<>();

    private String rowIdName;

    private List<HBaseColumnHandle> columnHandles;

    /**
     * set columnHandle
     *
     * @param columnHandleList columnHandleList
     */
    public void setColumnHandleList(List<HBaseColumnHandle> columnHandleList)
    {
        this.columnHandles = columnHandleList;
    }

    @Override
    public void setRowIdName(String name)
    {
        this.rowIdName = name;
    }

    @Override
    public void setMapping(String name, String family, String qualifier)
    {
        columnValues.put(name, null);
        Map<String, String> qualifierColumnMap = familyQualifierColumnMap.get(family);
        if (qualifierColumnMap == null) {
            qualifierColumnMap = new HashMap<>();
            familyQualifierColumnMap.put(family, qualifierColumnMap);
        }

        qualifierColumnMap.put(qualifier, name);
    }

    @Override
    public void reset()
    {
        columnValues.clear();
    }

    public Map<String, String> getColumnValues()
    {
        return columnValues;
    }

    /**
     * deserialize
     *
     * @param result Entry to deserialize
     * @param defaultValue defaultValue
     */
    public void deserialize(Result result, String defaultValue)
    {
        if (!columnValues.containsKey(rowIdName)) {
            columnValues.put(rowIdName, new String(result.getRow(), Charset.forName("UTF-8")));
        }

        String family;
        String qualifer;
        String value = null;
        byte[] bytes;
        for (HBaseColumnHandle hc : columnHandles) {
            if (!hc.getName().equals(rowIdName)) {
                family = hc.getFamily().get();
                qualifer = hc.getQualifier().get();
                bytes = result.getValue(family.getBytes(UTF_8), qualifer.getBytes(UTF_8));
                if (bytes == null) {
                    value = defaultValue;
                }
                else {
                    Charset cs = Charset.forName("UTF-8");
                    CharsetDecoder coder = cs.newDecoder();
                    try {
                        value = coder.decode(ByteBuffer.wrap(bytes)).toString();
                    }
                    catch (CharacterCodingException e) {
                        LOG.error("bytes decode to string error, cause by %s", e.getMessage());
                        e.printStackTrace();
                    }
                }
                columnValues.put(familyQualifierColumnMap.get(family).get(qualifer), value);
            }
        }
    }

    /**
     * set Object Bytes
     *
     * @param type Type
     * @param value Object
     * @return get byte[] for HBase add column.
     */
    @Override
    public byte[] setObjectBytes(Type type, Object value)
    {
        if (type.equals(BIGINT) && value instanceof Integer) {
            return ((Integer) value).toString().getBytes(UTF_8);
        }
        else if (type.equals(BIGINT) && value instanceof Long) {
            return ((Long) value).toString().getBytes(UTF_8);
        }
        else if (type.equals(BOOLEAN)) {
            return ((Boolean) (value.equals(Boolean.TRUE))).toString().getBytes(UTF_8);
        }
        else if (type.equals(DATE)) {
            return new Date(DAYS.toMillis((Long) value)).toString().getBytes(UTF_8);
        }
        else if (type.equals(DOUBLE)) {
            return value.toString().getBytes(UTF_8);
        }
        else if (type.equals(INTEGER) && value instanceof Integer) {
            return ((Integer) value).toString().getBytes(UTF_8);
        }
        else if (type.equals(INTEGER) && value instanceof Long) {
            return ((Long) value).toString().getBytes(UTF_8);
        }
        else if (type.equals(SMALLINT)) {
            return ((Long) value).toString().getBytes(UTF_8);
        }
        else if (type.equals(TIME)) {
            return new Time((Long) value).toString().getBytes(UTF_8);
        }
        else if (type.equals(TINYINT)) {
            return ((Long) value).toString().getBytes(UTF_8);
        }
        else if (type.equals(TIMESTAMP)) {
            return new Timestamp((Long) value).toString().getBytes(UTF_8);
        }
        else if (type instanceof VarcharType && value instanceof String) {
            return ((String) value).getBytes(UTF_8);
        }
        else if (type instanceof VarcharType && value instanceof Slice) {
            return ((Slice) value).toStringUtf8().getBytes(UTF_8);
        }
        else {
            LOG.error("getBytes: Unsupported type %s", type);
            throw new UnsupportedOperationException("Unsupported type " + type);
        }
    }

    /**
     * get Bytes Object
     *
     * @param type Type
     * @param columnName String
     * @param <T> Type
     * @return read from HBase, set into output
     */
    public <T> T getBytesObject(Type type, String columnName)
    {
        String fieldValue = getFieldValue(columnName);

        if (type.equals(BIGINT)) {
            return (T) (Long) Long.parseLong(fieldValue);
        }
        else if (type.equals(BOOLEAN)) {
            return (T) (Boolean) Boolean.parseBoolean(fieldValue);
        }
        else if (type.equals(DATE)) {
            LocalDate end = LocalDate.parse(fieldValue, DateTimeFormatter.ofPattern("yyy-MM-dd"));
            return (T) Long.valueOf(end.toEpochDay());
        }
        else if (type.equals(DOUBLE)) {
            return (T) (Double) Double.parseDouble(fieldValue);
        }
        else if (type.equals(INTEGER)) {
            return (T) ((Long) ((Integer) Integer.parseInt(fieldValue)).longValue());
        }
        else if (type.equals(SMALLINT)) {
            return (T) ((Long) ((Short) Short.parseShort(fieldValue)).longValue());
        }
        else if (type.equals(TIME)) {
            return (T) (Long) Time.valueOf(fieldValue).getTime();
        }
        else if (type.equals(TIMESTAMP)) {
            return (T) (Long) Timestamp.valueOf(fieldValue).getTime();
        }
        else if (type.equals(TINYINT)) {
            return (T) ((Long) ((Byte) Byte.parseByte(fieldValue)).longValue());
        }
        else if (type instanceof VarcharType) {
            return (T) Slices.utf8Slice(fieldValue);
        }
        else {
            LOG.error("decode: StringRowSerializer does not support decoding type %s", type);
            throw new PrestoException(NOT_SUPPORTED, "StringRowSerializer does not support decoding type " + type);
        }
    }

    @Override
    public boolean isNull(String name)
    {
        return columnValues.get(name) == null || columnValues.get(name).equals("NULL");
    }

    @Override
    public Block getArray(String name, Type type)
    {
        return HBaseRowSerializer.getBlockFromArray(type, getBytesObject(type, name));
    }

    @Override
    public Block getMap(String name, Type type)
    {
        return HBaseRowSerializer.getBlockFromMap(type, getBytesObject(type, name));
    }

    private String getFieldValue(String name)
    {
        return columnValues.get(name);
    }
}
