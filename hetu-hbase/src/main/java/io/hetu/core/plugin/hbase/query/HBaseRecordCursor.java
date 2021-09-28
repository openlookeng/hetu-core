/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.hbase.query;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.split.HBaseSplit;
import io.hetu.core.plugin.hbase.utils.serializers.HBaseRowSerializer;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;

/**
 * HBaseRecordCursor
 *
 * @since 2020-03-06
 */
public class HBaseRecordCursor
        implements RecordCursor
{
    private static final Logger LOG = Logger.get(HBaseRecordCursor.class);

    /**
     * columnHandles list
     */
    public List<HBaseColumnHandle> columnHandles;

    /**
     * columnTypes list
     */
    public List<Type> columnTypes;

    /**
     * serializer: encode/decode
     */
    public HBaseRowSerializer serializer;

    /**
     * result iterator
     */
    public Iterator<Result> iterator;

    /**
     * result scanner
     */
    public ResultScanner scanner;

    /**
     * column name
     */
    public String[] fieldToColumnName;

    /**
     * rowId name
     */
    public String rowIdName;

    /**
     * read bytes
     */
    public long bytesRead;

    /**
     * hbase split
     */
    public HBaseSplit split;

    /**
     * start time
     */
    public long startTime;

    /**
     * default
     */
    public String defaultValue;

    /**
     * constructor
     *
     * @param columnHandles columnHandles
     * @param columnTypes columnTypes
     * @param serializer serializer
     * @param rowIdName rowIdName
     * @param fieldToColumnName fieldToColumnName
     * @param defaultValue defaultValue
     */
    public HBaseRecordCursor(
            List<HBaseColumnHandle> columnHandles,
            List<Type> columnTypes,
            HBaseRowSerializer serializer,
            String[] fieldToColumnName,
            String rowIdName,
            String defaultValue)
    {
        this.serializer = serializer;
        this.columnHandles = columnHandles;
        this.columnTypes = columnTypes;
        this.fieldToColumnName = fieldToColumnName;
        this.rowIdName = rowIdName;
        this.serializer.setColumnHandleList(columnHandles);
        this.fieldToColumnName = fieldToColumnName.clone();
        this.defaultValue = defaultValue;
    }

    /**
     * constructor
     *
     * @param columnHandles columnHandles
     * @param columnTypes columnTypes
     * @param serializer serializer
     * @param scanner scanner
     * @param rowIdName rowIdName
     * @param fieldToColumnName fieldToColumnName
     * @param defaultValue defaultValue
     */
    public HBaseRecordCursor(
            List<HBaseColumnHandle> columnHandles,
            List<Type> columnTypes,
            HBaseRowSerializer serializer,
            ResultScanner scanner,
            String[] fieldToColumnName,
            String rowIdName,
            String defaultValue)
    {
        this.columnHandles = columnHandles;
        this.columnTypes = columnTypes;
        this.serializer = serializer;
        this.serializer.setColumnHandleList(columnHandles);
        this.scanner = scanner;
        this.iterator = scanner.iterator();
        this.fieldToColumnName = fieldToColumnName.clone();
        this.rowIdName = rowIdName;
        this.defaultValue = defaultValue;
    }

    @Override
    public long getCompletedBytes()
    {
        return bytesRead;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field >= 0 && field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (iterator.hasNext()) {
            serializer.reset();
            Result row = iterator.next();
            serializer.deserialize(row, this.defaultValue);
            return true;
        }
        return false;
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return serializer.isNull(fieldToColumnName[field]);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, ImmutableList.of(BOOLEAN));
        Type type = getType(field);
        return (boolean) (serializer.getBytesObject(type, fieldToColumnName[field]));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, ImmutableList.of(DOUBLE));
        Type type = getType(field);
        return (double) (serializer.getBytesObject(type, fieldToColumnName[field]));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, ImmutableList.of(BIGINT, DATE, INTEGER, REAL, SMALLINT, TIME, TIMESTAMP, TINYINT));
        Type type = getType(field);
        return (long) (serializer.getBytesObject(type, fieldToColumnName[field]));
    }

    @Override
    public Object getObject(int field)
    {
        Type type = getType(field);
        return serializer.getMap(fieldToColumnName[field], type);
    }

    @Override
    public Slice getSlice(int field)
    {
        Type type = getType(field);
        return (Slice) (serializer.getBytesObject(type, fieldToColumnName[field]));
    }

    /**
     * Checks that the given field is one of the provided types.
     *
     * @param field Ordinal of the field
     * @param expectedListTypes An array of expected types
     * @throws IllegalArgumentException If the given field does not match one of the types
     */
    private void checkFieldType(int field, List<Type> expectedListTypes)
    {
        Type actual = getType(field);
        for (Type type : expectedListTypes) {
            if (actual.equals(type)) {
                return;
            }
        }
        LOG.error(
                format(
                        "checkFieldType: Expected field %s to be a type of %s but is %s",
                        field, StringUtils.join(expectedListTypes, ","), actual));
        throw new IllegalArgumentException(
                format(
                        "Expected field %s to be a type of %s but is %s",
                        field, StringUtils.join(expectedListTypes, ","), actual));
    }

    @Override
    public void close()
    {
        if (this.scanner != null && !(this.scanner instanceof ClientSideRegionScanner)) {
            this.scanner.close();
        }
    }
}
