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

import io.airlift.slice.Slice;
import io.hetu.core.plugin.hbase.utils.TestSliceUtils;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.ShortArrayBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import org.apache.hadoop.io.Text;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
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
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

/**
 * AbstractTestHBaseRowSerializer
 *
 * @since 2020-03-20
 */
public abstract class AbstractTestHBaseRowSerializer
{
    private static final String COLUMN_NAME = "foo";
    private final HBaseRowSerializer serializer;

    protected AbstractTestHBaseRowSerializer(HBaseRowSerializer serializer)
    {
        this.serializer = serializer;
    }

    /**
     * testArray
     *
     * @throws UnsupportedOperationException Exception
     */
    @Test
    public void testArray()
    {
        short[] values = {1, 2, 3};
        try {
            Text text = new Text();
            text.set(
                    this.serializer.setObjectBytes(
                            new ArrayType(VARCHAR), new ShortArrayBlock(0, Optional.empty(), values)));
            this.serializer.getArray("array", new ArrayType(VARCHAR));
            throw new UnsupportedOperationException("testArray : failed");
        }
        catch (UnsupportedOperationException e) {
            assertEquals(e.toString(), "java.lang.UnsupportedOperationException: Unsupported type array(varchar)");
        }
    }

    /**
     * testBoolean
     */
    @Test
    public void testBoolean()
    {
        boolean flag = true;
        Text text = new Text();
        text.set(this.serializer.setObjectBytes(BOOLEAN, flag));
        TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

        flag = this.serializer.getBytesObject(BOOLEAN, COLUMN_NAME);
        assertEquals(true, flag);
    }

    /**
     * testDate
     */
    @Test
    public void testDate()
    {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC-00:00"));
        LocalDate date = LocalDate.now(Clock.systemUTC());
        Text text = new Text();
        Long expected = Long.valueOf(date.toEpochDay());
        text.set(this.serializer.setObjectBytes(DATE, expected));
        TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

        Long actual = this.serializer.getBytesObject(DATE, COLUMN_NAME);
        assertEquals(actual.toString(), expected.toString());
    }

    /**
     * testDouble
     */
    @Test
    public void testDouble()
    {
        Double expected = 123.45678;
        Text text = new Text();
        text.set(this.serializer.setObjectBytes(DOUBLE, expected));
        TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

        Double actual = serializer.getBytesObject(DOUBLE, COLUMN_NAME);
        assertEquals(actual, expected);
    }

    /**
     * testFloat
     */
    @Test
    public void testFloat()
    {
        try {
            Float expected = 123.45678f;
            Double expectedDouble = expected.doubleValue();
            Long expectedLong = expectedDouble.longValue();
            Text text = new Text();
            text.set(this.serializer.setObjectBytes(REAL, expectedLong));
            TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

            Long actual = serializer.getBytesObject(REAL, COLUMN_NAME);
            assertEquals(actual, expectedLong);
            throw new UnsupportedOperationException("Unsupported type real");
        }
        catch (UnsupportedOperationException e) {
            assertEquals(e.toString(), "java.lang.UnsupportedOperationException: Unsupported type real");
        }
    }

    /**
     * testInt
     */
    @Test
    public void testInt()
    {
        Integer expected = 123456;
        Text text = new Text();
        text.set(this.serializer.setObjectBytes(INTEGER, expected));
        TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

        Long actual = serializer.getBytesObject(INTEGER, COLUMN_NAME);
        Integer readActual = actual.intValue();
        assertEquals(readActual, expected);
    }

    /**
     * testLong
     */
    @Test
    public void testLong()
    {
        Long expected = 12345678901L;
        Text text = new Text();
        text.set(this.serializer.setObjectBytes(BIGINT, expected));
        TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

        Long actual = serializer.getBytesObject(BIGINT, COLUMN_NAME);
        assertEquals(actual, expected);
    }

    /**
     * testBigint
     */
    @Test
    public void testBigint()
    {
        Integer expected = 123456;
        Text text = new Text();
        text.set(this.serializer.setObjectBytes(BIGINT, expected));
        TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

        Long actual = serializer.getBytesObject(BIGINT, COLUMN_NAME);
        Integer realActual = actual.intValue();
        assertEquals(realActual, expected);
    }

    /**
     * testInteger
     */
    @Test
    public void testInteger()
    {
        Long expected = 123456789L;
        Text text = new Text();
        text.set(this.serializer.setObjectBytes(INTEGER, expected));
        TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

        Long actual = serializer.getBytesObject(INTEGER, COLUMN_NAME);
        assertEquals(actual, expected);
    }

    /**
     * testShort
     */
    @Test
    public void testShort()
    {
        Short expected = 12345;
        Text text = new Text();
        text.set(this.serializer.setObjectBytes(SMALLINT, expected.longValue()));
        TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

        Long actual = serializer.getBytesObject(SMALLINT, COLUMN_NAME);
        Short realActual = actual.shortValue();
        assertEquals(realActual, expected);
    }

    /**
     * testTime
     */
    @Test
    public void testTime()
    {
        Time expected = new Time(new java.util.Date().getTime());
        Long expectedTime = expected.getTime();
        Text text = new Text();
        text.set(this.serializer.setObjectBytes(TIME, expectedTime));
        TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

        Long actual = serializer.getBytesObject(TIME, COLUMN_NAME);
        assertEquals(new Time(actual).toString(), expected.toString());
    }

    /**
     * testTimestamp
     */
    @Test
    public void testTimestamp()
    {
        Timestamp expected = new Timestamp(new java.util.Date().getTime());
        Long expectedTime = expected.getTime();
        Text text = new Text();
        text.set(this.serializer.setObjectBytes(TIMESTAMP, expectedTime));
        TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

        Long actual = serializer.getBytesObject(TIMESTAMP, COLUMN_NAME);
        assertEquals(actual, expectedTime);
    }

    /**
     * testTinyInt
     */
    @Test
    public void testTinyInt()
    {
        Byte expected = 123;
        Text text = new Text();
        text.set(this.serializer.setObjectBytes(TINYINT, expected.longValue()));
        TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

        Long actual = serializer.getBytesObject(TINYINT, COLUMN_NAME);
        Byte realActual = actual.byteValue();
        assertEquals(realActual, expected);
    }

    /**
     * testVarbinary
     */
    @Test
    public void testVarbinary()
    {
        try {
            byte[] expected = UUID.randomUUID().toString().getBytes();
            Text text = new Text();
            text.set(this.serializer.setObjectBytes(VARBINARY, expected));
            TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

            Slice actual = serializer.getBytesObject(VARBINARY, COLUMN_NAME);
            assertEquals(actual.getBytes(), expected);
            throw new UnsupportedOperationException("AbstractTestHBaseRowSerializer-> testVarbinary : failed");
        }
        catch (UnsupportedOperationException e) {
            assertEquals(e.toString(), "java.lang.UnsupportedOperationException: Unsupported type varbinary");
        }
    }

    /**
     * testVarchar
     *
     * @throws PrestoException
     */
    @Test
    public void testVarchar()
            throws PrestoException
    {
        String expected = UUID.randomUUID().toString();
        Text text = new Text();
        text.set(this.serializer.setObjectBytes(VARCHAR, expected));
        TestStringRowSerializer.PSERIALIZER.getColumnValues().put(COLUMN_NAME, new String(text.getBytes()));

        Slice actual = serializer.getBytesObject(VARCHAR, COLUMN_NAME);
        assertEquals(actual.toStringUtf8(), expected);

        try {
            serializer.getBytesObject(REAL, COLUMN_NAME);
            throw new PrestoException(NOT_SUPPORTED, " testREAL : failed");
        }
        catch (PrestoException e) {
            assertEquals(
                    e.toString(),
                    "io.prestosql.spi.PrestoException: StringRowSerializer does not support decoding type real");
        }
    }

    /**
     * testGetDefault
     */
    @Test
    public void testGetDefault()
    {
        HBaseRowSerializer.getDefault();
    }

    /**
     * testGetArray
     *
     * @throws PrestoException
     */
    @Test
    public void testGetArray()
            throws PrestoException
    {
        try {
            serializer.getArray("array", new ArrayType(VARCHAR));
            throw new PrestoException(NOT_SUPPORTED, " testGetArray : failed");
        }
        catch (PrestoException e) {
            assertEquals(
                    e.toString(),
                    "io.prestosql.spi.PrestoException: StringRowSerializer does not support decoding type"
                            + " array(varchar)");
        }
    }

    /**
     * testGetArrayFromBlock
     */
    @Test
    public void testGetArrayFromBlock()
    {
        int[] offsets = {0, 4};
        Block rowkey = new VariableWidthBlock(1, TestSliceUtils.createSlice("0001"), offsets, Optional.empty());
        HBaseRowSerializer.getArrayFromBlock(VARCHAR, rowkey);
    }

    /**
     * testGetMapFromBlock
     *
     * @throws IllegalArgumentException
     */
    @Test
    public void testGetMapFromBlock()
            throws Exception
    {
        MethodHandle methodHandle = Mockito.mock(MethodHandle.class);
        MapType type = new MapType(VARCHAR, BIGINT, methodHandle, methodHandle, methodHandle, methodHandle);
        int[] offsets = {0, 2, 4};
        Block rowkey = new VariableWidthBlock(2, TestSliceUtils.createSlice("12345678901"), offsets, Optional.empty());
        HBaseRowSerializer.getMapFromBlock(type, rowkey);
    }

    /**
     * testGetBlockFromArray
     */
    @Test
    public void testGetBlockFromArray()
    {
        List<String> array = new ArrayList<>();
        array.add("column");
        HBaseRowSerializer.getBlockFromArray(VARCHAR, array);
    }
}
