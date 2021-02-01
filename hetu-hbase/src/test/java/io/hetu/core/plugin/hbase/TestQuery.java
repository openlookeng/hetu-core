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
package io.hetu.core.plugin.hbase;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.hetu.core.plugin.hbase.client.TestUtils;
import io.hetu.core.plugin.hbase.client.TestingConnectorSession;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.hetu.core.plugin.hbase.connector.TestHBaseClientConnection;
import io.hetu.core.plugin.hbase.metadata.TestingHetuMetastore;
import io.hetu.core.plugin.hbase.query.HBaseRecordCursor;
import io.hetu.core.plugin.hbase.query.HBaseRecordSet;
import io.hetu.core.plugin.hbase.split.HBaseSplit;
import io.hetu.core.plugin.hbase.utils.TestSliceUtils;
import io.hetu.core.plugin.hbase.utils.serializers.StringRowSerializer;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.type.Type;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

/**
 * TestQuery
 *
 * @since 2020-03-20
 */
public class TestQuery
{
    private HBaseConnection hconn;
    private HBaseConfig hCConf = new HBaseConfig();
    private ConnectorSession session;
    private HBaseRecordSet recordSet;
    private TestingHetuMetastore hetuMetastore;
    private HBaseSplit split;

    /**
     * setUp
     */
    @BeforeClass
    public void setUp()
    {
        hCConf.setZkClientPort("2181");
        hCConf.setZkQuorum("zk1");
        hetuMetastore = new TestingHetuMetastore();
        hconn = new TestHBaseClientConnection(hCConf, hetuMetastore.getHetuMetastore());
        hconn.getConn();
        session = new TestingConnectorSession("root");
        split =
                new HBaseSplit(
                        "rowKey",
                        TestUtils.createHBaseTableHandle(),
                        new ArrayList<HostAddress>(1),
                        "startrow",
                        "endrow",
                        new HashMap<>(),
                        -1,
                        false,
                        null);
        recordSet =
                new HBaseRecordSet(
                        hconn, session, split, TestUtils.createHBaseTableHandle(), TestUtils.createColumnList());
    }

    /**
     * clear
     */
    @AfterClass
    public void clear()
    {
        hetuMetastore.close();
    }

    /**
     * testHBaseRecordSetCursorIsBatchGet
     */
    @Test
    public void testHBaseRecordSetCursorIsBatchGet()
    {
        HBaseTableHandle tableHandle =
                new HBaseTableHandle(
                        "hbase",
                        "test_table",
                        "rowkey",
                        false,
                        "io.hetu.core.plugin.hbase.utils.serializers.StringRowSerializer",
                        Optional.of("test_table"),
                        "",
                        TestUtils.createTupleDomain(1),
                        TestUtils.createColumnList(),
                        0,
                        OptionalLong.empty());
        HBaseSplit hBasesplit =
                new HBaseSplit(
                        "rowKey",
                        tableHandle,
                        new ArrayList<HostAddress>(1),
                        "startrow",
                        "endrow",
                        new HashMap<>(),
                        -1,
                        false,
                        null);
        HBaseRecordSet rSet = new HBaseRecordSet(hconn, session, hBasesplit, tableHandle, TestUtils.createColumnList());
        rSet.cursor();
    }

    /**
     * testHBaseRecordSetGetFiltersFromDomains
     */
    @Test
    public void testHBaseRecordSetGetFiltersFromDomains()
    {
        List<HBaseColumnHandle> list = new ArrayList<>();
        list.add(TestUtils.createHBaseColumnRowId("rowkey"));
        HBaseTableHandle tableHandle =
                new HBaseTableHandle(
                        "hbase",
                        "test_table",
                        "rowkey",
                        false,
                        "io.hetu.core.plugin.hbase.utils.serializers.StringRowSerializer",
                        Optional.of("test_table"),
                        "",
                        null,
                        list,
                        0,
                        OptionalLong.empty());

        // case ABOVE
        Map<Integer, List<Range>> ranges = new HashMap<>();
        long startRow = 1;
        long endRow = 12345678;
        List<Range> range = new ArrayList<>();
        range.add(new Range(Marker.above(BIGINT, startRow), Marker.below(BIGINT, endRow)));
        ranges.put(0, range);
        HBaseSplit hBasesplit =
                new HBaseSplit(
                        "rowkey", tableHandle, new ArrayList<HostAddress>(1), "1", "12345678", ranges, -1, false, null);
        HBaseRecordSet rSet = new HBaseRecordSet(hconn, session, hBasesplit, tableHandle, list);
        rSet.getFiltersFromDomains(ranges);

        // List<Range> is null
        ranges.clear();
        ranges.put(0, null);
        rSet.getFiltersFromDomains(ranges);

        // case EXACTLY
        range.clear();
        ranges.clear();
        long exactly = 12345678;
        range.add(Range.equal(BIGINT, exactly));
        ranges.put(0, range);
        rSet.getFiltersFromDomains(ranges);

        // other case
        range.add(Range.range(BIGINT, exactly, true, exactly * 2, true));
        ranges.put(1, range);
        list.add(TestUtils.createHBaseColumnHandle("a", "f_a", "q_a", 1));
        HBaseRecordSet rSet2Column = new HBaseRecordSet(hconn, session, hBasesplit, tableHandle, list);
        rSet2Column.getFiltersFromDomains(ranges);
    }

    private HBaseColumnHandle createHBaseColumnHandle(Type type)
    {
        return new HBaseColumnHandle("name", Optional.empty(), Optional.empty(), type, 0, "HBase row ID", false);
    }

    /**
     * testHBaseRecordSetCheckPredicateType
     */
    @Test
    public void testHBaseRecordSetCheckPredicateType()
            throws Exception
    {
        Method method = HBaseRecordSet.class.getDeclaredMethod("checkPredicateType", HBaseColumnHandle.class);
        method.setAccessible(true);
        assertEquals(true, method.invoke(recordSet, createHBaseColumnHandle(BOOLEAN)));
        assertEquals(true, method.invoke(recordSet, createHBaseColumnHandle(DOUBLE)));
        assertEquals(true, method.invoke(recordSet, createHBaseColumnHandle(BIGINT)));
        assertEquals(true, method.invoke(recordSet, createHBaseColumnHandle(INTEGER)));
        assertEquals(true, method.invoke(recordSet, createHBaseColumnHandle(SMALLINT)));
        assertEquals(true, method.invoke(recordSet, createHBaseColumnHandle(TINYINT)));
        assertEquals(false, method.invoke(recordSet, createHBaseColumnHandle(DATE)));
    }

    /**
     * testHBaseRecordSetGetRangeValue
     */
    @Test
    public void testHBaseRecordSetGetRangeValue()
            throws Exception
    {
        Method method = HBaseRecordSet.class.getDeclaredMethod("getRangeValue", Object.class);
        method.setAccessible(true);
        method.invoke(recordSet, TestSliceUtils.createSlice("slice"));
    }

    private HBaseColumnHandle createColumnList(String name, String family, String qualifer, int ordinal, Type type)
    {
        HBaseColumnHandle hBaseColumnHandle =
                new HBaseColumnHandle(
                        name,
                        Optional.of(family),
                        Optional.of(qualifer),
                        type,
                        ordinal,
                        "HBase column Optional[" + family + "]:Optional[" + qualifer + "]. Indexed: false",
                        false);
        return hBaseColumnHandle;
    }

    /**
     * testHBaseRecordCursor
     *
     * @throws ClassCastException Exception
     */
    @Test
    public void testHBaseRecordCursor()
    {
        List<HBaseColumnHandle> columnHandles = new ArrayList<>();
        columnHandles.add(TestUtils.createHBaseColumnRowId("rowkey"));
        columnHandles.add(createColumnList("a", "f", "q_a", 1, BOOLEAN));
        columnHandles.add(createColumnList("b", "f", "q_b", 2, DOUBLE));
        columnHandles.add(createColumnList("c", "f", "q_c", 3, BIGINT));
        columnHandles.add(createColumnList("d", "f", "q_d", 4, VARCHAR));
        String[] fieldToColumnName = {"rowkey", "a", "b", "c", "d"};
        StringRowSerializer serializer = new StringRowSerializer();
        serializer.setColumnHandleList(TestUtils.createColumnList());
        boolean flag = true;
        serializer.getColumnValues().put(fieldToColumnName[1], new String(serializer.setObjectBytes(BOOLEAN, flag)));
        Double expectedDouble = 123.45678;
        serializer
                .getColumnValues()
                .put(fieldToColumnName[2], new String(serializer.setObjectBytes(DOUBLE, expectedDouble)));
        long expectedLong = 123456789L;
        serializer
                .getColumnValues()
                .put(fieldToColumnName[3], new String(serializer.setObjectBytes(BIGINT, expectedLong)));
        Slice expectedSlice = Slices.utf8Slice(UUID.randomUUID().toString());
        serializer
                .getColumnValues()
                .put(fieldToColumnName[4], new String(serializer.setObjectBytes(VARCHAR, expectedSlice)));

        HBaseRecordCursor hrc =
                new HBaseRecordCursor(columnHandles, null, serializer, "rowkey", fieldToColumnName, "defaultValue");

        assertEquals(flag, hrc.getBoolean(1));
        assertEquals(expectedDouble, hrc.getDouble(2));
        assertEquals(expectedLong, hrc.getLong(3));
        assertEquals(expectedSlice, hrc.getSlice(4));
        try {
            hrc.getObject(4);
        }
        catch (ClassCastException e) {
            assertEquals(
                    e.toString(),
                    "java.lang.ClassCastException: io.airlift.slice.Slice cannot be cast to java.util.Map");
        }
    }
}
