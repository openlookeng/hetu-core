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
package io.hetu.core.plugin.hbase.connector;

import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static org.testng.Assert.assertEquals;

/**
 * TestHBaseTableHandle
 *
 * @since 2020-03-20
 */
public class TestHBaseTableHandle
{
    private String rowId = "rowkey";
    private String schema = "hbase";
    private String serializerClassName = "io.hetu.core.plugin.hbase.utils.serializers.StringRowSerializer";
    private String table = "table-1";
    private HBaseTableHandle hBaseTableHandle;

    /**
     * testCreateHBaseTableHandle
     */
    @Test
    public void testCreateHBaseTableHandle()
    {
        hBaseTableHandle =
                new HBaseTableHandle(
                        schema,
                        table,
                        rowId,
                        false,
                        serializerClassName,
                        Optional.of(table),
                        "",
                        OptionalLong.empty());
        assertEquals(
                "HBaseTableHandle{schema=hbase, table=table-1, rowId=rowkey, internal=false, "
                        + "serializerClassName=io.hetu.core.plugin.hbase."
                        + "utils.serializers.StringRowSerializer, indexColumns=, "
                        + "hbaseTableName=Optional[table-1]}",
                hBaseTableHandle.toString());
    }

    /**
     * testMethod
     */
    @Test
    public void testMethod()
    {
        if (hBaseTableHandle == null) {
            testCreateHBaseTableHandle();
        }
        assertEquals(schema, hBaseTableHandle.getSchema());
        assertEquals(table, hBaseTableHandle.getTable());
        assertEquals(rowId, hBaseTableHandle.getRowId());
        assertEquals(serializerClassName, hBaseTableHandle.getSerializerClassName());
    }

    /**
     * testEquals
     */
    @Test
    public void testEquals()
    {
        if (hBaseTableHandle == null) {
            testCreateHBaseTableHandle();
        }
        HBaseTableHandle hBaseTableHandle2 =
                new HBaseTableHandle(
                        schema,
                        table,
                        rowId,
                        false,
                        serializerClassName,
                        Optional.of(table),
                        "",
                        OptionalLong.empty());

        assertEquals(true, hBaseTableHandle.equals(hBaseTableHandle2));
    }

    /**
     * testRunOnly
     */
    @Test
    public void testRunOnly()
    {
        if (hBaseTableHandle == null) {
            testCreateHBaseTableHandle();
        }
        this.hBaseTableHandle.hashCode();
        this.hBaseTableHandle.toSchemaTableName();
        assertEquals("hbase.table-1", this.hBaseTableHandle.getFullTableName());
        this.hBaseTableHandle.equals(this.hBaseTableHandle);
        this.hBaseTableHandle.equals(null);
    }
}
