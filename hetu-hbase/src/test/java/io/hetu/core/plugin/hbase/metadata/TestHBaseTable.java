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
package io.hetu.core.plugin.hbase.metadata;

import io.hetu.core.plugin.hbase.client.TestUtils;
import io.prestosql.spi.connector.SchemaTableName;
import org.codehaus.jettison.json.JSONException;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

/**
 * TestHBaseTable
 *
 * @since 2020-03-20
 */
public class TestHBaseTable
{
    private HBaseTable hBaseTable;

    /**
     * testHBaseTable
     */
    @Test
    public void testHBaseTable()
    {
        hBaseTable =
                new HBaseTable(
                        "hbase",
                        "table",
                        TestUtils.createColumnList(),
                        "rowkey",
                        false,
                        Optional.of("StringRowSerializer"),
                        Optional.of("table"),
                        Optional.of(""),
                        Optional.of(""));
    }

    /**
     * testGet
     */
    @Test
    public void testGet()
    {
        if (hBaseTable == null) {
            testHBaseTable();
        }

        assertEquals("rowkey", hBaseTable.getRowId());
        assertEquals("hbase", hBaseTable.getSchema());
        assertEquals("table", hBaseTable.getTable());
        assertEquals("hbase.table", hBaseTable.getFullTableName());
        assertEquals("hbase.table", HBaseTable.getFullTableName(hBaseTable.getSchema(), hBaseTable.getTable()));
        assertEquals("hbase.table", HBaseTable.getFullTableName(new SchemaTableName("hbase", "table")));
        assertEquals(5, hBaseTable.getColumns().size());
        assertEquals(
                "StringRowSerializer",
                hBaseTable.getSerializerClassName());
        assertEquals(false, hBaseTable.isExternal());
        hBaseTable.toString();
        hBaseTable.getSchemaTableName();
        hBaseTable.isIndexed();
        hBaseTable.getRowIdOrdinal();
        hBaseTable.getColumnMetadatas();

        assertEquals("", hBaseTable.getIndexTableName());
        assertEquals("", hBaseTable.getMetricsTableName());
        try {
            hBaseTable.parseHbaseTableToJson();
        }
        catch (JSONException e) {
            assertEquals(1, 2);
        }
    }
}
