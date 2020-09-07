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
package io.hetu.core.plugin.hbase.conf;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.testng.Assert.assertEquals;

/**
 * TestHBaseTableProperties
 *
 * @since 2020-03-20
 */
public class TestHBaseTableProperties
{
    private static Map<String, Object> tableProperties;

    static {
        tableProperties = new HashMap();
    }

    /**
     * testHBaseTableProperties
     */
    @Test
    public void testHBaseTableProperties()
    {
        HBaseTableProperties htp = new HBaseTableProperties();
        htp.getTableProperties();
    }

    /**
     * testGetColumnMapping
     */
    @Test
    public void testGetColumnMapping()
    {
        tableProperties.put("column_mapping", null);
        Map<String, Pair<String, String>> xx = HBaseTableProperties.getColumnMapping(tableProperties).orElse(null);
        assertEquals(null, xx);

        tableProperties.put("column_mapping", "a:f_a:q_a");
        xx = HBaseTableProperties.getColumnMapping(tableProperties).orElse(null);
        assertEquals("f_a", xx.get("a").getLeft());
        assertEquals("q_a", xx.get("a").getRight());
    }

    /**
     * testGetIndexColumns
     */
    @Test
    public void testGetIndexColumns()
    {
        tableProperties.put("index_columns", null);
        List<String> xx = HBaseTableProperties.getIndexColumns(tableProperties).orElse(null);
        assertEquals(null, xx);

        tableProperties.put("index_columns", "row");
        xx = HBaseTableProperties.getIndexColumns(tableProperties).orElse(null);
        assertEquals("row", xx.get(0));
    }

    /**
     * testGetLocalityGroups
     */
    @Test
    public void testGetLocalityGroups()
    {
        tableProperties.put("locality_groups", null);
        Map<String, Set<String>> xx = HBaseTableProperties.getLocalityGroups(tableProperties).orElse(null);
        assertEquals(null, xx);

        tableProperties.put("locality_groups", "foo:b,c");
        xx = HBaseTableProperties.getLocalityGroups(tableProperties).orElse(null);
        assertEquals(2, xx.get("foo").size());
    }

    /**
     * testGetRowId
     */
    @Test
    public void testGetRowId()
    {
        tableProperties.put("row_id", "row_id");
        String rowid = HBaseTableProperties.getRowId(tableProperties).orElse("");
        assertEquals("row_id", rowid);
    }

    /**
     * testGetSerializerClass
     */
    @Test
    public void testGetSerializerClass()
    {
        Optional<String> expected =
                Optional.of("StringRowSerializer");
        tableProperties.put("serializer", expected);
        Optional<String> seria = HBaseTableProperties.getSerializerClass(tableProperties);
        assertEquals(String.valueOf(expected), seria.get());
    }

    /**
     * testIsExternal
     */
    @Test
    public void testIsExternal()
    {
        tableProperties.put("external", false);
        boolean flag = HBaseTableProperties.isExternal(tableProperties);
        assertEquals(false, flag);
    }
}
