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
package io.hetu.core.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static org.testng.Assert.assertEquals;

/**
 * This is testing ClickHouseConfig functions
 */
public class ClickHouseConfigTest
{
    private static final int COMMUNICATION_TIMEOUT = 10;

    private static final int PACKET_SIZE = 150000;

    /**
     * This is testing ClickHousePropertyMappings
     */
    @Test
    public void testClickHousePropertyMappings()
    {
        final String tableTypes = "TABLE,VIEW,USER DEFINED,SYNONYM,OLAP VIEW,JOIN VIEW,HIERARCHY VIEW" + ",CALC VIEW,SYSTEM TABLE,NO LOGGING TEMPORARY,GLOBAL TEMPORARY";
        final String schemaPattern = "default";

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("clickhouse.table-types", tableTypes)
                .put("clickhouse.schema-pattern", schemaPattern)
                .put("clickhouse.query.pushdown.enabled", "false")
                .put("clickhouse.socket_timeout", "100")
                .build();

        ClickHouseConfig expected = new ClickHouseConfig()
                .setTableTypes(tableTypes)
                .setSchemaPattern(schemaPattern)
                .setQueryPushDownEnabled(false)
                .setSocketTimeout(100);
        assertFullMapping(properties, expected);
    }

    /**
     * This is testing ClickHouseConfig class inner functions
     */
    @Test
    public void testGetFuncions()
    {
        ClickHouseConfig config = new ClickHouseConfig();

        String tableTypes = config.getTableTypes();
        assertEquals(tableTypes, ClickHouseConstants.DEFAULT_TABLE_TYPES);

        String schemaPattern = config.getSchemaPattern();
        assertEquals(schemaPattern, null);

        boolean isQueryPushDown = config.isQueryPushDownEnabled();
        assertEquals(isQueryPushDown, true);
    }
}
