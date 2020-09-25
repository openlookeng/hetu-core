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
package io.hetu.core.plugin.hana;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static org.testng.Assert.assertEquals;

/**
 * This is testing HanaConfig functions
 *
 * @since 2019-07-11
 */
public class TestHanaConfig
{
    private static final int COMMUNICATION_TIMEOUT = 10;

    private static final int PACKET_SIZE = 150000;

    /**
     * This is testing HanaPropertyMappings
     *
     */
    @Test
    public void testHanaPropertyMappings()
    {
        final String tableTypes = "TABLE,VIEW,USER DEFINED,SYNONYM,OLAP VIEW,JOIN VIEW,HIERARCHY VIEW" + ",CALC VIEW,SYSTEM TABLE,NO LOGGING TEMPORARY,GLOBAL TEMPORARY";
        final String schemaPattern = "information_schema";

        Map<String, String> properties = new ImmutableMap.Builder<String, String>().put("hana.auto-commit", "false")
                .put("hana.communication-timeout", Integer.toString(COMMUNICATION_TIMEOUT))
                .put("hana.encrypt", "true")
                .put("hana.packet-size", Integer.toString(PACKET_SIZE))
                .put("hana.read-only", "true")
                .put("hana.reconnect", "false")
                .put("hana.table-types", tableTypes)
                .put("hana.schema-pattern", schemaPattern)
                .put("hana.query.pushdown.enabled", "false")
                .put("hana.query.bypassDataSourceTimeZone", "false")
                .put("hana.query.datasourceTimeZoneKey", "America/Chicago")
                .build();

        HanaConfig expected = new HanaConfig().setAutoCommit(false)
                .setCommunicationTimeout(COMMUNICATION_TIMEOUT)
                .setEncrypt(true)
                .setPacketSize(PACKET_SIZE)
                .setReadOnly(true)
                .setReconnect(false)
                .setTableTypes(tableTypes)
                .setSchemaPattern(schemaPattern)
                .setQueryPushDownEnabled(false)
                .setByPassDataSourceTimeZone(false)
                .setDataSourceTimeZoneKey("America/Chicago");
        assertFullMapping(properties, expected);
    }

    /**
     * This is testing HanaConfig class inner functions
     *
     */
    @Test
    public void testGetFuncions()
    {
        HanaConfig config = new HanaConfig();

        boolean isAutoCommit = config.isAutoCommit();
        assertEquals(isAutoCommit, true);

        int communicationTimeout = config.getCommunicationTimeout();
        assertEquals(communicationTimeout, HanaConstants.DEFAULT_COMMUNICATION_TIMEOUT);

        boolean isEncrypt = config.isEncrypt();
        assertEquals(isEncrypt, false);

        int packetSize = config.getPacketSize();
        assertEquals(packetSize, HanaConstants.DEFAULT_PACKET_SIZE);

        boolean isReadOnly = config.isReadOnly();
        assertEquals(isReadOnly, false);

        boolean isReconnect = config.isReconnect();
        assertEquals(isReconnect, true);

        String tableTypes = config.getTableTypes();
        assertEquals(tableTypes, HanaConstants.DEFAULT_TABLE_TYPES);

        String schemaPattern = config.getSchemaPattern();
        assertEquals(schemaPattern, null);

        boolean isQueryPushDown = config.isQueryPushDownEnabled();
        assertEquals(isQueryPushDown, true);
    }
}
