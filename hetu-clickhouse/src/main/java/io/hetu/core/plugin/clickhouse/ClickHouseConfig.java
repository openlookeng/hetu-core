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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Locale;

/**
 * To get the custom properties to connect to the database.
 */
public class ClickHouseConfig
{
    private static final int DEFAULT_SOCKET_TIMEOUT = 120000;
    private int socketTimeout = DEFAULT_SOCKET_TIMEOUT;
    private String tableTypes = ClickHouseConstants.DEFAULT_TABLE_TYPES;
    private String schemaPattern;
    private boolean isQueryPushDownEnabled = true;
    private String clickHouseSqlVersion = "DEFAULT";

    public int getSocketTimeout()
    {
        return socketTimeout;
    }

    @Config("clickhouse.socket_timeout")
    @ConfigDescription("Connection ClickHouse socket timeout ")
    public ClickHouseConfig setSocketTimeout(int socketTimeout)
    {
        this.socketTimeout = socketTimeout;
        return this;
    }

    @Config("clickhouse.query.pushdown.enabled")
    @ConfigDescription("Enable sub-query push down to clickhouse. It's set by default")
    public ClickHouseConfig setQueryPushDownEnabled(boolean isQueryPushDownEnabledParameter)
    {
        this.isQueryPushDownEnabled = isQueryPushDownEnabledParameter;
        return this;
    }

    public boolean isQueryPushDownEnabled()
    {
        return this.isQueryPushDownEnabled;
    }

    public String getTableTypes()
    {
        return this.tableTypes;
    }

    public String getSchemaPattern()
    {
        return schemaPattern;
    }

    /**
     * setTableTypes
     *
     * @param tableTypes the table types to set
     * @return ClickHouseConfig
     */
    @Config("clickhouse.table-types")
    public ClickHouseConfig setTableTypes(String tableTypes)
    {
        this.tableTypes = tableTypes.toUpperCase(Locale.ENGLISH);
        return this;
    }

    public String getClickHouseSqlVersion()
    {
        return this.clickHouseSqlVersion;
    }

    /**
     * setSchemaPattern
     *
     * @param schemaPattern the schema pattern to set
     * @return ClickHouseConfig
     */
    @Config("clickhouse.schema-pattern")
    public ClickHouseConfig setSchemaPattern(String schemaPattern)
    {
        this.schemaPattern = schemaPattern;
        return this;
    }
}
