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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Locale;

/**
 * To get the custom properties to connect to the database. User, password and URL is provided by de BaseJdbcClient is
 * not required. If there is another custom configuration it should be put in here.
 *
 * @since 2019-07-10
 */
public class HanaConfig
{
    private boolean isAutoCommit = true;

    private int communicationTimeout = HanaConstants.DEFAULT_COMMUNICATION_TIMEOUT;

    private boolean isEncrypt;

    private int packetSize = HanaConstants.DEFAULT_PACKET_SIZE;

    private boolean isReadOnly;

    private boolean isReconnect = true;

    /*
     * The table types in hana: TABLE, VIEW, USER DEFINED, SYNONYM, OLAP VIEW, JOIN VIEW, HIERARCHY VIEW, CALC VIEW,
     * SYSTEM TABLE, NO LOGGING TEMPORARY, GLOBAL TEMPORARY
     */
    private String tableTypes = HanaConstants.DEFAULT_TABLE_TYPES;

    private String schemaPattern;

    private boolean isQueryPushDownEnabled = true;

    private String hanaSqlVersion = "DEFAULT";

    private boolean isByPassDsTzSetting = true;

    private String dsTimeZoneKey = "";

    public boolean isAutoCommit()
    {
        return isAutoCommit;
    }

    /**
     * setAutoCommit
     *
     * @param isHanaAutoCommit the autoCommit to set
     * @return HanaConfig
     */
    @Config("hana.auto-commit")
    public HanaConfig setAutoCommit(boolean isHanaAutoCommit)
    {
        this.isAutoCommit = isHanaAutoCommit;
        return this;
    }

    public int getCommunicationTimeout()
    {
        return communicationTimeout;
    }

    /**
     * setCommunicationTimeout
     *
     * @param communicationTimeout the communicationTimeout to set
     * @return HanaConfig
     */
    @Config("hana.communication-timeout")
    public HanaConfig setCommunicationTimeout(int communicationTimeout)
    {
        this.communicationTimeout = communicationTimeout;
        return this;
    }

    public boolean isEncrypt()
    {
        return isEncrypt;
    }

    /**
     * setEncrypt
     *
     * @param isHanaEncrypt the encrypt to set
     * @return HanaConfig
     */
    @Config("hana.encrypt")
    public HanaConfig setEncrypt(boolean isHanaEncrypt)
    {
        this.isEncrypt = isHanaEncrypt;
        return this;
    }

    public int getPacketSize()
    {
        return packetSize;
    }

    /**
     * setPacketSize
     *
     * @param packetSize the packetSize to set
     * @return HanaConfig
     */
    @Config("hana.packet-size")
    public HanaConfig setPacketSize(int packetSize)
    {
        this.packetSize = packetSize;
        return this;
    }

    public boolean isReadOnly()
    {
        return isReadOnly;
    }

    /**
     * setReadOnly
     *
     * @param isHanaReadOnly the readOnly to set
     * @return HanaConfig
     */
    @Config("hana.read-only")
    public HanaConfig setReadOnly(boolean isHanaReadOnly)
    {
        this.isReadOnly = isHanaReadOnly;
        return this;
    }

    public boolean isReconnect()
    {
        return isReconnect;
    }

    /**
     * setReconnect
     *
     * @param isHanaReconnect the reconnect to set
     * @return HanaConfig
     */
    @Config("hana.reconnect")
    public HanaConfig setReconnect(boolean isHanaReconnect)
    {
        this.isReconnect = isHanaReconnect;
        return this;
    }

    public String getTableTypes()
    {
        return tableTypes;
    }

    /**
     * setTableTypes
     *
     * @param tableTypes the table types to set
     * @return HanaConfig
     */
    @Config("hana.table-types")
    public HanaConfig setTableTypes(String tableTypes)
    {
        this.tableTypes = tableTypes.toUpperCase(Locale.ENGLISH);
        return this;
    }

    public String getSchemaPattern()
    {
        return schemaPattern;
    }

    /**
     * setSchemaPattern
     *
     * @param schemaPattern the schema pattern to set
     * @return HanaConfig
     */
    @Config("hana.schema-pattern")
    public HanaConfig setSchemaPattern(String schemaPattern)
    {
        this.schemaPattern = schemaPattern;
        return this;
    }

    /**
     * set Query Push Down Enabled
     *
     * @param isQueryPushDownEnabledParameter config from properties
     * @return HanaConfig
     */
    @Config("hana.query.pushdown.enabled")
    @ConfigDescription("Enable sub-query push down to hana. It's set by default")
    public HanaConfig setQueryPushDownEnabled(boolean isQueryPushDownEnabledParameter)
    {
        this.isQueryPushDownEnabled = isQueryPushDownEnabledParameter;
        return this;
    }

    public boolean isQueryPushDownEnabled()
    {
        return isQueryPushDownEnabled;
    }

    /**
     * get the Sql rewrite configuration properties file path default set to be resource file path
     *
     * @return String the usable file path
     */
    public String getHanaSqlVersion()
    {
        return this.hanaSqlVersion;
    }

    /**
     * Is ignore data source's time zone,just use onquery's timezone
     *
     * @param isByPassTz is bypass the hana time zone setting
     * @return HanaConfig
     */
    @Config("hana.query.bypassDataSourceTimeZone")
    @ConfigDescription("ignore datasource's timezone setting, and do not do convert")
    public HanaConfig setByPassDataSourceTimeZone(boolean isByPassTz)
    {
        this.isByPassDsTzSetting = isByPassTz;
        return this;
    }

    /**
     * is bypass the data source's timezone setting.
     *
     * @return boolean
     */
    public boolean isByPassDataSourceTimeZone()
    {
        return isByPassDsTzSetting;
    }

    /**
     * set data source's time zone key
     *
     * @param timeZoneKey time zone key
     * @return HanaConfig
     */
    @Config("hana.query.datasourceTimeZoneKey")
    @ConfigDescription("ignore data source's timezone setting, and do not do convert")
    public HanaConfig setDataSourceTimeZoneKey(String timeZoneKey)
    {
        dsTimeZoneKey = timeZoneKey;
        return this;
    }

    /**
     * get datasource's time zone
     *
     * @return time zone key
     */
    public String getDataSourceTimeZoneKey()
    {
        if (dsTimeZoneKey == null || dsTimeZoneKey.isEmpty()) {
            dsTimeZoneKey = "GMT+00:00";
        }
        return dsTimeZoneKey;
    }
}
