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

/**
 * HanaConstants class
 *
 * @since 2019-07-10
 */
public class HanaConstants
{
    /**
     * default packet size for hana connection
     */
    public static final int DEFAULT_PACKET_SIZE = 130000;

    /**
     * default communication timeout for hana connection
     */
    public static final int DEFAULT_COMMUNICATION_TIMEOUT = 0;

    /**
     * default table type list for hana
     */
    public static final String DEFAULT_TABLE_TYPES = "TABLE,VIEW,USER DEFINED,SYNONYM,OLAP VIEW,JOIN VIEW,HIERARCHY VIEW,CALC VIEW";

    /**
     * default String buffer's relate class init capactiy
     */
    public static final int DEAFULT_STRINGBUFFER_CAPACITY = 30;

    /**
     * this connector's name
     */
    public static final String CONNECTOR_NAME = "Hana";

    /**
     * sap hana jdbc driver class name
     */
    public static final String SAP_HANA_JDBC_DRIVER_CLASS_NAME = "com.sap.db.jdbc.Driver";

    private HanaConstants()
    {
    }
}
