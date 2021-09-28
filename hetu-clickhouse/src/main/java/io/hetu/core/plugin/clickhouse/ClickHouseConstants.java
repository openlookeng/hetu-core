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

public class ClickHouseConstants
{
    public static final String CLICKHOUSE_JDBC_DRIVER_CLASS_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

    public static final int DEAFULT_STRINGBUFFER_CAPACITY = 30;

    /**
     * default table type list for clickhouse
     */
    public static final String DEFAULT_TABLE_TYPES = "TABLE,VIEW";

    public static final String CONNECTOR_NAME = "ClickHouse";

    private ClickHouseConstants()
    {
    }
}
