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
package io.hetu.core.plugin.clickhouse.optimization;

import io.hetu.core.plugin.clickhouse.ClickHouseConfig;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownParameter;
import io.prestosql.spi.function.StandardFunctionResolution;

/**
 * Push Down parameter module
 */
public class ClickHousePushDownParameter
        extends JdbcPushDownParameter
{
    private final ClickHouseConfig clickHouseConfig;

    public ClickHousePushDownParameter(String identifierQuote, boolean nameCaseInsensitive, JdbcPushDownModule pushDownModule, ClickHouseConfig clickHouseConfig, StandardFunctionResolution functionResolution)
    {
        super(identifierQuote, nameCaseInsensitive, pushDownModule, functionResolution);
        this.clickHouseConfig = clickHouseConfig;
    }

    public ClickHouseConfig getClickHouseConfig()
    {
        return clickHouseConfig;
    }
}
