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
package io.hetu.core.plugin.clickhouse.optimization.externalfunc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.function.ExternalFunctionInfo;
import io.prestosql.sql.builder.functioncall.JdbcExternalFunctionHub;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ClickHouseExternalFunctionHub
        extends JdbcExternalFunctionHub
{
    private final BaseJdbcConfig jdbcConfig;

    @Inject
    public ClickHouseExternalFunctionHub(BaseJdbcConfig jdbcConfig)
    {
        this.jdbcConfig = requireNonNull(jdbcConfig, "jdbcConfig is null");
    }

    @Override
    public Optional<CatalogSchemaName> getExternalFunctionCatalogSchemaName()
    {
        return jdbcConfig.getConnectorRegistryFunctionNamespace();
    }

    @Override
    public Set<ExternalFunctionInfo> getExternalFunctions()
    {
        return ImmutableSet.<ExternalFunctionInfo>builder()
                .addAll(ClickHouseExternalDateTimeFunctions.getFunctionsInfo())
                .build();
    }

    public static Map<String, String> getPrimalFunctionNameMap()
    {
        // The click house is case sensitive and presto only support the lower case function name,
        // we need to handle this function into lower and store its primal name.
        // The click house do not contain two different functions have the equal function name ignore case.
        // If a data base contain two different functions have the equal function name ignore case,
        // then we should register them into different function name space
        return ImmutableMap.<String, String>builder()
                .putAll(ClickHouseExternalDateTimeFunctions.getDateTimeHubFunctionStringMap())
                .build();
    }
}
