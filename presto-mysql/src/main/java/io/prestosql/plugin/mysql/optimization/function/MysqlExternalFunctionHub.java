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
package io.prestosql.plugin.mysql.optimization.function;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.function.ExternalFunctionInfo;
import io.prestosql.sql.builder.functioncall.JdbcExternalFunctionHub;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class MysqlExternalFunctionHub
        extends JdbcExternalFunctionHub
{
    private final BaseJdbcConfig jdbcConfig;

    @Inject
    public MysqlExternalFunctionHub(BaseJdbcConfig jdbcConfig)
    {
        this.jdbcConfig = requireNonNull(jdbcConfig, "jdbcConfig is null");
    }

    @Override
    public Optional<CatalogSchemaName> getExternalFunctionCatalogSchemaName()
    {
        return jdbcConfig.getConnectorRegistryFunctionNamespace();
    }

    public Set<ExternalFunctionInfo> getExternalFunctions()
    {
        return ImmutableSet.<ExternalFunctionInfo>builder()
                .addAll(MysqlExternalMathFunctions.getFunctionsInfo())
                .addAll(MysqlExternalStringFunctions.getFunctionsInfo())
                .addAll(MysqlExternalDateTimeFunctions.getFunctionsInfo())
                .build();
    }
}
