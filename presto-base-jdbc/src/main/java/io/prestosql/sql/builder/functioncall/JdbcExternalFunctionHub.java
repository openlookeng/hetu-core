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
package io.prestosql.sql.builder.functioncall;

import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.function.ExternalFunctionHub;
import io.prestosql.spi.function.ExternalFunctionInfo;
import io.prestosql.spi.function.RoutineCharacteristics;

import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptySet;

public class JdbcExternalFunctionHub
        implements ExternalFunctionHub
{
    @Override
    public Set<ExternalFunctionInfo> getExternalFunctions()
    {
        return emptySet();
    }

    @Override
    public final RoutineCharacteristics.Language getExternalFunctionLanguage()
    {
        return RoutineCharacteristics.Language.JDBC;
    }

    @Override
    public Optional<CatalogSchemaName> getExternalFunctionCatalogSchemaName()
    {
        return Optional.empty();
    }
}
