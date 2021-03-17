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
package io.prestosql.spi.function;

import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ConnectorContext;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

public interface ExternalFunctionHub
{
    Set<ExternalFunctionInfo> getExternalFunctions();

    RoutineCharacteristics.Language getExternalFunctionLanguage();

    default void registerExternalFunctions(CatalogSchemaName catalogSchemaName, ExternalFunctionHub externalFunctionHub, ConnectorContext context)
    {
        requireNonNull(catalogSchemaName);
        requireNonNull(externalFunctionHub);
        requireNonNull(context);
        FunctionMetadataManager functionMetadataManager = context.getFunctionMetadataManager();
        Optional<BiFunction<ExternalFunctionHub, CatalogSchemaName, Set<SqlInvokedFunction>>> functionOptional = context.getExternalParserFunction();
        if (functionOptional.isPresent()) {
            for (SqlInvokedFunction sqlInvokedFunction : functionOptional.get().apply(externalFunctionHub, catalogSchemaName)) {
                functionMetadataManager.createFunction(sqlInvokedFunction, true);
            }
        }
    }
}
