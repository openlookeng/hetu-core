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

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.function.ExternalFunctionInfo;
import io.prestosql.spi.type.StandardTypes;

import java.util.Optional;
import java.util.Set;

public class TestingJdbcExternalFunctionHub
        extends JdbcExternalFunctionHub
{
    private CatalogSchemaName catalogSchemaName = new CatalogSchemaName("jdbc", "foo");

    public Set<ExternalFunctionInfo> getExternalFunctions()
    {
        return ImmutableSet.<ExternalFunctionInfo>builder().add(EXTERNAL_FUNCTION_INFO).build();
    }

    @Override
    public Optional<CatalogSchemaName> getExternalFunctionCatalogSchemaName()
    {
        return Optional.of(catalogSchemaName);
    }

    protected static final ExternalFunctionInfo EXTERNAL_FUNCTION_INFO =
            ExternalFunctionInfo
                    .builder()
                    .functionName("foo")
                    .calledOnNullInput(true)
                    .description("foo-fun")
                    .deterministic(true)
                    .inputArgs(StandardTypes.DOUBLE)
                    .isHidden(false)
                    .returnType(StandardTypes.DOUBLE)
                    .build();
}
