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
import io.prestosql.spi.function.ExternalFunctionInfo;
import io.prestosql.spi.function.RoutineCharacteristics;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Set;

import static io.prestosql.sql.builder.functioncall.TestingJdbcExternalFunctionHub.EXTERNAL_FUNCTION_INFO;
import static org.testng.Assert.assertEquals;

public class TestJdbcExternalFunctionHub
{
    private JdbcExternalFunctionHub jdbcExternalFunctionHub;

    @BeforeClass
    public void setup()
    {
        jdbcExternalFunctionHub = new TestingJdbcExternalFunctionHub();
    }

    @Test
    public void testGetExternalFunctions()
    {
        Set<ExternalFunctionInfo> set = jdbcExternalFunctionHub.getExternalFunctions();
        assertEquals(set.size(), 1);
        assertEquals(set.iterator().next(), EXTERNAL_FUNCTION_INFO);
    }

    @Test
    public void testGetExternalFunctionLanguage()
    {
        assertEquals(jdbcExternalFunctionHub.getExternalFunctionLanguage(), RoutineCharacteristics.Language.JDBC);
    }

    @Test
    public void testGetExternalFunctionCatalogSchemaName()
    {
        CatalogSchemaName catalogSchemaName = new CatalogSchemaName("jdbc", "foo");
        assertEquals(jdbcExternalFunctionHub.getExternalFunctionCatalogSchemaName().get(), catalogSchemaName);
    }
}
