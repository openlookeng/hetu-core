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
package io.prestosql.metadata;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class TestFunctionSchema
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
    }

    @Test
    public void testFunctionSchema()
    {
        assertFunction("hive.bicoredata.CONCAT('hello', ' world')", VARCHAR, "hello world");
        assertFunction("bicoredata.CONCAT('', '')", VARCHAR, "");
        assertFunction("test123.bicoredata.CONCAT('what', '')", VARCHAR, "what");
        assertFunction("biads.CONCAT('', 'what')", VARCHAR, "what");
        assertFunction("mysql.bicoredata.CONCAT(CONCAT('this', ' is'), ' cool')", VARCHAR, "this is cool");
        assertFunction("postgress.bicoredata.CONCAT('this', CONCAT(' is', ' cool'))", VARCHAR, "this is cool");
    }
}
