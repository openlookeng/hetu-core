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
package io.prestosql.spi.function;

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.testng.annotations.BeforeMethod;

import java.util.Arrays;

public class DynamicSqlScalarFunctionTest
{
    private DynamicSqlScalarFunction dynamicSqlScalarFunctionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        dynamicSqlScalarFunctionUnderTest = new DynamicSqlScalarFunction(
                new Signature(new QualifiedObjectName("catalogName", "schemaName", "objectName"), FunctionKind.SCALAR,
                        Arrays.asList(new TypeVariableConstraint("name", false, false, "variadicBound")),
                        Arrays.asList(new LongVariableConstraint("name", "expression")),
                        new TypeSignature("base", TypeSignatureParameter.of(0L)),
                        Arrays.asList(new TypeSignature("base", TypeSignatureParameter.of(0L))), false)) {
            @Override
            public BuiltInScalarFunctionImplementation specialize()
            {
                return null;
            }

            @Override
            public boolean isHidden()
            {
                return false;
            }

            @Override
            public boolean isDeterministic()
            {
                return false;
            }

            @Override
            public boolean isCalledOnNullInput()
            {
                return false;
            }

            @Override
            public String getDescription()
            {
                return null;
            }
        };
    }
}
