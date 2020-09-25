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
package io.prestosql.sql.builder.functioncall.config;

import io.prestosql.configmanager.DefaultUdfRewriteConfigSupplier;
import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;
import io.prestosql.sql.builder.functioncall.functions.config.DefaultConnectorConfigFunctionRewriter;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.prestosql.sql.builder.functioncall.UdfPropertiesConstants.Test_UDF_REWRITE_PATTERNS;
import static org.testng.Assert.assertEquals;

public class TestDefaultConnectorConfigFunctionRewriter
{
    @Test
    public void testDefaultConnectorConfigFunctionRewriter()
    {
        List<String> functionName = new ArrayList<>();
        functionName.add("CORR");
        List<String> argsList = new ArrayList<>();
        argsList.add("var1");
        argsList.add("var2");
        FunctionCallArgsPackage functionCallArgsPackage =
                new FunctionCallArgsPackage(new QualifiedName(functionName), false, argsList,
                        Optional.empty(), Optional.empty(), Optional.empty());
        String connectorName = "jdbc_connector";
        DefaultUdfRewriteConfigSupplier defaultUdfRewriteConfigSupplier = new DefaultUdfRewriteConfigSupplier(Test_UDF_REWRITE_PATTERNS);
        DefaultConnectorConfigFunctionRewriter defaultConnectorConfigFunctionRewriter = new DefaultConnectorConfigFunctionRewriter(connectorName, defaultUdfRewriteConfigSupplier);
        assertEquals(defaultConnectorConfigFunctionRewriter.rewriteFunctionCall(functionCallArgsPackage), "CORR(var1, var2)");
    }
}
