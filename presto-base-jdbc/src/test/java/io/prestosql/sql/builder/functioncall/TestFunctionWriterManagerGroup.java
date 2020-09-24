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
package io.prestosql.sql.builder.functioncall;

import io.prestosql.configmanager.DefaultUdfRewriteConfigSupplier;
import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.sql.builder.functioncall.base.UnsupportedFunctionCallRewriterForUt;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;
import io.prestosql.sql.builder.functioncall.functions.config.DefaultConnectorConfigFunctionRewriter;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.sql.builder.functioncall.UdfPropertiesConstants.Test_UDF_REWRITE_PATTERNS;
import static org.testng.Assert.assertEquals;

public class TestFunctionWriterManagerGroup
{
    private static FunctionWriterManager functionWriterManager1;

    @BeforeTest
    public void setUp()
    {
        String connectorName = "jdbc_connector";
        String versionName = "default";
        Map<String, FunctionCallRewriter> functionCallRewriterMap = new HashMap<>();
        functionCallRewriterMap.put(VarbinaryLiteralFunctionCallRewriterForUt.INNER_FUNC_VARBINARY_LITERAL, new VarbinaryLiteralFunctionCallRewriterForUt());
        functionCallRewriterMap.put(UnsupportedFunctionCallRewriterForUt.UNSUPPORTED_FUNCTION_NAME_TEST, new UnsupportedFunctionCallRewriterForUt(connectorName));

        DefaultUdfRewriteConfigSupplier defaultUdfRewriteConfigSupplier = new DefaultUdfRewriteConfigSupplier(Test_UDF_REWRITE_PATTERNS);
        DefaultConnectorConfigFunctionRewriter defaultConnectorConfigFunctionRewriter = new DefaultConnectorConfigFunctionRewriter(connectorName, defaultUdfRewriteConfigSupplier);

        functionWriterManager1 = FunctionWriterManagerGroup.newFunctionWriterManagerInstance(connectorName,
                versionName, functionCallRewriterMap, defaultConnectorConfigFunctionRewriter);

        FunctionWriterManager functionWriterManager2 = FunctionWriterManagerGroup.newFunctionWriterManagerInstance(connectorName,
                versionName, functionCallRewriterMap, defaultConnectorConfigFunctionRewriter);

        assertEquals(functionWriterManager1.toString(), functionWriterManager2.toString());
    }

    @Test
    public void testFunctionWriterManagerSupportFunctions()
    {
        String functionName = "LOG10";
        List<String> functionList = new ArrayList<>();
        functionList.add(functionName);
        List<String> argsList = new ArrayList<>();
        argsList.add("var1");
        assertEquals(functionWriterManager1.getFunctionRewriteResult(
                new QualifiedName(functionList), false, argsList, Optional.empty(), Optional.empty(), Optional.empty()), "LOG(10, var1)");

        functionName = "$literal$varbinary";
        functionList.clear();
        functionList.add(functionName);
        argsList.clear();
        argsList.add("1232");
        assertEquals(functionWriterManager1.getFunctionRewriteResult(
                new QualifiedName(functionList), false, argsList, Optional.empty(), Optional.empty(), Optional.empty()), "X'1232'");

        functionName = "CORR";
        functionList.clear();
        functionList.add(functionName);
        argsList.clear();
        argsList.add("var1");
        argsList.add("var2");
        assertEquals(functionWriterManager1.getFunctionRewriteResult(
                new QualifiedName(functionList), false, argsList, Optional.empty(), Optional.empty(), Optional.empty()), "CORR(var1, var2)");

        functionName = "LOG10";
        functionList.clear();
        functionList.add(functionName);
        argsList.clear();
        argsList.add("var1");
        assertEquals(functionWriterManager1.getFunctionRewriteResult(
                new QualifiedName(functionList), false, argsList, Optional.empty(), Optional.empty(), Optional.empty()), "LOG(10, var1)");
    }

    @Test
    public void testFunctionWriterManagerUnsupportedFunctions()
    {
        String functionName = "LO10";
        List<String> functionList = new ArrayList<>();
        functionList.add(functionName);
        List<String> argsList = new ArrayList<>();
        argsList.add("var1");
        try {
            functionWriterManager1.getFunctionRewriteResult(
                    new QualifiedName(functionList), false, argsList, Optional.empty(), Optional.empty(), Optional.empty());
        }
        catch (UnsupportedOperationException exception) {
            assertEquals(exception.getMessage(), "jdbc_connector Connector does not support function call of LO10");
        }
    }
}
