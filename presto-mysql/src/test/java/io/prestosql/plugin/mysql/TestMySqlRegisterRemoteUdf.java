/*
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
package io.prestosql.plugin.mysql;

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.mysql.optimization.function.MysqlExternalFunctionHub;
import io.prestosql.spi.function.ExternalFunctionInfo;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMySqlRegisterRemoteUdf
{
    @Test
    public void testDefaults()
    {
        MysqlExternalFunctionHub externalFunctionHub = new MysqlExternalFunctionHub(new BaseJdbcConfig());
        Set<ExternalFunctionInfo> externalFunctionInfo = externalFunctionHub.getExternalFunctions();
        assertTrue(externalFunctionInfo.size() > 0);

        List<ExternalFunctionInfo> functionList = externalFunctionInfo.stream()
                .filter(x -> x.getFunctionName().get().equals("lower"))
                .collect(Collectors.toList());
        assertEquals(functionList.size(), 1);

        ExternalFunctionInfo functionInfo = functionList.get(0);
        assertEquals(functionInfo.getInputArgs().get(0), StandardTypes.VARCHAR);
        assertEquals(functionInfo.getReturnType().get(), StandardTypes.VARCHAR);
        assertTrue(functionInfo.isDeterministic());
        assertFalse(functionInfo.isCalledOnNullInput());
    }

    // test over load function register
    @Test
    public void testOverloadFunctions()
    {
        MysqlExternalFunctionHub externalFunctionHub = new MysqlExternalFunctionHub(new BaseJdbcConfig());
        Set<ExternalFunctionInfo> externalFunctionInfo = externalFunctionHub.getExternalFunctions();
        assertTrue(externalFunctionInfo.size() > 0);

        List<ExternalFunctionInfo> functionList = externalFunctionInfo.stream()
                .filter(x -> x.getFunctionName().get().equals("timestamp"))
                .collect(Collectors.toList());
        assertEquals(functionList.size(), 1);

        ExternalFunctionInfo functionInfo = functionList.get(0);
        List<String> inputArgs = functionInfo.getInputArgs();
        assertEquals(inputArgs.size(), 2);
        assertEquals(inputArgs.get(0), StandardTypes.TIMESTAMP);
        assertEquals(inputArgs.get(1), StandardTypes.TIME);
        assertEquals(functionInfo.getReturnType().get(), StandardTypes.TIMESTAMP);
        assertTrue(functionInfo.isDeterministic());
        assertFalse(functionInfo.isCalledOnNullInput());
    }

    // test func defination with more than one params
    @Test
    public void testMultiParamsFunctions()
    {
        MysqlExternalFunctionHub externalFunctionHub = new MysqlExternalFunctionHub(new BaseJdbcConfig());
        Set<ExternalFunctionInfo> externalFunctionInfo = externalFunctionHub.getExternalFunctions();
        assertTrue(externalFunctionInfo.size() > 0);

        List<ExternalFunctionInfo> functionList = externalFunctionInfo.stream()
                .filter(x -> x.getFunctionName().get().equals("abs"))
                .collect(Collectors.toList());
        assertEquals(functionList.size(), 2);
    }
}
