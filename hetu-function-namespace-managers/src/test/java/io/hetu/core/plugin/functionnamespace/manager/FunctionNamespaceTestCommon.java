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
package io.hetu.core.plugin.functionnamespace.manager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.hetu.core.plugin.functionnamespace.AbstractSqlInvokedFunctionNamespaceManager;
import io.hetu.core.plugin.functionnamespace.memory.InMemoryFunctionNamespaceManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.Parameter;
import io.prestosql.spi.function.RoutineCharacteristics;
import io.prestosql.spi.function.SqlInvokedFunction;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static io.prestosql.spi.function.RoutineCharacteristics.Language.JDBC;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class FunctionNamespaceTestCommon
{
    private String catalogName;
    private String funcNsSchemaName;
    private String functionName;

    public FunctionNamespaceTestCommon(String catalogName, String funcNsSchemaName, String functionName)
    {
        this.catalogName = catalogName;
        this.funcNsSchemaName = funcNsSchemaName;
        this.functionName = functionName;
    }

    /**
     * create function
     */
    public void createFunction(AbstractSqlInvokedFunctionNamespaceManager functionNamespaceManager, String functionName, List<Parameter> parameters)
    {
        QualifiedObjectName powerTower = QualifiedObjectName.valueOf(new CatalogSchemaName(this.catalogName, funcNsSchemaName), functionName);
        SqlInvokedFunction function = new SqlInvokedFunction(
                powerTower,
                parameters,
                parseTypeSignature(DOUBLE),
                "power tower test",
                RoutineCharacteristics.builder().setDeterminism(DETERMINISTIC).setLanguage(JDBC).build(),
                "RETURN pow(x, x)",
                ImmutableMap.of("executor", "mysql"),
                Optional.empty());

        try {
            functionNamespaceManager.createFunction(function, false);
        }
        catch (PrestoException e) {
            fail(format("Create function(id=%s) failed.", function.getFunctionId().toString()));
        }
    }

    /**
     * test get function
     */
    public void testGetFunction(AbstractSqlInvokedFunctionNamespaceManager functionNamespaceManager)
    {
        QualifiedObjectName powerTower = QualifiedObjectName.valueOf(new CatalogSchemaName(this.catalogName, funcNsSchemaName), functionName);
        List<Parameter> parameters = ImmutableList.of(new Parameter("x", parseTypeSignature(DOUBLE)));
        List<Parameter> parameters1 = ImmutableList.of(new Parameter("y", parseTypeSignature(INTEGER)), new Parameter("x", parseTypeSignature(DOUBLE)));
        String funcDesc = "power tower test";
        String funcBody = "RETURN pow(x, x)";
        Map<String, String> funcProperties = ImmutableMap.of("executor", "mysql");

        SqlInvokedFunction function = new SqlInvokedFunction(
                powerTower,
                parameters,
                parseTypeSignature(DOUBLE),
                funcDesc,
                RoutineCharacteristics.builder().setDeterminism(DETERMINISTIC).setLanguage(JDBC).build(),
                funcBody,
                funcProperties,
                Optional.empty());
        SqlInvokedFunction function1 = new SqlInvokedFunction(
                powerTower,
                parameters1,
                parseTypeSignature(DOUBLE),
                funcDesc,
                RoutineCharacteristics.builder().setDeterminism(DETERMINISTIC).setLanguage(JDBC).build(),
                funcBody,
                funcProperties,
                Optional.empty());

        try {
            functionNamespaceManager.createFunction(function, false);
            functionNamespaceManager.createFunction(function1, false);
        }
        catch (PrestoException e) {
            fail(format("Create function(id=%s) failed.", function.getFunctionId().toString()));
        }

        if ((functionNamespaceManager instanceof InMemoryFunctionNamespaceManager)) {
            // trans to InMemoryFunctionNamespaceManager obj for sub classes method test
            InMemoryFunctionNamespaceManager manager = (InMemoryFunctionNamespaceManager) functionNamespaceManager;
            List<SqlInvokedFunction> storedFunc = manager.listFunctions();
            assertTrue(storedFunc.size() == 2);

            storedFunc = manager.fetchFunctionsDirect(powerTower);
            assertTrue(storedFunc.size() == 2);

            Optional<SqlInvokedFunction> singleFunc = manager.fetchFunctionsDirect(powerTower,
                    parameters.stream().map(x -> x.getType()).collect(Collectors.toList()));
            assertTrue(singleFunc.isPresent());

            FunctionMetadata funcMeta = manager.fetchFunctionMetadataDirect(singleFunc.get().getFunctionHandle().get());
            assertEquals(funcMeta.getName(), powerTower);
        }
    }
}
