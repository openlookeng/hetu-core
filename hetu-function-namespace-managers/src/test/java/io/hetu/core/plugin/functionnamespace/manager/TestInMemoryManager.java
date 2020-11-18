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
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.hetu.core.plugin.functionnamespace.memory.InMemoryFunctionNamespaceManager;
import io.hetu.core.plugin.functionnamespace.memory.InMemoryFunctionNamespaceManagerModule;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.Parameter;
import io.prestosql.spi.function.SqlInvokedFunction;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestInMemoryManager
{
    private String catalogName = "memory_test";
    private String funcNsSchemaName = "default";
    private String functionName = "test_function";
    private Injector injector;
    private InMemoryFunctionNamespaceManager functionNamespaceManager;
    private FunctionNamespaceTestCommon testProc;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new InMemoryFunctionNamespaceManagerModule(catalogName));

        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("supported-function-languages", "JDBC")
                .put("function-namespace-manager.name", "memory")
                .build();

        try {
            this.injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            functionNamespaceManager = injector.getInstance(InMemoryFunctionNamespaceManager.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }

        testProc = new FunctionNamespaceTestCommon(catalogName, funcNsSchemaName, functionName);
    }

    @AfterMethod
    public void cleanup()
    {
    }

    /**
     * test get function
     */
    @Test
    public void testCreateAndGetFunction()
    {
        QualifiedObjectName powerTower = QualifiedObjectName.valueOf(new CatalogSchemaName(this.catalogName, funcNsSchemaName), functionName);
        List<Parameter> parameters = ImmutableList.of(new Parameter("x", parseTypeSignature(DOUBLE)));
        List<Parameter> parameters1 = ImmutableList.of(new Parameter("y", parseTypeSignature(INTEGER)), new Parameter("x", parseTypeSignature(DOUBLE)));

        testProc.createFunction(functionNamespaceManager, functionName, parameters);
        testProc.createFunction(functionNamespaceManager, functionName, parameters1);

        List<SqlInvokedFunction> storedFunc = functionNamespaceManager.listFunctions();
        assertTrue(storedFunc.size() == 2);

        storedFunc = functionNamespaceManager.fetchFunctionsDirect(powerTower);
        assertTrue(storedFunc.size() == 2);

        Optional<SqlInvokedFunction> singleFunc = functionNamespaceManager.fetchFunctionsDirect(powerTower,
                parameters.stream().map(x -> x.getType()).collect(Collectors.toList()));
        assertTrue(singleFunc.isPresent());

        FunctionMetadata funcMeta = functionNamespaceManager.fetchFunctionMetadataDirect(singleFunc.get().getFunctionHandle().get());
        assertEquals(funcMeta.getName(), powerTower);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
    }
}
