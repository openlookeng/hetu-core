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
package io.prestosql.plugin.hive;

import io.prestosql.plugin.hive.util.FieldSetterFactoryTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.weakref.jmx.ObjectNameGenerator;

import java.util.HashMap;

public class ConnectorObjectNameGeneratorModuleTest
{
    private ConnectorObjectNameGeneratorModule connectorObjectNameGeneratorModuleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        connectorObjectNameGeneratorModuleUnderTest = new ConnectorObjectNameGeneratorModule("catalogName",
                "packageName", "defaultDomainBase");
    }

    @Test
    public void testCreatePrefixObjectNameGenerator()
    {
        // Setup
        final ConnectorObjectNameGeneratorModule.ConnectorObjectNameGeneratorConfig config = new ConnectorObjectNameGeneratorModule.ConnectorObjectNameGeneratorConfig();
        config.setDomainBase("domainBase");

        // Run the test
        final ObjectNameGenerator result = connectorObjectNameGeneratorModuleUnderTest.createPrefixObjectNameGenerator(config);

        // Verify the results
    }

    @Test
    public void testConnectorObjectNameGenerator()
    {
        ConnectorObjectNameGeneratorModule.ConnectorObjectNameGenerator connectorObjectNameGenerator = new ConnectorObjectNameGeneratorModule.ConnectorObjectNameGenerator("domainBase", "catalogName");
        connectorObjectNameGenerator.generatedNameOf(FieldSetterFactoryTest.Type.class, new HashMap<>());
    }
}
