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
package io.prestosql.spi.connector;

import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class ConnectorViewDefinitionTest
{
    private ConnectorViewDefinition connectorViewDefinitionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        connectorViewDefinitionUnderTest = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"),
                Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))),
                Optional.of("value"), false);
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = connectorViewDefinitionUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testWithoutOwner() throws Exception
    {
        // Setup
        // Run the test
        final ConnectorViewDefinition result = connectorViewDefinitionUnderTest.withoutOwner();

        // Verify the results
    }
}
