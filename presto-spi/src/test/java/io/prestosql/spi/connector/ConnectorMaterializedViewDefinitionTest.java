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

import io.prestosql.spi.type.TypeId;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ConnectorMaterializedViewDefinitionTest
{
    private ConnectorMaterializedViewDefinition connectorMaterializedViewDefinitionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        connectorMaterializedViewDefinitionUnderTest = new ConnectorMaterializedViewDefinition("originalSql",
                Optional.of(new CatalogSchemaTableName("catalogName", "schemaName", "tableName")),
                Optional.of("value"), Optional.of("value"),
                Arrays.asList(new ConnectorMaterializedViewDefinition.Column("name", TypeId.of("id"))),
                Optional.of("value"), Optional.of("value"), new HashMap<>());
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = connectorMaterializedViewDefinitionUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(connectorMaterializedViewDefinitionUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, connectorMaterializedViewDefinitionUnderTest.hashCode());
    }
}
