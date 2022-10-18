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
package io.hetu.core.plugin.iceberg;

import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.type.TypeId;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class IcebergMaterializedViewDefinitionTest
{
    private IcebergMaterializedViewDefinition icebergMaterializedViewDefinitionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        icebergMaterializedViewDefinitionUnderTest = new IcebergMaterializedViewDefinition("originalSql",
                Optional.of("value"), Optional.of("value"),
                Arrays.asList(new IcebergMaterializedViewDefinition.Column("name", TypeId.of("id"))),
                Optional.of("value"));
    }

    @Test
    public void testToString()
    {
        // Setup
        // Run the test
        final String result = icebergMaterializedViewDefinitionUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testEncodeMaterializedViewData()
    {
        // Setup
        final IcebergMaterializedViewDefinition definition = new IcebergMaterializedViewDefinition("originalSql",
                Optional.of("value"), Optional.of("value"),
                Arrays.asList(new IcebergMaterializedViewDefinition.Column("name", TypeId.of("id"))),
                Optional.of("value"));

        // Run the test
        final String result = IcebergMaterializedViewDefinition.encodeMaterializedViewData(definition);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testDecodeMaterializedViewData()
    {
        // Run the test
        final IcebergMaterializedViewDefinition result = IcebergMaterializedViewDefinition.decodeMaterializedViewData(
                "data");
        assertEquals("originalSql", result.getOriginalSql());
        assertEquals(Optional.of("value"), result.getCatalog());
        assertEquals(Optional.of("value"), result.getSchema());
        assertEquals(Arrays.asList(new IcebergMaterializedViewDefinition.Column("name", TypeId.of("id"))),
                result.getColumns());
        assertEquals(Optional.of("value"), result.getComment());
        assertEquals("result", result.toString());
    }

    @Test
    public void testFromConnectorMaterializedViewDefinition()
    {
        // Setup
        final ConnectorMaterializedViewDefinition definition = new ConnectorMaterializedViewDefinition("originalSql",
                Optional.of(new CatalogSchemaTableName("catalogName", "schemaName", "tableName")), Optional.of("value"),
                Optional.of("value"),
                Arrays.asList(new ConnectorMaterializedViewDefinition.Column("name", TypeId.of("id"))),
                Optional.of("value"), Optional.of("value"), new HashMap<>());

        // Run the test
        final IcebergMaterializedViewDefinition result = IcebergMaterializedViewDefinition.fromConnectorMaterializedViewDefinition(
                definition);
        assertEquals("originalSql", result.getOriginalSql());
        assertEquals(Optional.of("value"), result.getCatalog());
        assertEquals(Optional.of("value"), result.getSchema());
        assertEquals(Arrays.asList(new IcebergMaterializedViewDefinition.Column("name", TypeId.of("id"))),
                result.getColumns());
        assertEquals(Optional.of("value"), result.getComment());
        assertEquals("result", result.toString());
    }
}
