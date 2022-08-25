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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class QualifiedObjectNameTest
{
    private QualifiedObjectName qualifiedObjectNameUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        qualifiedObjectNameUnderTest = new QualifiedObjectName("catalogName", "schemaName", "objectName");
    }

    @Test
    public void testGetCatalogSchemaName() throws Exception
    {
        // Setup
        final CatalogSchemaName expectedResult = new CatalogSchemaName("catalogName", "schemaName");

        // Run the test
        final CatalogSchemaName result = qualifiedObjectNameUnderTest.getCatalogSchemaName();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testAsSchemaTableName() throws Exception
    {
        // Setup
        final SchemaTableName expectedResult = new SchemaTableName("schemaName", "objectName");

        // Run the test
        final SchemaTableName result = qualifiedObjectNameUnderTest.asSchemaTableName();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(qualifiedObjectNameUnderTest.equals("obj"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, qualifiedObjectNameUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", qualifiedObjectNameUnderTest.toString());
    }

    @Test
    public void testValueOf1()
    {
        // Run the test
        final QualifiedObjectName result = QualifiedObjectName.valueOf("name");
        assertEquals(new CatalogSchemaName("catalogName", "schemaName"), result.getCatalogSchemaName());
        assertEquals(new SchemaTableName("schemaName", "objectName"), result.asSchemaTableName());
        assertEquals("catalogName", result.getCatalogName());
        assertEquals("schemaName", result.getSchemaName());
        assertEquals("objectName", result.getObjectName());
        assertTrue(result.equals("obj"));
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
    }

    @Test
    public void testValueOfDefaultFunction()
    {
        // Run the test
        final QualifiedObjectName result = QualifiedObjectName.valueOfDefaultFunction("objectName");
        assertEquals(new CatalogSchemaName("catalogName", "schemaName"), result.getCatalogSchemaName());
        assertEquals(new SchemaTableName("schemaName", "objectName"), result.asSchemaTableName());
        assertEquals("catalogName", result.getCatalogName());
        assertEquals("schemaName", result.getSchemaName());
        assertEquals("objectName", result.getObjectName());
        assertTrue(result.equals("obj"));
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
    }

    @Test
    public void testValueOf2() throws Exception
    {
        // Setup
        final CatalogSchemaName catalogSchemaName = new CatalogSchemaName("catalogName", "schemaName");

        // Run the test
        final QualifiedObjectName result = QualifiedObjectName.valueOf(catalogSchemaName, "objectName");
        assertEquals(new CatalogSchemaName("catalogName", "schemaName"), result.getCatalogSchemaName());
        assertEquals(new SchemaTableName("schemaName", "objectName"), result.asSchemaTableName());
        assertEquals("catalogName", result.getCatalogName());
        assertEquals("schemaName", result.getSchemaName());
        assertEquals("objectName", result.getObjectName());
        assertTrue(result.equals("obj"));
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
    }

    @Test
    public void testValueOf3()
    {
        // Run the test
        final QualifiedObjectName result = QualifiedObjectName.valueOf("catalogName", "schemaName", "objectName");
        assertEquals(new CatalogSchemaName("catalogName", "schemaName"), result.getCatalogSchemaName());
        assertEquals(new SchemaTableName("schemaName", "objectName"), result.asSchemaTableName());
        assertEquals("catalogName", result.getCatalogName());
        assertEquals("schemaName", result.getSchemaName());
        assertEquals("objectName", result.getObjectName());
        assertTrue(result.equals("obj"));
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
    }
}
