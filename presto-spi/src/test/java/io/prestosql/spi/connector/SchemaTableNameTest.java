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

public class SchemaTableNameTest
{
    private SchemaTableName schemaTableNameUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        schemaTableNameUnderTest = new SchemaTableName("schemaName", "tableName");
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, schemaTableNameUnderTest.hashCode());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(schemaTableNameUnderTest.equals("obj"));
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", schemaTableNameUnderTest.toString());
    }

    @Test
    public void testToSchemaTablePrefix() throws Exception
    {
        // Setup
        final SchemaTablePrefix expectedResult = new SchemaTablePrefix("schemaName", "tableName");

        // Run the test
        final SchemaTablePrefix result = schemaTableNameUnderTest.toSchemaTablePrefix();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testSchemaTableName() throws Exception
    {
        // Run the test
        final SchemaTableName result = SchemaTableName.schemaTableName("schemaName", "tableName");
        assertEquals("schemaName", result.getSchemaName());
        assertEquals("tableName", result.getTableName());
        assertEquals(0, result.hashCode());
        assertTrue(result.equals("obj"));
        assertEquals("result", result.toString());
        assertEquals(new SchemaTablePrefix("schemaName", "tableName"), result.toSchemaTablePrefix());
    }
}
