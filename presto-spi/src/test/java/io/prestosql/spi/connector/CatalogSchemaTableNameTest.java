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

public class CatalogSchemaTableNameTest
{
    private CatalogSchemaTableName catalogSchemaTableNameUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        catalogSchemaTableNameUnderTest = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(catalogSchemaTableNameUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, catalogSchemaTableNameUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = catalogSchemaTableNameUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }
}
