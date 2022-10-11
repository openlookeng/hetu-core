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

public class CatalogNameTest
{
    private CatalogName catalogNameUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        catalogNameUnderTest = new CatalogName("catalogName");
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(catalogNameUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, catalogNameUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("catalogName", catalogNameUnderTest.toString());
    }

    @Test
    public void testIsInternalSystemConnector() throws Exception
    {
        // Setup
        final CatalogName catalogName = new CatalogName("catalogName");

        // Run the test
        final boolean result = CatalogName.isInternalSystemConnector(catalogName);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testCreateInformationSchemaCatalogName() throws Exception
    {
        // Setup
        final CatalogName catalogName = new CatalogName("catalogName");
        final CatalogName expectedResult = new CatalogName("catalogName");

        // Run the test
        final CatalogName result = CatalogName.createInformationSchemaCatalogName(catalogName);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCreateSystemTablesCatalogName()
    {
        // Setup
        final CatalogName catalogName = new CatalogName("catalogName");
        final CatalogName expectedResult = new CatalogName("catalogName");

        // Run the test
        final CatalogName result = CatalogName.createSystemTablesCatalogName(catalogName);

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
