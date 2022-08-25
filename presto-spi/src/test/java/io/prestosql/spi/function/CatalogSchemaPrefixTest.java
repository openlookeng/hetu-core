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
package io.prestosql.spi.function;

import io.prestosql.spi.connector.CatalogSchemaName;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class CatalogSchemaPrefixTest
{
    private CatalogSchemaPrefix catalogSchemaPrefixUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        catalogSchemaPrefixUnderTest = new CatalogSchemaPrefix("catalogName", Optional.of("value"));
    }

    @Test
    public void testIncludes1() throws Exception
    {
        // Setup
        final CatalogSchemaName catalogSchemaName = new CatalogSchemaName("catalogName", "schemaName");

        // Run the test
        final boolean result = catalogSchemaPrefixUnderTest.includes(catalogSchemaName);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIncludes2() throws Exception
    {
        // Setup
        final CatalogSchemaPrefix that = new CatalogSchemaPrefix("catalogName", Optional.of("value"));

        // Run the test
        final boolean result = catalogSchemaPrefixUnderTest.includes(that);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(catalogSchemaPrefixUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, catalogSchemaPrefixUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = catalogSchemaPrefixUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testOf() throws Exception
    {
        // Run the test
        final CatalogSchemaPrefix result = CatalogSchemaPrefix.of("prefix");
        assertEquals("catalogName", result.getCatalogName());
        assertEquals(Optional.of("value"), result.getSchemaName());
        final CatalogSchemaName catalogSchemaName = new CatalogSchemaName("catalogName", "schemaName");
        assertTrue(result.includes(catalogSchemaName));
        final CatalogSchemaPrefix that = new CatalogSchemaPrefix("catalogName", Optional.of("value"));
        assertTrue(result.includes(that));
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
    }
}
