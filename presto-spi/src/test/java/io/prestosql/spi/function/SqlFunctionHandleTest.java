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
import io.prestosql.spi.connector.QualifiedObjectName;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SqlFunctionHandleTest
{
    @Mock
    private SqlFunctionId mockFunctionId;

    private SqlFunctionHandle sqlFunctionHandleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        sqlFunctionHandleUnderTest = new SqlFunctionHandle(mockFunctionId, "version");
    }

    @Test
    public void testGetFunctionNamespace() throws Exception
    {
        // Setup
        final CatalogSchemaName expectedResult = new CatalogSchemaName("catalogName", "schemaName");

        // Configure SqlFunctionId.getFunctionName(...).
        final QualifiedObjectName qualifiedObjectName = new QualifiedObjectName("catalogName", "schemaName",
                "objectName");
        when(mockFunctionId.getFunctionName()).thenReturn(qualifiedObjectName);

        // Run the test
        final CatalogSchemaName result = sqlFunctionHandleUnderTest.getFunctionNamespace();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(sqlFunctionHandleUnderTest.equals("obj"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, sqlFunctionHandleUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", sqlFunctionHandleUnderTest.toString());
    }
}
