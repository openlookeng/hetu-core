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
package io.prestosql.spi.metadata;

import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class TableHandleTest
{
    @Mock
    private ConnectorTableHandle mockConnectorHandle;
    @Mock
    private ConnectorTransactionHandle mockTransaction;

    private TableHandle tableHandleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        tableHandleUnderTest = new TableHandle(new CatalogName("catalogName"), mockConnectorHandle, mockTransaction,
                Optional.empty());
    }

    @Test
    public void testGetFullyQualifiedName()
    {
        // Setup
        when(mockConnectorHandle.getSchemaPrefixedTableName()).thenReturn("result");

        // Run the test
        final String result = tableHandleUnderTest.getFullyQualifiedName();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", tableHandleUnderTest.toString());
    }
}
