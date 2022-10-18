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
package io.prestosql.plugin.base.classloader;

import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ClassLoaderSafeSystemTableTest
{
    @Mock
    private SystemTable mockDelegate;
    @Mock
    private ClassLoader mockClassLoader;

    private ClassLoaderSafeSystemTable classLoaderSafeSystemTableUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        classLoaderSafeSystemTableUnderTest = new ClassLoaderSafeSystemTable(mockDelegate, mockClassLoader);
    }

    @Test
    public void testGetDistribution()
    {
        // Setup
        when(mockDelegate.getDistribution()).thenReturn(SystemTable.Distribution.ALL_NODES);

        // Run the test
        final SystemTable.Distribution result = classLoaderSafeSystemTableUnderTest.getDistribution();

        // Verify the results
        assertEquals(SystemTable.Distribution.ALL_NODES, result);
    }

    @Test
    public void testGetTableMetadata()
    {
        // Run the test
        final ConnectorTableMetadata result = classLoaderSafeSystemTableUnderTest.getTableMetadata();

        // Verify the results
    }

    @Test
    public void testCursor()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final TupleDomain<Integer> constraint = TupleDomain.withColumnDomains(new HashMap<>());

        // Configure SystemTable.cursor(...).
        final RecordCursor mockRecordCursor = mock(RecordCursor.class);
        when(mockDelegate.cursor(any(ConnectorTransactionHandle.class), any(ConnectorSession.class),
                eq(TupleDomain.withColumnDomains(new HashMap<>())))).thenReturn(mockRecordCursor);

        // Run the test
        final RecordCursor result = classLoaderSafeSystemTableUnderTest.cursor(transactionHandle, session, constraint);
    }

    @Test
    public void testPageSource() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final TupleDomain<Integer> constraint = TupleDomain.withColumnDomains(new HashMap<>());

        // Configure SystemTable.pageSource(...).
        final ConnectorPageSource mockConnectorPageSource = mock(ConnectorPageSource.class);
        when(mockDelegate.pageSource(any(ConnectorTransactionHandle.class), any(ConnectorSession.class),
                eq(TupleDomain.withColumnDomains(new HashMap<>())))).thenReturn(mockConnectorPageSource);

        // Run the test
        final ConnectorPageSource result = classLoaderSafeSystemTableUnderTest.pageSource(transactionHandle, session,
                constraint);
    }
}
