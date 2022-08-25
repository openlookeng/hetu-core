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
package io.prestosql.spi.connector.classloader;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ClassLoaderSafeConnectorPageSourceProviderTest
{
    @Mock
    private ConnectorPageSourceProvider mockDelegate;
    @Mock
    private ClassLoader mockClassLoader;

    private ClassLoaderSafeConnectorPageSourceProvider classLoaderSafeConnectorPageSourceProviderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        classLoaderSafeConnectorPageSourceProviderUnderTest = new ClassLoaderSafeConnectorPageSourceProvider(
                mockDelegate, mockClassLoader);
    }

    @Test
    public void testCreatePageSource1() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final ConnectorSplit split = null;
        final List<ColumnHandle> columns = Arrays.asList();

        // Configure ConnectorPageSourceProvider.createPageSource(...).
        final ConnectorPageSource mockConnectorPageSource = mock(ConnectorPageSource.class);
        when(mockDelegate.createPageSource(any(ConnectorTransactionHandle.class), any(ConnectorSession.class),
                any(ConnectorSplit.class), eq(Arrays.asList()))).thenReturn(mockConnectorPageSource);

        // Run the test
        final ConnectorPageSource result = classLoaderSafeConnectorPageSourceProviderUnderTest.createPageSource(
                transactionHandle, session, split, columns);

        // Verify the results
        verify(mockConnectorPageSource).close();
    }

    @Test
    public void testCreatePageSource2() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorSession session = null;
        final ConnectorSplit split = null;
        final ConnectorTableHandle table = null;
        final List<ColumnHandle> columns = Arrays.asList();

        // Configure ConnectorPageSourceProvider.createPageSource(...).
        final ConnectorPageSource mockConnectorPageSource = mock(ConnectorPageSource.class);
        when(mockDelegate.createPageSource(any(ConnectorTransactionHandle.class), any(ConnectorSession.class),
                any(ConnectorSplit.class), any(ConnectorTableHandle.class), eq(Arrays.asList())))
                .thenReturn(mockConnectorPageSource);

        // Run the test
        final ConnectorPageSource result = classLoaderSafeConnectorPageSourceProviderUnderTest.createPageSource(
                transaction, session, split, table, columns);

        // Verify the results
        verify(mockConnectorPageSource).close();
    }

    @Test
    public void testCreatePageSource3() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorSession session = null;
        final ConnectorSplit split = null;
        final ConnectorTableHandle table = null;
        final List<ColumnHandle> columns = Arrays.asList();
        final Optional<DynamicFilterSupplier> dynamicFilterSupplier = Optional.of(
                new DynamicFilterSupplier(() -> Arrays.asList(new HashMap<>()), 0L, 0L));

        // Configure ConnectorPageSourceProvider.createPageSource(...).
        final ConnectorPageSource mockConnectorPageSource = mock(ConnectorPageSource.class);
        when(mockDelegate.createPageSource(any(ConnectorTransactionHandle.class), any(ConnectorSession.class),
                any(ConnectorSplit.class), any(ConnectorTableHandle.class), eq(Arrays.asList()),
                eq(Optional.of(new DynamicFilterSupplier(() -> Arrays.asList(new HashMap<>()), 0L, 0L)))))
                .thenReturn(mockConnectorPageSource);

        // Run the test
        final ConnectorPageSource result = classLoaderSafeConnectorPageSourceProviderUnderTest.createPageSource(
                transaction, session, split, table, columns, dynamicFilterSupplier);

        // Verify the results
        verify(mockConnectorPageSource).close();
    }
}
