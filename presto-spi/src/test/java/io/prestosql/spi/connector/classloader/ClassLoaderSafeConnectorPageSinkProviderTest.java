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

import io.prestosql.spi.connector.ConnectorDeleteAsInsertTableHandle;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.ConnectorUpdateTableHandle;
import io.prestosql.spi.connector.ConnectorVacuumTableHandle;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ClassLoaderSafeConnectorPageSinkProviderTest
{
    @Mock
    private ConnectorPageSinkProvider mockDelegate;
    @Mock
    private ClassLoader mockClassLoader;

    private ClassLoaderSafeConnectorPageSinkProvider classLoaderSafeConnectorPageSinkProviderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        classLoaderSafeConnectorPageSinkProviderUnderTest = new ClassLoaderSafeConnectorPageSinkProvider(mockDelegate,
                mockClassLoader);
    }

    @Test
    public void testCreatePageSink1()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final ConnectorOutputTableHandle outputTableHandle = null;
        when(mockDelegate.createPageSink(any(ConnectorTransactionHandle.class),
                any(ConnectorSession.class), any(ConnectorOutputTableHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorPageSink result = classLoaderSafeConnectorPageSinkProviderUnderTest.createPageSink(
                transactionHandle, session, outputTableHandle);

        // Verify the results
    }

    @Test
    public void testCreatePageSink2()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final ConnectorInsertTableHandle insertTableHandle = null;
        when(mockDelegate.createPageSink(any(ConnectorTransactionHandle.class),
                any(ConnectorSession.class), any(ConnectorInsertTableHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorPageSink result = classLoaderSafeConnectorPageSinkProviderUnderTest.createPageSink(
                transactionHandle, session, insertTableHandle);

        // Verify the results
    }

    @Test
    public void testCreatePageSink3()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final ConnectorUpdateTableHandle updateTableHandle = null;
        when(mockDelegate.createPageSink(any(ConnectorTransactionHandle.class),
                any(ConnectorSession.class), any(ConnectorUpdateTableHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorPageSink result = classLoaderSafeConnectorPageSinkProviderUnderTest.createPageSink(
                transactionHandle, session, updateTableHandle);

        // Verify the results
    }

    @Test
    public void testCreatePageSink4()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final ConnectorDeleteAsInsertTableHandle deleteTableHandle = null;
        when(mockDelegate.createPageSink(any(ConnectorTransactionHandle.class),
                any(ConnectorSession.class), any(ConnectorDeleteAsInsertTableHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorPageSink result = classLoaderSafeConnectorPageSinkProviderUnderTest.createPageSink(
                transactionHandle, session, deleteTableHandle);

        // Verify the results
    }

    @Test
    public void testCreatePageSink5()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final ConnectorVacuumTableHandle vacuumTableHandle = null;
        when(mockDelegate.createPageSink(any(ConnectorTransactionHandle.class),
                any(ConnectorSession.class), any(ConnectorVacuumTableHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorPageSink result = classLoaderSafeConnectorPageSinkProviderUnderTest.createPageSink(
                transactionHandle, session, vacuumTableHandle);

        // Verify the results
    }
}
