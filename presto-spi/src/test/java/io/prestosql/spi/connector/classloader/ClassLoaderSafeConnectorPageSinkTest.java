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

import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ClassLoaderSafeConnectorPageSinkTest
{
    @Mock
    private ConnectorPageSink mockDelegate;
    @Mock
    private ClassLoader mockClassLoader;

    private ClassLoaderSafeConnectorPageSink classLoaderSafeConnectorPageSinkUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        classLoaderSafeConnectorPageSinkUnderTest = new ClassLoaderSafeConnectorPageSink(mockDelegate, mockClassLoader);
    }

    @Test
    public void testGetCompletedBytes() throws Exception
    {
        // Setup
        when(mockDelegate.getCompletedBytes()).thenReturn(0L);

        // Run the test
        final long result = classLoaderSafeConnectorPageSinkUnderTest.getCompletedBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetRowsWritten()
    {
        // Setup
        when(mockDelegate.getRowsWritten()).thenReturn(0L);

        // Run the test
        final long result = classLoaderSafeConnectorPageSinkUnderTest.getRowsWritten();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetSystemMemoryUsage() throws Exception
    {
        // Setup
        when(mockDelegate.getSystemMemoryUsage()).thenReturn(0L);

        // Run the test
        final long result = classLoaderSafeConnectorPageSinkUnderTest.getSystemMemoryUsage();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetValidationCpuNanos() throws Exception
    {
        // Setup
        when(mockDelegate.getValidationCpuNanos()).thenReturn(0L);

        // Run the test
        final long result = classLoaderSafeConnectorPageSinkUnderTest.getValidationCpuNanos();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testAppendPage() throws Exception
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);
        doReturn(CompletableFuture.completedFuture(null)).when(mockDelegate).appendPage(any(Page.class));

        // Run the test
        final CompletableFuture<?> result = classLoaderSafeConnectorPageSinkUnderTest.appendPage(page);

        // Verify the results
    }

    @Test
    public void testAppendPage_ConnectorPageSinkReturnsFailure() throws Exception
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);

        // Configure ConnectorPageSink.appendPage(...).
        final CompletableFuture<?> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new Exception("message"));
        doReturn(completableFuture).when(mockDelegate).appendPage(any(Page.class));

        // Run the test
        final CompletableFuture<?> result = classLoaderSafeConnectorPageSinkUnderTest.appendPage(page);

        // Verify the results
    }

    @Test
    public void testFinish() throws Exception
    {
        // Setup
        // Configure ConnectorPageSink.finish(...).
        final CompletableFuture<Collection<Slice>> collectionCompletableFuture = CompletableFuture.completedFuture(
                Arrays.asList());
        when(mockDelegate.finish()).thenReturn(collectionCompletableFuture);

        // Run the test
        final CompletableFuture<Collection<Slice>> result = classLoaderSafeConnectorPageSinkUnderTest.finish();

        // Verify the results
    }

    @Test
    public void testFinish_ConnectorPageSinkReturnsNoItems() throws Exception
    {
        // Setup
        // Configure ConnectorPageSink.finish(...).
        final CompletableFuture<Collection<Slice>> collectionCompletableFuture = CompletableFuture.completedFuture(
                Collections.emptyList());
        when(mockDelegate.finish()).thenReturn(collectionCompletableFuture);

        // Run the test
        final CompletableFuture<Collection<Slice>> result = classLoaderSafeConnectorPageSinkUnderTest.finish();

        // Verify the results
    }

    @Test
    public void testFinish_ConnectorPageSinkReturnsFailure() throws Exception
    {
        // Setup
        // Configure ConnectorPageSink.finish(...).
        final CompletableFuture<Collection<Slice>> collectionCompletableFuture = new CompletableFuture<>();
        collectionCompletableFuture.completeExceptionally(new Exception("message"));
        when(mockDelegate.finish()).thenReturn(collectionCompletableFuture);

        // Run the test
        final CompletableFuture<Collection<Slice>> result = classLoaderSafeConnectorPageSinkUnderTest.finish();

        // Verify the results
    }

    @Test
    public void testAbort() throws Exception
    {
        // Setup
        // Run the test
        classLoaderSafeConnectorPageSinkUnderTest.abort();

        // Verify the results
        verify(mockDelegate).abort();
    }

    @Test
    public void testCancelToResume() throws Exception
    {
        // Setup
        // Run the test
        classLoaderSafeConnectorPageSinkUnderTest.cancelToResume();

        // Verify the results
        verify(mockDelegate).cancelToResume();
    }

    @Test
    public void testVacuum() throws Exception
    {
        // Setup
        final ConnectorPageSourceProvider pageSourceProvider = null;
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorTableHandle connectorTableHandle = null;
        final List<ConnectorSplit> splits = Arrays.asList();

        // Configure ConnectorPageSink.vacuum(...).
        final ConnectorPageSink.VacuumResult vacuumResult = new ConnectorPageSink.VacuumResult(
                new Page(0, new Properties(), null), false);
        when(mockDelegate.vacuum(any(ConnectorPageSourceProvider.class), any(ConnectorTransactionHandle.class),
                any(ConnectorTableHandle.class), eq(Arrays.asList()))).thenReturn(vacuumResult);

        // Run the test
        final ConnectorPageSink.VacuumResult result = classLoaderSafeConnectorPageSinkUnderTest.vacuum(
                pageSourceProvider, transactionHandle, connectorTableHandle, splits);

        // Verify the results
    }

    @Test
    public void testCapture() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;
        when(mockDelegate.capture(any(BlockEncodingSerdeProvider.class))).thenReturn("result");

        // Run the test
        final Object result = classLoaderSafeConnectorPageSinkUnderTest.capture(serdeProvider);

        // Verify the results
    }

    @Test
    public void testRestore() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        classLoaderSafeConnectorPageSinkUnderTest.restore("state", serdeProvider, 0L);

        // Verify the results
        verify(mockDelegate).restore(any(Object.class), any(BlockEncodingSerdeProvider.class), eq(0L));
    }
}
