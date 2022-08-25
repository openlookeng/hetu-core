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

import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSplitSource;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ClassLoaderSafeConnectorSplitSourceTest
{
    @Mock
    private ConnectorSplitSource mockDelegate;
    @Mock
    private ClassLoader mockClassLoader;

    private ClassLoaderSafeConnectorSplitSource classLoaderSafeConnectorSplitSourceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        classLoaderSafeConnectorSplitSourceUnderTest = new ClassLoaderSafeConnectorSplitSource(mockDelegate,
                mockClassLoader);
    }

    @Test
    public void testGetNextBatch()
    {
        // Setup
        final ConnectorPartitionHandle partitionHandle = null;

        // Configure ConnectorSplitSource.getNextBatch(...).
        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> connectorSplitBatchCompletableFuture = CompletableFuture.completedFuture(
                new ConnectorSplitSource.ConnectorSplitBatch(
                        Arrays.asList(), false));
        when(mockDelegate.getNextBatch(null, 0)).thenReturn(connectorSplitBatchCompletableFuture);

        // Run the test
        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> result = classLoaderSafeConnectorSplitSourceUnderTest.getNextBatch(
                partitionHandle, 0);

        // Verify the results
    }

    @Test
    public void testGetNextBatch_ConnectorSplitSourceReturnsFailure()
    {
        // Setup
        final ConnectorPartitionHandle partitionHandle = null;

        // Configure ConnectorSplitSource.getNextBatch(...).
        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> connectorSplitBatchCompletableFuture = new CompletableFuture<>();
        connectorSplitBatchCompletableFuture.completeExceptionally(new Exception("message"));
        when(mockDelegate.getNextBatch(null, 0)).thenReturn(connectorSplitBatchCompletableFuture);

        // Run the test
        final CompletableFuture<ConnectorSplitSource.ConnectorSplitBatch> result = classLoaderSafeConnectorSplitSourceUnderTest.getNextBatch(
                partitionHandle, 0);

        // Verify the results
    }

    @Test
    public void testGetTableExecuteSplitsInfo()
    {
        // Setup
        when(mockDelegate.getTableExecuteSplitsInfo()).thenReturn(Optional.of(Arrays.asList("value")));

        // Run the test
        final Optional<List<Object>> result = classLoaderSafeConnectorSplitSourceUnderTest.getTableExecuteSplitsInfo();

        // Verify the results
    }

    @Test
    public void testGetTableExecuteSplitsInfo_ConnectorSplitSourceReturnsAbsent()
    {
        // Setup
        when(mockDelegate.getTableExecuteSplitsInfo()).thenReturn(Optional.empty());

        // Run the test
        final Optional<List<Object>> result = classLoaderSafeConnectorSplitSourceUnderTest.getTableExecuteSplitsInfo();

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetTableExecuteSplitsInfo_ConnectorSplitSourceReturnsNoItems()
    {
        // Setup
        when(mockDelegate.getTableExecuteSplitsInfo()).thenReturn(Optional.of(Collections.emptyList()));

        // Run the test
        final Optional<List<Object>> result = classLoaderSafeConnectorSplitSourceUnderTest.getTableExecuteSplitsInfo();

        // Verify the results
        assertEquals(Optional.of(Collections.emptyList()), result);
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        classLoaderSafeConnectorSplitSourceUnderTest.close();

        // Verify the results
        verify(mockDelegate).close();
    }

    @Test
    public void testIsFinished()
    {
        classLoaderSafeConnectorSplitSourceUnderTest.isFinished();
    }
}
