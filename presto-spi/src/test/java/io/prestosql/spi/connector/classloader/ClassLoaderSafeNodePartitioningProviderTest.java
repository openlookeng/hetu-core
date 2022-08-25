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

import io.prestosql.spi.connector.BucketFunction;
import io.prestosql.spi.connector.ConnectorBucketNodeMap;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.ToIntFunction;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ClassLoaderSafeNodePartitioningProviderTest
{
    @Mock
    private ConnectorNodePartitioningProvider mockDelegate;
    @Mock
    private ClassLoader mockClassLoader;

    private ClassLoaderSafeNodePartitioningProvider classLoaderSafeNodePartitioningProviderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        classLoaderSafeNodePartitioningProviderUnderTest = new ClassLoaderSafeNodePartitioningProvider(mockDelegate,
                mockClassLoader);
    }

    @Test
    public void testGetBucketFunction() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final ConnectorPartitioningHandle partitioningHandle = null;
        final List<Type> partitionChannelTypes = Arrays.asList();
        when(mockDelegate.getBucketFunction(any(ConnectorTransactionHandle.class),
                any(ConnectorSession.class), any(ConnectorPartitioningHandle.class), eq(Arrays.asList()),
                eq(0))).thenReturn(null);

        // Run the test
        final BucketFunction result = classLoaderSafeNodePartitioningProviderUnderTest.getBucketFunction(
                transactionHandle, session, partitioningHandle, partitionChannelTypes, 0);

        // Verify the results
    }

    @Test
    public void testListPartitionHandles() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final ConnectorPartitioningHandle partitioningHandle = null;
        final List<ConnectorPartitionHandle> expectedResult = Arrays.asList();
        when(mockDelegate.listPartitionHandles(any(ConnectorTransactionHandle.class),
                any(ConnectorSession.class), any(ConnectorPartitioningHandle.class))).thenReturn(Arrays.asList());

        // Run the test
        final List<ConnectorPartitionHandle> result = classLoaderSafeNodePartitioningProviderUnderTest.listPartitionHandles(
                transactionHandle, session, partitioningHandle);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListPartitionHandles_ConnectorNodePartitioningProviderReturnsNoItems() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final ConnectorPartitioningHandle partitioningHandle = null;
        when(mockDelegate.listPartitionHandles(any(ConnectorTransactionHandle.class), any(ConnectorSession.class),
                any(ConnectorPartitioningHandle.class))).thenReturn(Collections.emptyList());

        // Run the test
        final List<ConnectorPartitionHandle> result = classLoaderSafeNodePartitioningProviderUnderTest.listPartitionHandles(
                transactionHandle, session, partitioningHandle);

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testGetBucketNodeMap() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final ConnectorPartitioningHandle partitioningHandle = null;

        // Configure ConnectorNodePartitioningProvider.getBucketNodeMap(...).
        final ConnectorBucketNodeMap connectorBucketNodeMap = ConnectorBucketNodeMap.createBucketNodeMap(0);
        when(mockDelegate.getBucketNodeMap(any(ConnectorTransactionHandle.class), any(ConnectorSession.class),
                any(ConnectorPartitioningHandle.class))).thenReturn(connectorBucketNodeMap);

        // Run the test
        final ConnectorBucketNodeMap result = classLoaderSafeNodePartitioningProviderUnderTest.getBucketNodeMap(
                transactionHandle, session, partitioningHandle);

        // Verify the results
    }

    @Test
    public void testGetSplitBucketFunction() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final ConnectorPartitioningHandle partitioningHandle = null;
        when(mockDelegate.getSplitBucketFunction(any(ConnectorTransactionHandle.class),
                any(ConnectorSession.class), any(ConnectorPartitioningHandle.class))).thenReturn(val ->
                {
                    return 0; });

        // Run the test
        final ToIntFunction<ConnectorSplit> result = classLoaderSafeNodePartitioningProviderUnderTest.getSplitBucketFunction(
                transactionHandle, session, partitioningHandle);

        // Verify the results
    }
}
