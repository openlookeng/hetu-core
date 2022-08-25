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

import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.resourcegroups.QueryType;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ClassLoaderSafeConnectorSplitManagerTest
{
    @Mock
    private ConnectorSplitManager mockDelegate;
    @Mock
    private ClassLoader mockClassLoader;

    private ClassLoaderSafeConnectorSplitManager classLoaderSafeConnectorSplitManagerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        classLoaderSafeConnectorSplitManagerUnderTest = new ClassLoaderSafeConnectorSplitManager(mockDelegate,
                mockClassLoader);
    }

    @Test
    public void testGetSplits1()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorSession session = null;
        final ConnectorTableLayoutHandle layout = null;

        // Configure ConnectorSplitManager.getSplits(...).
        final ConnectorSplitSource mockConnectorSplitSource = mock(ConnectorSplitSource.class);
        when(mockDelegate.getSplits(any(ConnectorTransactionHandle.class), any(ConnectorSession.class),
                any(ConnectorTableLayoutHandle.class),
                eq(ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING)))
                .thenReturn(mockConnectorSplitSource);

        // Run the test
        final ConnectorSplitSource result = classLoaderSafeConnectorSplitManagerUnderTest.getSplits(transactionHandle,
                session, layout,
                ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING);

        // Verify the results
        verify(mockConnectorSplitSource).close();
    }

    @Test
    public void testGetSplits2() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;

        // Configure ConnectorSplitManager.getSplits(...).
        final ConnectorSplitSource mockConnectorSplitSource = mock(ConnectorSplitSource.class);
        when(mockDelegate.getSplits(any(ConnectorTransactionHandle.class), any(ConnectorSession.class),
                any(ConnectorTableHandle.class),
                eq(ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING)))
                .thenReturn(mockConnectorSplitSource);

        // Run the test
        final ConnectorSplitSource result = classLoaderSafeConnectorSplitManagerUnderTest.getSplits(transaction,
                session, table,
                ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING);

        // Verify the results
        verify(mockConnectorSplitSource).close();
    }

    @Test
    public void testGetSplits3() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;
        final Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier = () -> Arrays.asList(new HashSet<>());
        final Map<String, Object> queryInfo = new HashMap<>();
        final Set<TupleDomain<ColumnMetadata>> userDefinedCachePredicates = new HashSet<>(
                Arrays.asList(TupleDomain.withColumnDomains(new HashMap<>())));

        // Configure ConnectorSplitManager.getSplits(...).
        final ConnectorSplitSource mockConnectorSplitSource = mock(ConnectorSplitSource.class);
        when(mockDelegate.getSplits(any(ConnectorTransactionHandle.class), any(ConnectorSession.class),
                any(ConnectorTableHandle.class), eq(ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING),
                any(Supplier.class), eq(Optional.of(QueryType.DATA_DEFINITION)), eq(new HashMap<>()),
                eq(new HashSet<>(
                        Arrays.asList(TupleDomain.withColumnDomains(new HashMap<>())))), eq(false)))
                .thenReturn(mockConnectorSplitSource);

        // Run the test
        final ConnectorSplitSource result = classLoaderSafeConnectorSplitManagerUnderTest.getSplits(transaction,
                session, table,
                ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING, dynamicFilterSupplier,
                Optional.of(QueryType.DATA_DEFINITION), queryInfo, userDefinedCachePredicates, false);

        // Verify the results
        verify(mockConnectorSplitSource).close();
    }
}
