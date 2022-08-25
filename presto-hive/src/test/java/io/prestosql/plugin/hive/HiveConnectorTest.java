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
package io.prestosql.plugin.hive;

import io.airlift.bootstrap.LifeCycleManager;
import io.prestosql.plugin.hive.util.FieldSetterFactoryTest;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertTrue;

public class HiveConnectorTest
{
    @Mock
    private HiveTransactionManager mockTransactionManager;
    @Mock
    private ConnectorSplitManager mockSplitManager;
    @Mock
    private ConnectorPageSourceProvider mockPageSourceProvider;
    @Mock
    private ConnectorPageSinkProvider mockPageSinkProvider;
    @Mock
    private ConnectorNodePartitioningProvider mockNodePartitioningProvider;
    @Mock
    private ConnectorAccessControl mockAccessControl;
    @Mock
    private ClassLoader mockClassLoader;

    private HiveConnector hiveConnectorUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hiveConnectorUnderTest = new HiveConnector(new LifeCycleManager(Arrays.asList("envalue"), null), () -> null,
                mockTransactionManager, mockSplitManager, mockPageSourceProvider, mockPageSinkProvider,
                mockNodePartitioningProvider, new HashSet<>(), new HashSet<>(
                Arrays.asList(new Procedure("schema", "NAME",
                        Arrays.asList(new Procedure.Argument("NAME", new FieldSetterFactoryTest.Type(), false, "defaultValue")), null))),
                Arrays.asList(new PropertyMetadata<>("NAME", "description", new FieldSetterFactoryTest.Type(), Object.class, null, false, val -> {
                    return null;
                }, val -> {
                    return null;
                })),
                Arrays.asList(new PropertyMetadata<>("NAME", "description", new FieldSetterFactoryTest.Type(), Object.class, null, false, val -> {
                    return null;
                }, val -> {
                    return null;
                })),
                Arrays.asList(new PropertyMetadata<>("NAME", "description", new FieldSetterFactoryTest.Type(), Object.class, null, false, val -> {
                    return null;
                }, val -> {
                    return null;
                })),
                Arrays.asList(new PropertyMetadata<>("NAME", "description", new FieldSetterFactoryTest.Type(), Object.class, null, false, val -> {
                    return null;
                }, val -> {
                    return null;
                })), mockAccessControl, mockClassLoader);
    }

    @Test
    public void testGetMetadata()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        when(mockTransactionManager.get(any(ConnectorTransactionHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorMetadata result = hiveConnectorUnderTest.getMetadata(transaction);

        // Verify the results
    }

    @Test
    public void testIsSingleStatementWritesOnly()
    {
        assertTrue(hiveConnectorUnderTest.isSingleStatementWritesOnly());
    }

    @Test
    public void testBeginTransaction()
    {
        // Setup
        // Run the test
        final ConnectorTransactionHandle result = hiveConnectorUnderTest.beginTransaction(IsolationLevel.SERIALIZABLE,
                false);

        // Verify the results
        verify(mockTransactionManager).put(any(ConnectorTransactionHandle.class), any(TransactionalMetadata.class));
    }

    @Test
    public void testCommit() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        when(mockTransactionManager.remove(any(ConnectorTransactionHandle.class))).thenReturn(null);

        // Run the test
        hiveConnectorUnderTest.commit(transaction);

        // Verify the results
    }

    @Test
    public void testRollback()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        when(mockTransactionManager.remove(any(ConnectorTransactionHandle.class))).thenReturn(null);

        // Run the test
        hiveConnectorUnderTest.rollback(transaction);

        // Verify the results
    }

    @Test
    public void testShutdown()
    {
        // Setup
        // Run the test
        hiveConnectorUnderTest.shutdown();

        // Verify the results
    }

    @Test
    public void testGetConnectorMetadata()
    {
        // Setup
        // Run the test
        final ConnectorMetadata result = hiveConnectorUnderTest.getConnectorMetadata();

        // Verify the results
    }

    @Test
    public void test()
    {
        hiveConnectorUnderTest.getPageSinkProvider();
        hiveConnectorUnderTest.getNodePartitioningProvider();
        hiveConnectorUnderTest.getSystemTables();
        hiveConnectorUnderTest.getProcedures();
        hiveConnectorUnderTest.getSessionProperties();
        hiveConnectorUnderTest.getSchemaProperties();
        hiveConnectorUnderTest.getAnalyzeProperties();
        hiveConnectorUnderTest.getTableProperties();
        hiveConnectorUnderTest.getAccessControl();
        hiveConnectorUnderTest.isSingleStatementWritesOnly();
    }
}
