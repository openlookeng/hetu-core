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
package io.hetu.core.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorCapabilities;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.TableProcedureExecutionMode;
import io.prestosql.spi.connector.TableProcedureMetadata;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.spi.type.CharType;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class IcebergConnectorTest
{
    @Mock
    private IcebergTransactionManager mockTransactionManager;
    @Mock
    private ConnectorSplitManager mockSplitManager;
    @Mock
    private ConnectorPageSourceProvider mockPageSourceProvider;
    @Mock
    private ConnectorPageSinkProvider mockPageSinkProvider;
    @Mock
    private ConnectorNodePartitioningProvider mockNodePartitioningProvider;
    @Mock
    private MethodHandle mockMethodHandle;

    private IcebergConnector icebergConnectorUnderTest;

    private static final MethodHandle ROLLBACK_TO_SNAPSHOT = methodHandle(
            RollbackToSnapshotProcedure.class,
            "rollbackToSnapshot",
            ConnectorSession.class,
            String.class,
            String.class,
            Long.class);

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        CharType charType = CharType.createCharType(10);
        LifeCycleManager value = new LifeCycleManager(Arrays.asList("value"), null);
        PropertyMetadata<Object> objectPropertyMetadata = new PropertyMetadata<>("name", "description", charType, Object.class, null, false, val -> {
            return null;
        }, val -> {
            return null;
        });
        Procedure procedure = new Procedure(
                "schema",
                "rollback_to_snapshot",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMAS", VARCHAR.getTypeSignature()),
                        new Procedure.Argument("SCHEMA", VARCHAR.getTypeSignature()),
                        new Procedure.Argument("TABLE", VARCHAR.getTypeSignature()),
                        new Procedure.Argument("SNAPSHOT_ID", BIGINT.getTypeSignature())),
                ROLLBACK_TO_SNAPSHOT);
//        Procedure procedure = new Procedure("schema", "NAME", Arrays.asList(new Procedure.Argument("NAME", charType, true, "defaultValue")), mockMethodHandle);
        TableProcedureMetadata name = new TableProcedureMetadata("NAME", TableProcedureExecutionMode.coordinatorOnly(),
                Arrays.asList(objectPropertyMetadata));
        icebergConnectorUnderTest = new IcebergConnector(value,
                mockTransactionManager, mockSplitManager, mockPageSourceProvider, mockPageSinkProvider,
                mockNodePartitioningProvider, new HashSet<>(),
                Arrays.asList(objectPropertyMetadata),
                Arrays.asList(objectPropertyMetadata),
                Optional.empty(), new HashSet<>(
                Arrays.asList(procedure)),
                new HashSet<>(Arrays.asList(name)));
    }

    @Test
    public void testGetCapabilities()
    {
        assertEquals(new HashSet<>(Arrays.asList(ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT)),
                icebergConnectorUnderTest.getCapabilities());
    }

    @Test
    public void testGetMetadata()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTransactionHandle transaction = null;

        // Configure IcebergTransactionManager.get(...).
        final HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setTargetMaxFileSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxInitialSplits(0);
        hiveConfig.setMaxInitialSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setSplitLoaderConcurrency(0);
        hiveConfig.setMaxSplitsPerSecond(0);
        hiveConfig.setDomainCompactionThreshold(0);
        hiveConfig.setWriterSortBufferSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setForceLocalScheduling(false);
        hiveConfig.setMaxConcurrentFileRenames(0);
        hiveConfig.setRecursiveDirWalkerEnabled(false);
        hiveConfig.setMaxSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxPartitionsPerScan(0);
        hiveConfig.setMaxOutstandingSplits(0);
        hiveConfig.setMaxOutstandingSplitsSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxSplitIteratorThreads(0);
        HiveConfig hdfsConfig = new HiveConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        final IcebergMetadata icebergMetadata = new IcebergMetadata(null, null, null,
                new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication()));
        when(mockTransactionManager.get(any(ConnectorTransactionHandle.class),
                eq(new ConnectorIdentity("user", new HashSet<>(
                        Arrays.asList("value")), Optional.empty(),
                        Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))),
                        new HashMap<>())))).thenReturn(icebergMetadata);

        // Run the test
        final ConnectorMetadata result = icebergConnectorUnderTest.getMetadata(session, transaction);

        // Verify the results
    }

    @Test
    public void testGetMaterializedViewProperties()
    {
        // Setup
        // Run the test
        final List<PropertyMetadata<?>> result = icebergConnectorUnderTest.getMaterializedViewProperties();

        // Verify the results
    }

    @Test
    public void testGet()
    {
        icebergConnectorUnderTest.getSplitManager();
        icebergConnectorUnderTest.getPageSourceProvider();
        icebergConnectorUnderTest.getPageSinkProvider();
        icebergConnectorUnderTest.getNodePartitioningProvider();
        icebergConnectorUnderTest.getProcedures();
        icebergConnectorUnderTest.getTableProcedures();
        icebergConnectorUnderTest.getSessionProperties();
        icebergConnectorUnderTest.getSchemaProperties();
        icebergConnectorUnderTest.getTableProperties();
        icebergConnectorUnderTest.getMaterializedViewProperties();
        // Verify the results
    }

    @Test
    public void testGetAccessControl()
    {
        // Setup
        // Run the test
        final ConnectorAccessControl result = icebergConnectorUnderTest.getAccessControl();

        // Verify the results
    }

    @Test
    public void testGetAccessControl_ThrowsUnsupportedOperationException()
    {
        // Setup
        // Run the test
        assertThrows(UnsupportedOperationException.class, () -> icebergConnectorUnderTest.getAccessControl());
    }

    @Test
    public void testBeginTransaction()
    {
        // Setup
        // Run the test
        final ConnectorTransactionHandle result = icebergConnectorUnderTest.beginTransaction(
                IsolationLevel.SERIALIZABLE, false);

        // Verify the results
        verify(mockTransactionManager).begin(any(ConnectorTransactionHandle.class));
    }

    @Test
    public void testCommit()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;

        // Run the test
        icebergConnectorUnderTest.commit(transaction);

        // Verify the results
        verify(mockTransactionManager).commit(any(ConnectorTransactionHandle.class));
    }

    @Test
    public void testRollback()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;

        // Run the test
        icebergConnectorUnderTest.rollback(transaction);

        // Verify the results
        verify(mockTransactionManager).rollback(any(ConnectorTransactionHandle.class));
    }

    @Test
    public void testShutdown()
    {
        // Setup
        // Run the test
        icebergConnectorUnderTest.shutdown();

        // Verify the results
    }
}
