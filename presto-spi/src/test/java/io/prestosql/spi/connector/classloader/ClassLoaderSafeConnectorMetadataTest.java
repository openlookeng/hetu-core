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
import io.prestosql.spi.PartialAndFinalAggregationType;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorDeleteAsInsertTableHandle;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorResolvedIndex;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutResult;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTablePartitioning;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.ConnectorUpdateTableHandle;
import io.prestosql.spi.connector.ConnectorVacuumTableHandle;
import io.prestosql.spi.connector.ConnectorVacuumTableInfo;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.DiscretePredicates;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.SampleType;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.GrantInfo;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.PrivilegeInfo;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticMetadata;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatisticType;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.statistics.TableStatisticsMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class ClassLoaderSafeConnectorMetadataTest
{
    @Mock
    private ConnectorMetadata mockDelegate;
    @Mock
    private ClassLoader mockClassLoader;

    private ClassLoaderSafeConnectorMetadata classLoaderSafeConnectorMetadataUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        classLoaderSafeConnectorMetadataUnderTest = new ClassLoaderSafeConnectorMetadata(mockDelegate, mockClassLoader);
    }

    @Test
    public void testGetTableLayouts() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });
        final Optional<Set<ColumnHandle>> desiredColumns = Optional.of(new HashSet<>());

        // Configure ConnectorMetadata.getTableLayouts(...).
        final List<ConnectorTableLayoutResult> connectorTableLayoutResults = Arrays.asList(
                new ConnectorTableLayoutResult(new ConnectorTableLayout(null, Optional.of(
                        Arrays.asList()), TupleDomain.withColumnDomains(new HashMap<>()),
                        Optional.of(new ConnectorTablePartitioning(null, Arrays.asList())),
                        Optional.of(new HashSet<>()), Optional.of(new DiscretePredicates(
                        Arrays.asList(), Arrays.asList(TupleDomain.withColumnDomains(new HashMap<>())))),
                        Arrays.asList()), TupleDomain.withColumnDomains(new HashMap<>())));
        when(mockDelegate.getTableLayouts(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(Constraint.class), eq(Optional.of(new HashSet<>())))).thenReturn(connectorTableLayoutResults);

        // Run the test
        final List<ConnectorTableLayoutResult> result = classLoaderSafeConnectorMetadataUnderTest.getTableLayouts(
                session, table, constraint, desiredColumns);

        // Verify the results
    }

    @Test
    public void testGetTableLayouts_ConnectorMetadataReturnsNoItems() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });
        final Optional<Set<ColumnHandle>> desiredColumns = Optional.of(new HashSet<>());
        when(mockDelegate.getTableLayouts(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(Constraint.class),
                eq(Optional.of(new HashSet<>())))).thenReturn(Collections.emptyList());

        // Run the test
        final List<ConnectorTableLayoutResult> result = classLoaderSafeConnectorMetadataUnderTest.getTableLayouts(
                session, table, constraint, desiredColumns);

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testGetTableLayout() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableLayoutHandle handle = null;
        final ConnectorTableLayout expectedResult = new ConnectorTableLayout(null, Optional.of(Arrays.asList()),
                TupleDomain.withColumnDomains(new HashMap<>()),
                Optional.of(new ConnectorTablePartitioning(null, Arrays.asList())), Optional.of(new HashSet<>()),
                Optional.of(new DiscretePredicates(
                        Arrays.asList(), Arrays.asList(TupleDomain.withColumnDomains(new HashMap<>())))),
                Arrays.asList());

        // Configure ConnectorMetadata.getTableLayout(...).
        final ConnectorTableLayout connectorTableLayout = new ConnectorTableLayout(null, Optional.of(Arrays.asList()),
                TupleDomain.withColumnDomains(new HashMap<>()),
                Optional.of(new ConnectorTablePartitioning(null, Arrays.asList())), Optional.of(new HashSet<>()),
                Optional.of(new DiscretePredicates(
                        Arrays.asList(), Arrays.asList(TupleDomain.withColumnDomains(new HashMap<>())))),
                Arrays.asList());
        when(mockDelegate.getTableLayout(any(ConnectorSession.class),
                any(ConnectorTableLayoutHandle.class))).thenReturn(connectorTableLayout);

        // Run the test
        final ConnectorTableLayout result = classLoaderSafeConnectorMetadataUnderTest.getTableLayout(session, handle);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetCommonPartitioningHandle()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorPartitioningHandle left = null;
        final ConnectorPartitioningHandle right = null;
        when(mockDelegate.getCommonPartitioningHandle(any(ConnectorSession.class),
                any(ConnectorPartitioningHandle.class), any(ConnectorPartitioningHandle.class)))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorPartitioningHandle> result = classLoaderSafeConnectorMetadataUnderTest.getCommonPartitioningHandle(
                session, left, right);

        // Verify the results
    }

    @Test
    public void testGetCommonPartitioningHandle_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorPartitioningHandle left = null;
        final ConnectorPartitioningHandle right = null;
        when(mockDelegate.getCommonPartitioningHandle(any(ConnectorSession.class),
                any(ConnectorPartitioningHandle.class),
                any(ConnectorPartitioningHandle.class))).thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorPartitioningHandle> result = classLoaderSafeConnectorMetadataUnderTest.getCommonPartitioningHandle(
                session, left, right);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testMakeCompatiblePartitioning1()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableLayoutHandle tableLayoutHandle = null;
        final ConnectorPartitioningHandle partitioningHandle = null;
        when(mockDelegate.makeCompatiblePartitioning(any(ConnectorSession.class),
                any(ConnectorTableLayoutHandle.class), any(ConnectorPartitioningHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorTableLayoutHandle result = classLoaderSafeConnectorMetadataUnderTest.makeCompatiblePartitioning(
                session, tableLayoutHandle, partitioningHandle);

        // Verify the results
    }

    @Test
    public void testMakeCompatiblePartitioning2() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ConnectorPartitioningHandle partitioningHandle = null;
        when(mockDelegate.makeCompatiblePartitioning(any(ConnectorSession.class),
                any(ConnectorTableHandle.class), any(ConnectorPartitioningHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorTableHandle result = classLoaderSafeConnectorMetadataUnderTest.makeCompatiblePartitioning(
                session, tableHandle, partitioningHandle);

        // Verify the results
    }

    @Test
    public void testGetNewTableLayout() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schemaName", "tableName"), Arrays.asList(
                        new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)),
                new HashMap<>(), Optional.of("value"), Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), Optional.of(new HashSet<>(
                Arrays.asList("value"))));

        // Configure ConnectorMetadata.getNewTableLayout(...).
        final Optional<ConnectorNewTableLayout> connectorNewTableLayout = Optional.of(
                new ConnectorNewTableLayout(null, Arrays.asList("value")));
        when(mockDelegate.getNewTableLayout(any(ConnectorSession.class), any(ConnectorTableMetadata.class)))
                .thenReturn(connectorNewTableLayout);

        // Run the test
        final Optional<ConnectorNewTableLayout> result = classLoaderSafeConnectorMetadataUnderTest.getNewTableLayout(
                session, tableMetadata);

        // Verify the results
    }

    @Test
    public void testGetNewTableLayout_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schemaName", "tableName"), Arrays.asList(
                        new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)),
                new HashMap<>(), Optional.of("value"), Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), Optional.of(new HashSet<>(
                Arrays.asList("value"))));
        when(mockDelegate.getNewTableLayout(any(ConnectorSession.class), any(ConnectorTableMetadata.class)))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorNewTableLayout> result = classLoaderSafeConnectorMetadataUnderTest.getNewTableLayout(
                session, tableMetadata);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetInsertLayout() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;

        // Configure ConnectorMetadata.getInsertLayout(...).
        final Optional<ConnectorNewTableLayout> connectorNewTableLayout = Optional.of(
                new ConnectorNewTableLayout(null, Arrays.asList("value")));
        when(mockDelegate.getInsertLayout(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(connectorNewTableLayout);

        // Run the test
        final Optional<ConnectorNewTableLayout> result = classLoaderSafeConnectorMetadataUnderTest.getInsertLayout(
                session, tableHandle);

        // Verify the results
    }

    @Test
    public void testGetInsertLayout_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.getInsertLayout(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorNewTableLayout> result = classLoaderSafeConnectorMetadataUnderTest.getInsertLayout(
                session, tableHandle);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetUpdateLayout() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;

        // Configure ConnectorMetadata.getUpdateLayout(...).
        final Optional<ConnectorNewTableLayout> connectorNewTableLayout = Optional.of(
                new ConnectorNewTableLayout(null, Arrays.asList("value")));
        when(mockDelegate.getUpdateLayout(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(connectorNewTableLayout);

        // Run the test
        final Optional<ConnectorNewTableLayout> result = classLoaderSafeConnectorMetadataUnderTest.getUpdateLayout(
                session, tableHandle);

        // Verify the results
    }

    @Test
    public void testGetUpdateLayout_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.getUpdateLayout(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorNewTableLayout> result = classLoaderSafeConnectorMetadataUnderTest.getUpdateLayout(
                session, tableHandle);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetStatisticsCollectionMetadataForWrite() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schemaName", "tableName"), Arrays.asList(
                        new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)),
                new HashMap<>(), Optional.of("value"), Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), Optional.of(new HashSet<>(
                Arrays.asList("value"))));

        // Configure ConnectorMetadata.getStatisticsCollectionMetadataForWrite(...).
        final TableStatisticsMetadata tableStatisticsMetadata = new TableStatisticsMetadata(new HashSet<>(
                Arrays.asList(new ColumnStatisticMetadata("columnName", ColumnStatisticType.MIN_VALUE))), new HashSet<>(
                Arrays.asList(TableStatisticType.ROW_COUNT)), Arrays.asList("value"));
        when(mockDelegate.getStatisticsCollectionMetadataForWrite(any(ConnectorSession.class),
                any(ConnectorTableMetadata.class))).thenReturn(tableStatisticsMetadata);

        // Run the test
        final TableStatisticsMetadata result = classLoaderSafeConnectorMetadataUnderTest.getStatisticsCollectionMetadataForWrite(
                session, tableMetadata);

        // Verify the results
    }

    @Test
    public void testGetStatisticsCollectionMetadata() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schemaName", "tableName"), Arrays.asList(
                        new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)),
                new HashMap<>(), Optional.of("value"), Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), Optional.of(new HashSet<>(
                Arrays.asList("value"))));

        // Configure ConnectorMetadata.getStatisticsCollectionMetadata(...).
        final TableStatisticsMetadata tableStatisticsMetadata = new TableStatisticsMetadata(new HashSet<>(
                Arrays.asList(new ColumnStatisticMetadata("columnName", ColumnStatisticType.MIN_VALUE))), new HashSet<>(
                Arrays.asList(TableStatisticType.ROW_COUNT)), Arrays.asList("value"));
        when(mockDelegate.getStatisticsCollectionMetadata(any(ConnectorSession.class),
                any(ConnectorTableMetadata.class))).thenReturn(tableStatisticsMetadata);

        // Run the test
        final TableStatisticsMetadata result = classLoaderSafeConnectorMetadataUnderTest.getStatisticsCollectionMetadata(
                session, tableMetadata);

        // Verify the results
    }

    @Test
    public void testBeginStatisticsCollection()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.beginStatisticsCollection(any(ConnectorSession.class),
                any(ConnectorTableHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorTableHandle result = classLoaderSafeConnectorMetadataUnderTest.beginStatisticsCollection(session,
                tableHandle);

        // Verify the results
    }

    @Test
    public void testFinishStatisticsCollection() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.finishStatisticsCollection(session, tableHandle, computedStatistics);

        // Verify the results
        verify(mockDelegate).finishStatisticsCollection(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null))));
    }

    @Test
    public void testSchemaExists()
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.schemaExists(any(ConnectorSession.class), eq("schemaName"))).thenReturn(false);

        // Run the test
        final boolean result = classLoaderSafeConnectorMetadataUnderTest.schemaExists(session, "schemaName");

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testListSchemaNames() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.listSchemaNames(any(ConnectorSession.class))).thenReturn(Arrays.asList("value"));

        // Run the test
        final List<String> result = classLoaderSafeConnectorMetadataUnderTest.listSchemaNames(session);

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testListSchemaNames_ConnectorMetadataReturnsNoItems() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.listSchemaNames(any(ConnectorSession.class))).thenReturn(Collections.emptyList());

        // Run the test
        final List<String> result = classLoaderSafeConnectorMetadataUnderTest.listSchemaNames(session);

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testGetTableHandle() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        when(mockDelegate.getTableHandle(any(ConnectorSession.class),
                eq(new SchemaTableName("schemaName", "tableName")))).thenReturn(null);

        // Run the test
        final ConnectorTableHandle result = classLoaderSafeConnectorMetadataUnderTest.getTableHandle(session,
                tableName);

        // Verify the results
    }

    @Test
    public void testGetTableHandle_ConnectorMetadataReturnsNull() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        when(mockDelegate.getTableHandle(any(ConnectorSession.class),
                eq(new SchemaTableName("schemaName", "tableName")))).thenReturn(null);

        // Run the test
        final ConnectorTableHandle result = classLoaderSafeConnectorMetadataUnderTest.getTableHandle(session,
                tableName);

        // Verify the results
        assertNull(result);
    }

    @Test
    public void testGetTableHandleForStatisticsCollection() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final Map<String, Object> analyzeProperties = new HashMap<>();
        when(mockDelegate.getTableHandleForStatisticsCollection(any(ConnectorSession.class),
                eq(new SchemaTableName("schemaName", "tableName")), eq(new HashMap<>()))).thenReturn(null);

        // Run the test
        final ConnectorTableHandle result = classLoaderSafeConnectorMetadataUnderTest.getTableHandleForStatisticsCollection(
                session, tableName, analyzeProperties);

        // Verify the results
    }

    @Test
    public void testGetTableHandleForStatisticsCollection_ConnectorMetadataReturnsNull() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final Map<String, Object> analyzeProperties = new HashMap<>();
        when(mockDelegate.getTableHandleForStatisticsCollection(any(ConnectorSession.class),
                eq(new SchemaTableName("schemaName", "tableName")), eq(new HashMap<>()))).thenReturn(null);

        // Run the test
        final ConnectorTableHandle result = classLoaderSafeConnectorMetadataUnderTest.getTableHandleForStatisticsCollection(
                session, tableName, analyzeProperties);

        // Verify the results
        assertNull(result);
    }

    @Test
    public void testGetSystemTable() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        when(mockDelegate.getSystemTable(any(ConnectorSession.class),
                eq(new SchemaTableName("schemaName", "tableName")))).thenReturn(Optional.empty());

        // Run the test
        final Optional<SystemTable> result = classLoaderSafeConnectorMetadataUnderTest.getSystemTable(session,
                tableName);

        // Verify the results
    }

    @Test
    public void testGetSystemTable_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        when(mockDelegate.getSystemTable(any(ConnectorSession.class),
                eq(new SchemaTableName("schemaName", "tableName")))).thenReturn(Optional.empty());

        // Run the test
        final Optional<SystemTable> result = classLoaderSafeConnectorMetadataUnderTest.getSystemTable(session,
                tableName);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetTableMetadata() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;

        // Configure ConnectorMetadata.getTableMetadata(...).
        final ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schemaName", "tableName"), Arrays.asList(
                        new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)),
                new HashMap<>(), Optional.of("value"), Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), Optional.of(new HashSet<>(
                Arrays.asList("value"))));
        when(mockDelegate.getTableMetadata(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(connectorTableMetadata);

        // Run the test
        final ConnectorTableMetadata result = classLoaderSafeConnectorMetadataUnderTest.getTableMetadata(session,
                table);

        // Verify the results
    }

    @Test
    public void testGetTableMetadata_ConnectorMetadataThrowsRuntimeException()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;
        when(mockDelegate.getTableMetadata(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenThrow(RuntimeException.class);

        // Run the test
        assertThrows(RuntimeException.class,
                () -> classLoaderSafeConnectorMetadataUnderTest.getTableMetadata(session, table));
    }

    @Test
    public void testGetInfo1()
    {
        // Setup
        final ConnectorTableLayoutHandle table = null;
        when(mockDelegate.getInfo(any(ConnectorTableLayoutHandle.class))).thenReturn(Optional.of("value"));

        // Run the test
        final Optional<Object> result = classLoaderSafeConnectorMetadataUnderTest.getInfo(table);

        // Verify the results
    }

    @Test
    public void testGetInfo1_ConnectorMetadataReturnsAbsent()
    {
        // Setup
        final ConnectorTableLayoutHandle table = null;
        when(mockDelegate.getInfo(any(ConnectorTableLayoutHandle.class))).thenReturn(Optional.empty());

        // Run the test
        final Optional<Object> result = classLoaderSafeConnectorMetadataUnderTest.getInfo(table);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetInfo1_ConnectorMetadataThrowsRuntimeException()
    {
        // Setup
        final ConnectorTableLayoutHandle table = null;
        when(mockDelegate.getInfo(any(ConnectorTableLayoutHandle.class))).thenThrow(RuntimeException.class);

        // Run the test
        assertThrows(RuntimeException.class, () -> classLoaderSafeConnectorMetadataUnderTest.getInfo(table));
    }

    @Test
    public void testGetInfo2()
    {
        // Setup
        final ConnectorTableHandle table = null;
        when(mockDelegate.getInfo(any(ConnectorTableHandle.class))).thenReturn(Optional.of("value"));

        // Run the test
        final Optional<Object> result = classLoaderSafeConnectorMetadataUnderTest.getInfo(table);

        // Verify the results
    }

    @Test
    public void testGetInfo2_ConnectorMetadataReturnsAbsent()
    {
        // Setup
        final ConnectorTableHandle table = null;
        when(mockDelegate.getInfo(any(ConnectorTableHandle.class))).thenReturn(Optional.empty());

        // Run the test
        final Optional<Object> result = classLoaderSafeConnectorMetadataUnderTest.getInfo(table);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testListTables() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure ConnectorMetadata.listTables(...).
        final List<SchemaTableName> schemaTableNames = Arrays.asList(new SchemaTableName("schemaName", "tableName"));
        when(mockDelegate.listTables(any(ConnectorSession.class), eq(Optional.of("value"))))
                .thenReturn(schemaTableNames);

        // Run the test
        final List<SchemaTableName> result = classLoaderSafeConnectorMetadataUnderTest.listTables(session,
                Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTables_ConnectorMetadataReturnsNoItems() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.listTables(any(ConnectorSession.class), eq(Optional.of("value"))))
                .thenReturn(Collections.emptyList());

        // Run the test
        final List<SchemaTableName> result = classLoaderSafeConnectorMetadataUnderTest.listTables(session,
                Optional.of("value"));

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testGetColumnHandles() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.getColumnHandles(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(new HashMap<>());

        // Run the test
        final Map<String, ColumnHandle> result = classLoaderSafeConnectorMetadataUnderTest.getColumnHandles(session,
                tableHandle);

        // Verify the results
    }

    @Test
    public void testGetColumnHandles_ConnectorMetadataThrowsRuntimeException()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.getColumnHandles(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenThrow(RuntimeException.class);

        // Run the test
        assertThrows(RuntimeException.class,
                () -> classLoaderSafeConnectorMetadataUnderTest.getColumnHandles(session, tableHandle));
    }

    @Test
    public void testGetColumnMetadata() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ColumnHandle columnHandle = null;
        final ColumnMetadata expectedResult = new ColumnMetadata("name", null, false, "comment", "extraInfo", false,
                new HashMap<>(), false);

        // Configure ConnectorMetadata.getColumnMetadata(...).
        final ColumnMetadata columnMetadata = new ColumnMetadata("name", null, false, "comment", "extraInfo", false,
                new HashMap<>(), false);
        when(mockDelegate.getColumnMetadata(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(ColumnHandle.class))).thenReturn(columnMetadata);

        // Run the test
        final ColumnMetadata result = classLoaderSafeConnectorMetadataUnderTest.getColumnMetadata(session, tableHandle,
                columnHandle);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetColumnMetadata_ConnectorMetadataThrowsRuntimeException()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ColumnHandle columnHandle = null;
        when(mockDelegate.getColumnMetadata(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(ColumnHandle.class))).thenThrow(RuntimeException.class);

        // Run the test
        assertThrows(RuntimeException.class,
                () -> classLoaderSafeConnectorMetadataUnderTest.getColumnMetadata(session, tableHandle, columnHandle));
    }

    @Test
    public void testListTableColumns() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTablePrefix prefix = new SchemaTablePrefix("schemaName", "tableName");
        final Map<SchemaTableName, List<ColumnMetadata>> expectedResult = new HashMap<>();
        when(mockDelegate.listTableColumns(any(ConnectorSession.class),
                eq(new SchemaTablePrefix("schemaName", "tableName")))).thenReturn(new HashMap<>());

        // Run the test
        final Map<SchemaTableName, List<ColumnMetadata>> result = classLoaderSafeConnectorMetadataUnderTest.listTableColumns(
                session, prefix);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetTableStatistics() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });
        final TableStatistics expectedResult = new TableStatistics(Estimate.of(0.0), 0L, 0L, new HashMap<>());

        // Configure ConnectorMetadata.getTableStatistics(...).
        final TableStatistics tableStatistics = new TableStatistics(Estimate.of(0.0), 0L, 0L, new HashMap<>());
        when(mockDelegate.getTableStatistics(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(Constraint.class), eq(false))).thenReturn(tableStatistics);

        // Run the test
        final TableStatistics result = classLoaderSafeConnectorMetadataUnderTest.getTableStatistics(session,
                tableHandle, constraint, false);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testAddColumn() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ColumnMetadata column = new ColumnMetadata("name", null, false, "comment", "extraInfo", false,
                new HashMap<>(), false);

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.addColumn(session, tableHandle, column);

        // Verify the results
        verify(mockDelegate).addColumn(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)));
    }

    @Test
    public void testCreateSchema() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final Map<String, Object> properties = new HashMap<>();

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.createSchema(session, "schemaName", properties);

        // Verify the results
        verify(mockDelegate).createSchema(any(ConnectorSession.class), eq("schemaName"), eq(new HashMap<>()));
    }

    @Test
    public void testDropSchema() throws Exception
    {
        // Setup
        final ConnectorSession session = null;

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.dropSchema(session, "schemaName");

        // Verify the results
        verify(mockDelegate).dropSchema(any(ConnectorSession.class), eq("schemaName"));
    }

    @Test
    public void testDropSchema_ConnectorMetadataThrowsPrestoException() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        doThrow(PrestoException.class).when(mockDelegate).dropSchema(any(ConnectorSession.class), eq("schemaName"));

        // Run the test
        assertThrows(PrestoException.class,
                () -> classLoaderSafeConnectorMetadataUnderTest.dropSchema(session, "schemaName"));
    }

    @Test
    public void testRenameSchema() throws Exception
    {
        // Setup
        final ConnectorSession session = null;

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.renameSchema(session, "source", "target");

        // Verify the results
        verify(mockDelegate).renameSchema(any(ConnectorSession.class), eq("source"), eq("target"));
    }

    @Test
    public void testCreateTable() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schemaName", "tableName"), Arrays.asList(
                        new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)),
                new HashMap<>(), Optional.of("value"), Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), Optional.of(new HashSet<>(
                Arrays.asList("value"))));

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.createTable(session, tableMetadata, false);

        // Verify the results
        verify(mockDelegate).createTable(any(ConnectorSession.class), any(ConnectorTableMetadata.class), eq(false));
    }

    @Test
    public void testCreateTable_ConnectorMetadataThrowsPrestoException() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schemaName", "tableName"), Arrays.asList(
                        new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)),
                new HashMap<>(), Optional.of("value"), Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), Optional.of(new HashSet<>(
                Arrays.asList("value"))));
        doThrow(PrestoException.class).when(mockDelegate).createTable(any(ConnectorSession.class),
                any(ConnectorTableMetadata.class), eq(false));

        // Run the test
        assertThrows(PrestoException.class,
                () -> classLoaderSafeConnectorMetadataUnderTest.createTable(session, tableMetadata, false));
    }

    @Test
    public void testDropTable() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.dropTable(session, tableHandle);

        // Verify the results
        verify(mockDelegate).dropTable(any(ConnectorSession.class), any(ConnectorTableHandle.class));
    }

    @Test
    public void testDropTable_ConnectorMetadataThrowsRuntimeException()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        doThrow(RuntimeException.class).when(mockDelegate).dropTable(any(ConnectorSession.class),
                any(ConnectorTableHandle.class));

        // Run the test
        assertThrows(RuntimeException.class,
                () -> classLoaderSafeConnectorMetadataUnderTest.dropTable(session, tableHandle));
    }

    @Test
    public void testRenameColumn() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ColumnHandle source = null;

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.renameColumn(session, tableHandle, source, "target");

        // Verify the results
        verify(mockDelegate).renameColumn(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(ColumnHandle.class), eq("target"));
    }

    @Test
    public void testDropColumn() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ColumnHandle column = null;

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.dropColumn(session, tableHandle, column);

        // Verify the results
        verify(mockDelegate).dropColumn(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(ColumnHandle.class));
    }

    @Test
    public void testRenameTable() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final SchemaTableName newTableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.renameTable(session, tableHandle, newTableName);

        // Verify the results
        verify(mockDelegate).renameTable(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(new SchemaTableName("schemaName", "tableName")));
    }

    @Test
    public void testSetTableComment() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.setTableComment(session, tableHandle, Optional.of("value"));

        // Verify the results
        verify(mockDelegate).setTableComment(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(Optional.of("value")));
    }

    @Test
    public void testBeginCreateTable() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schemaName", "tableName"), Arrays.asList(
                        new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)),
                new HashMap<>(), Optional.of("value"), Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), Optional.of(new HashSet<>(
                Arrays.asList("value"))));
        final Optional<ConnectorNewTableLayout> layout = Optional.of(
                new ConnectorNewTableLayout(null, Arrays.asList("value")));
        when(mockDelegate.beginCreateTable(any(ConnectorSession.class), any(ConnectorTableMetadata.class),
                eq(Optional.of(new ConnectorNewTableLayout(null, Arrays.asList("value")))))).thenReturn(null);

        // Run the test
        final ConnectorOutputTableHandle result = classLoaderSafeConnectorMetadataUnderTest.beginCreateTable(session,
                tableMetadata, layout);

        // Verify the results
    }

    @Test
    public void testFinishCreateTable() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorOutputTableHandle tableHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));
        when(mockDelegate.finishCreateTable(any(ConnectorSession.class),
                any(ConnectorOutputTableHandle.class), eq(Arrays.asList()),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null)))))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorOutputMetadata> result = classLoaderSafeConnectorMetadataUnderTest.finishCreateTable(
                session, tableHandle, fragments, computedStatistics);

        // Verify the results
    }

    @Test
    public void testFinishCreateTable_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorOutputTableHandle tableHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));
        when(mockDelegate.finishCreateTable(any(ConnectorSession.class), any(ConnectorOutputTableHandle.class),
                eq(Arrays.asList()),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null)))))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorOutputMetadata> result = classLoaderSafeConnectorMetadataUnderTest.finishCreateTable(
                session, tableHandle, fragments, computedStatistics);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testBeginQuery() throws Exception
    {
        // Setup
        final ConnectorSession session = null;

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.beginQuery(session);

        // Verify the results
        verify(mockDelegate).beginQuery(any(ConnectorSession.class));
    }

    @Test
    public void testCleanupQuery()
    {
        // Setup
        final ConnectorSession session = null;

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.cleanupQuery(session);

        // Verify the results
        verify(mockDelegate).cleanupQuery(any(ConnectorSession.class));
    }

    @Test
    public void testBeginInsert1() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.beginInsert(any(ConnectorSession.class), any(ConnectorTableHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorInsertTableHandle result = classLoaderSafeConnectorMetadataUnderTest.beginInsert(session,
                tableHandle);

        // Verify the results
    }

    @Test
    public void testBeginInsert2() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.beginInsert(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(false))).thenReturn(null);

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.beginInsert(session,
                tableHandle, false);

        // Verify the results
    }

    @Test
    public void testBeginUpdateAsInsert() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.beginUpdateAsInsert(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(null);

        // Run the test
        final ConnectorUpdateTableHandle result = classLoaderSafeConnectorMetadataUnderTest.beginUpdateAsInsert(session,
                tableHandle);

        // Verify the results
    }

    @Test
    public void testBeginUpdate() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final List<Type> updatedColumnTypes = Arrays.asList();
        when(mockDelegate.beginUpdate(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(Arrays.asList()))).thenReturn(null);

        // Run the test
        final ConnectorTableHandle result = classLoaderSafeConnectorMetadataUnderTest.beginUpdate(session, tableHandle,
                updatedColumnTypes);

        // Verify the results
    }

    @Test
    public void testFinishUpdate() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final Collection<Slice> fragments = Arrays.asList();

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.finishUpdate(session, tableHandle, fragments);

        // Verify the results
        verify(mockDelegate).finishUpdate(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(Arrays.asList()));
    }

    @Test
    public void testBeginDeletesAsInsert() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.beginDeletesAsInsert(any(ConnectorSession.class),
                any(ConnectorTableHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorDeleteAsInsertTableHandle result = classLoaderSafeConnectorMetadataUnderTest.beginDeletesAsInsert(
                session, tableHandle);

        // Verify the results
    }

    @Test
    public void testFinishDeleteAsInsert() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorDeleteAsInsertTableHandle updateHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));
        when(mockDelegate.finishDeleteAsInsert(any(ConnectorSession.class),
                any(ConnectorDeleteAsInsertTableHandle.class), eq(Arrays.asList()),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null)))))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorOutputMetadata> result = classLoaderSafeConnectorMetadataUnderTest.finishDeleteAsInsert(
                session, updateHandle, fragments, computedStatistics);

        // Verify the results
    }

    @Test
    public void testFinishDeleteAsInsert_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorDeleteAsInsertTableHandle updateHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));
        when(mockDelegate.finishDeleteAsInsert(any(ConnectorSession.class),
                any(ConnectorDeleteAsInsertTableHandle.class),
                eq(Arrays.asList()),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null)))))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorOutputMetadata> result = classLoaderSafeConnectorMetadataUnderTest.finishDeleteAsInsert(
                session, updateHandle, fragments, computedStatistics);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testFinishUpdateAsInsert() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorUpdateTableHandle updateHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));
        when(mockDelegate.finishUpdateAsInsert(any(ConnectorSession.class),
                any(ConnectorUpdateTableHandle.class), eq(Arrays.asList()),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null)))))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorOutputMetadata> result = classLoaderSafeConnectorMetadataUnderTest.finishUpdateAsInsert(
                session, updateHandle, fragments, computedStatistics);

        // Verify the results
    }

    @Test
    public void testFinishUpdateAsInsert_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorUpdateTableHandle updateHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));
        when(mockDelegate.finishUpdateAsInsert(any(ConnectorSession.class), any(ConnectorUpdateTableHandle.class),
                eq(Arrays.asList()),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null)))))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorOutputMetadata> result = classLoaderSafeConnectorMetadataUnderTest.finishUpdateAsInsert(
                session, updateHandle, fragments, computedStatistics);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testFinishInsert() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorInsertTableHandle insertHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));
        when(mockDelegate.finishInsert(any(ConnectorSession.class), any(ConnectorInsertTableHandle.class),
                eq(Arrays.asList()),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null)))))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorOutputMetadata> result = classLoaderSafeConnectorMetadataUnderTest.finishInsert(session,
                insertHandle, fragments, computedStatistics);

        // Verify the results
    }

    @Test
    public void testFinishInsert_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorInsertTableHandle insertHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));
        when(mockDelegate.finishInsert(any(ConnectorSession.class), any(ConnectorInsertTableHandle.class),
                eq(Arrays.asList()),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null)))))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorOutputMetadata> result = classLoaderSafeConnectorMetadataUnderTest.finishInsert(session,
                insertHandle, fragments, computedStatistics);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testBeginVacuum()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.beginVacuum(any(ConnectorSession.class), any(ConnectorTableHandle.class), eq(false),
                eq(false),
                eq(Optional.of("value")))).thenReturn(null);

        // Run the test
        final ConnectorVacuumTableHandle result = classLoaderSafeConnectorMetadataUnderTest.beginVacuum(session,
                tableHandle, false, false,
                Optional.of("value"));

        // Verify the results
    }

    @Test
    public void testFinishVacuum()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorVacuumTableHandle vacuumHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));
        when(mockDelegate.finishVacuum(any(ConnectorSession.class), any(ConnectorVacuumTableHandle.class),
                eq(Arrays.asList()),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null)))))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorOutputMetadata> result = classLoaderSafeConnectorMetadataUnderTest.finishVacuum(session,
                vacuumHandle, fragments, computedStatistics);

        // Verify the results
    }

    @Test
    public void testFinishVacuum_ConnectorMetadataReturnsAbsent()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorVacuumTableHandle vacuumHandle = null;
        final Collection<Slice> fragments = Arrays.asList();
        final Collection<ComputedStatistics> computedStatistics = Arrays.asList(
                ComputedStatistics.restoreComputedStatistics("state", null));
        when(mockDelegate.finishVacuum(any(ConnectorSession.class), any(ConnectorVacuumTableHandle.class),
                eq(Arrays.asList()),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null)))))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorOutputMetadata> result = classLoaderSafeConnectorMetadataUnderTest.finishVacuum(session,
                vacuumHandle, fragments, computedStatistics);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testCreateView() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"), false);

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.createView(session, viewName, definition, false);

        // Verify the results
        verify(mockDelegate).createView(any(ConnectorSession.class), eq(new SchemaTableName("schemaName", "tableName")),
                any(ConnectorViewDefinition.class), eq(false));
    }

    @Test
    public void testDropView() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.dropView(session, viewName);

        // Verify the results
        verify(mockDelegate).dropView(any(ConnectorSession.class), eq(new SchemaTableName("schemaName", "tableName")));
    }

    @Test
    public void testListViews() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure ConnectorMetadata.listViews(...).
        final List<SchemaTableName> schemaTableNames = Arrays.asList(new SchemaTableName("schemaName", "tableName"));
        when(mockDelegate.listViews(any(ConnectorSession.class), eq(Optional.of("value"))))
                .thenReturn(schemaTableNames);

        // Run the test
        final List<SchemaTableName> result = classLoaderSafeConnectorMetadataUnderTest.listViews(session,
                Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListViews_ConnectorMetadataReturnsNoItems() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.listViews(any(ConnectorSession.class), eq(Optional.of("value"))))
                .thenReturn(Collections.emptyList());

        // Run the test
        final List<SchemaTableName> result = classLoaderSafeConnectorMetadataUnderTest.listViews(session,
                Optional.of("value"));

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testGetViews() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.getViews(any(ConnectorSession.class), eq(Optional.of("value")))).thenReturn(new HashMap<>());

        // Run the test
        final Map<SchemaTableName, ConnectorViewDefinition> result = classLoaderSafeConnectorMetadataUnderTest.getViews(
                session,
                Optional.of("value"));

        // Verify the results
    }

    @Test
    public void testGetView() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Configure ConnectorMetadata.getView(...).
        final Optional<ConnectorViewDefinition> connectorViewDefinition = Optional.of(
                new ConnectorViewDefinition("originalSql", Optional.of("value"), Optional.of("value"), Arrays.asList(
                        new ConnectorViewDefinition.ViewColumn("name",
                                new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"), false));
        when(mockDelegate.getView(any(ConnectorSession.class),
                eq(new SchemaTableName("schemaName", "tableName")))).thenReturn(connectorViewDefinition);

        // Run the test
        final Optional<ConnectorViewDefinition> result = classLoaderSafeConnectorMetadataUnderTest.getView(session,
                viewName);

        // Verify the results
    }

    @Test
    public void testGetView_ConnectorMetadataReturnsAbsent()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");
        when(mockDelegate.getView(any(ConnectorSession.class),
                eq(new SchemaTableName("schemaName", "tableName")))).thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorViewDefinition> result = classLoaderSafeConnectorMetadataUnderTest.getView(session,
                viewName);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetDeleteRowIdColumnHandle() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.getDeleteRowIdColumnHandle(any(ConnectorSession.class),
                any(ConnectorTableHandle.class))).thenReturn(null);

        // Run the test
        final ColumnHandle result = classLoaderSafeConnectorMetadataUnderTest.getDeleteRowIdColumnHandle(session,
                tableHandle);

        // Verify the results
    }

    @Test
    public void testGetUpdateRowIdColumnHandle() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final List<ColumnHandle> updatedColumns = Arrays.asList();
        when(mockDelegate.getUpdateRowIdColumnHandle(any(ConnectorSession.class),
                any(ConnectorTableHandle.class), eq(Arrays.asList()))).thenReturn(null);

        // Run the test
        final ColumnHandle result = classLoaderSafeConnectorMetadataUnderTest.getUpdateRowIdColumnHandle(session,
                tableHandle, updatedColumns);

        // Verify the results
    }

    @Test
    public void testBeginDelete() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.beginDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorTableHandle result = classLoaderSafeConnectorMetadataUnderTest.beginDelete(session, tableHandle);

        // Verify the results
    }

    @Test
    public void testFinishDelete() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final Collection<Slice> fragments = Arrays.asList();

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.finishDelete(session, tableHandle, fragments);

        // Verify the results
        verify(mockDelegate).finishDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(Arrays.asList()));
    }

    @Test
    public void testSupportsMetadataDelete() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ConnectorTableLayoutHandle tableLayoutHandle = null;
        when(mockDelegate.supportsMetadataDelete(any(ConnectorSession.class),
                any(ConnectorTableHandle.class), any(ConnectorTableLayoutHandle.class))).thenReturn(false);

        // Run the test
        final boolean result = classLoaderSafeConnectorMetadataUnderTest.supportsMetadataDelete(session, tableHandle,
                tableLayoutHandle);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testMetadataDelete() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ConnectorTableLayoutHandle tableLayoutHandle = null;
        final OptionalLong expectedResult = OptionalLong.of(0);
        when(mockDelegate.metadataDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(ConnectorTableLayoutHandle.class))).thenReturn(OptionalLong.of(0));

        // Run the test
        final OptionalLong result = classLoaderSafeConnectorMetadataUnderTest.metadataDelete(session, tableHandle,
                tableLayoutHandle);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testMetadataDelete_ConnectorMetadataReturnsNull() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ConnectorTableLayoutHandle tableLayoutHandle = null;
        when(mockDelegate.metadataDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(ConnectorTableLayoutHandle.class))).thenReturn(OptionalLong.empty());

        // Run the test
        final OptionalLong result = classLoaderSafeConnectorMetadataUnderTest.metadataDelete(session, tableHandle,
                tableLayoutHandle);

        // Verify the results
        assertNull(result);
    }

    @Test
    public void testApplyDelete1() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        when(mockDelegate.applyDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorTableHandle> result = classLoaderSafeConnectorMetadataUnderTest.applyDelete(session,
                handle);

        // Verify the results
    }

    @Test
    public void testApplyDelete1_ConnectorMetadataReturnsAbsent()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        when(mockDelegate.applyDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorTableHandle> result = classLoaderSafeConnectorMetadataUnderTest.applyDelete(session,
                handle);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testApplyDelete2() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });
        when(mockDelegate.applyDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(Constraint.class))).thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorTableHandle> result = classLoaderSafeConnectorMetadataUnderTest.applyDelete(session,
                handle, constraint);

        // Verify the results
    }

    @Test
    public void testApplyDelete2_ConnectorMetadataReturnsAbsent()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });
        when(mockDelegate.applyDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(Constraint.class))).thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorTableHandle> result = classLoaderSafeConnectorMetadataUnderTest.applyDelete(session,
                handle, constraint);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testExecuteDelete() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        final OptionalLong expectedResult = OptionalLong.of(0);
        when(mockDelegate.executeDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(OptionalLong.of(0));

        // Run the test
        final OptionalLong result = classLoaderSafeConnectorMetadataUnderTest.executeDelete(session, handle);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testExecuteDelete_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        when(mockDelegate.executeDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(OptionalLong.empty());

        // Run the test
        final OptionalLong result = classLoaderSafeConnectorMetadataUnderTest.executeDelete(session, handle);

        // Verify the results
        assertEquals(OptionalLong.empty(), result);
    }

    @Test
    public void testExecuteUpdate() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        final OptionalLong expectedResult = OptionalLong.of(0);
        when(mockDelegate.executeUpdate(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(OptionalLong.of(0));

        // Run the test
        final OptionalLong result = classLoaderSafeConnectorMetadataUnderTest.executeUpdate(session, handle);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testExecuteUpdate_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        when(mockDelegate.executeUpdate(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(OptionalLong.empty());

        // Run the test
        final OptionalLong result = classLoaderSafeConnectorMetadataUnderTest.executeUpdate(session, handle);

        // Verify the results
        assertEquals(OptionalLong.empty(), result);
    }

    @Test
    public void testResolveIndex() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final Set<ColumnHandle> indexableColumns = new HashSet<>();
        final Set<ColumnHandle> outputColumns = new HashSet<>();
        final TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(new HashMap<>());

        // Configure ConnectorMetadata.resolveIndex(...).
        final Optional<ConnectorResolvedIndex> connectorResolvedIndex = Optional.of(
                new ConnectorResolvedIndex(null, TupleDomain.withColumnDomains(new HashMap<>())));
        when(mockDelegate.resolveIndex(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(new HashSet<>()), eq(new HashSet<>()),
                eq(TupleDomain.withColumnDomains(new HashMap<>())))).thenReturn(connectorResolvedIndex);

        // Run the test
        final Optional<ConnectorResolvedIndex> result = classLoaderSafeConnectorMetadataUnderTest.resolveIndex(session,
                tableHandle, indexableColumns, outputColumns, tupleDomain);

        // Verify the results
    }

    @Test
    public void testResolveIndex_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final Set<ColumnHandle> indexableColumns = new HashSet<>();
        final Set<ColumnHandle> outputColumns = new HashSet<>();
        final TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(new HashMap<>());
        when(mockDelegate.resolveIndex(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(new HashSet<>()), eq(new HashSet<>()),
                eq(TupleDomain.withColumnDomains(new HashMap<>())))).thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorResolvedIndex> result = classLoaderSafeConnectorMetadataUnderTest.resolveIndex(session,
                tableHandle, indexableColumns, outputColumns, tupleDomain);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testCreateRole() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final Optional<PrestoPrincipal> grantor = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.createRole(session, "role", grantor);

        // Verify the results
        verify(mockDelegate).createRole(any(ConnectorSession.class), eq("role"),
                eq(Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"))));
    }

    @Test
    public void testDropRole() throws Exception
    {
        // Setup
        final ConnectorSession session = null;

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.dropRole(session, "role");

        // Verify the results
        verify(mockDelegate).dropRole(any(ConnectorSession.class), eq("role"));
    }

    @Test
    public void testListRoles() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.listRoles(any(ConnectorSession.class))).thenReturn(new HashSet<>(Arrays.asList("value")));

        // Run the test
        final Set<String> result = classLoaderSafeConnectorMetadataUnderTest.listRoles(session);

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList("value")), result);
    }

    @Test
    public void testListRoles_ConnectorMetadataReturnsNoItems() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.listRoles(any(ConnectorSession.class))).thenReturn(Collections.emptySet());

        // Run the test
        final Set<String> result = classLoaderSafeConnectorMetadataUnderTest.listRoles(session);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testListRoleGrants() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, "name");
        final Set<RoleGrant> expectedResult = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "roleName", false)));

        // Configure ConnectorMetadata.listRoleGrants(...).
        final Set<RoleGrant> roleGrants = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "roleName", false)));
        when(mockDelegate.listRoleGrants(any(ConnectorSession.class),
                eq(new PrestoPrincipal(PrincipalType.USER, "name")))).thenReturn(roleGrants);

        // Run the test
        final Set<RoleGrant> result = classLoaderSafeConnectorMetadataUnderTest.listRoleGrants(session, principal);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListRoleGrants_ConnectorMetadataReturnsNoItems() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, "name");
        when(mockDelegate.listRoleGrants(any(ConnectorSession.class),
                eq(new PrestoPrincipal(PrincipalType.USER, "name")))).thenReturn(Collections.emptySet());

        // Run the test
        final Set<RoleGrant> result = classLoaderSafeConnectorMetadataUnderTest.listRoleGrants(session, principal);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testGrantRoles() throws Exception
    {
        // Setup
        final ConnectorSession connectorSession = null;
        final Set<PrestoPrincipal> grantees = new HashSet<>(
                Arrays.asList(new PrestoPrincipal(PrincipalType.USER, "name")));
        final Optional<PrestoPrincipal> grantor = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.grantRoles(connectorSession, new HashSet<>(Arrays.asList("value")),
                grantees, false, grantor);

        // Verify the results
        verify(mockDelegate).grantRoles(any(ConnectorSession.class), eq(new HashSet<>(Arrays.asList("value"))),
                eq(new HashSet<>(
                        Arrays.asList(new PrestoPrincipal(PrincipalType.USER, "name")))), eq(false),
                eq(Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"))));
    }

    @Test
    public void testRevokeRoles() throws Exception
    {
        // Setup
        final ConnectorSession connectorSession = null;
        final Set<PrestoPrincipal> grantees = new HashSet<>(
                Arrays.asList(new PrestoPrincipal(PrincipalType.USER, "name")));
        final Optional<PrestoPrincipal> grantor = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.revokeRoles(connectorSession, new HashSet<>(Arrays.asList("value")),
                grantees, false, grantor);

        // Verify the results
        verify(mockDelegate).revokeRoles(any(ConnectorSession.class), eq(new HashSet<>(Arrays.asList("value"))),
                eq(new HashSet<>(
                        Arrays.asList(new PrestoPrincipal(PrincipalType.USER, "name")))), eq(false),
                eq(Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"))));
    }

    @Test
    public void testListApplicableRoles() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, "name");
        final Set<RoleGrant> expectedResult = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "roleName", false)));

        // Configure ConnectorMetadata.listApplicableRoles(...).
        final Set<RoleGrant> roleGrants = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "roleName", false)));
        when(mockDelegate.listApplicableRoles(any(ConnectorSession.class),
                eq(new PrestoPrincipal(PrincipalType.USER, "name")))).thenReturn(roleGrants);

        // Run the test
        final Set<RoleGrant> result = classLoaderSafeConnectorMetadataUnderTest.listApplicableRoles(session, principal);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListApplicableRoles_ConnectorMetadataReturnsNoItems() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, "name");
        when(mockDelegate.listApplicableRoles(any(ConnectorSession.class),
                eq(new PrestoPrincipal(PrincipalType.USER, "name")))).thenReturn(Collections.emptySet());

        // Run the test
        final Set<RoleGrant> result = classLoaderSafeConnectorMetadataUnderTest.listApplicableRoles(session, principal);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testListEnabledRoles() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.listEnabledRoles(any(ConnectorSession.class))).thenReturn(new HashSet<>(
                Arrays.asList("value")));

        // Run the test
        final Set<String> result = classLoaderSafeConnectorMetadataUnderTest.listEnabledRoles(session);

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList("value")), result);
    }

    @Test
    public void testListEnabledRoles_ConnectorMetadataReturnsNoItems() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.listEnabledRoles(any(ConnectorSession.class))).thenReturn(Collections.emptySet());

        // Run the test
        final Set<String> result = classLoaderSafeConnectorMetadataUnderTest.listEnabledRoles(session);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testGrantTablePrivileges() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal grantee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.grantTablePrivileges(session, tableName,
                new HashSet<>(Arrays.asList(Privilege.SELECT)), grantee, false);

        // Verify the results
        verify(mockDelegate).grantTablePrivileges(any(ConnectorSession.class),
                eq(new SchemaTableName("schemaName", "tableName")), eq(new HashSet<>(
                        Arrays.asList(Privilege.SELECT))), eq(new PrestoPrincipal(PrincipalType.USER, "name")),
                eq(false));
    }

    @Test
    public void testRevokeTablePrivileges() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal grantee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.revokeTablePrivileges(session, tableName,
                new HashSet<>(Arrays.asList(Privilege.SELECT)), grantee, false);

        // Verify the results
        verify(mockDelegate).revokeTablePrivileges(any(ConnectorSession.class),
                eq(new SchemaTableName("schemaName", "tableName")), eq(new HashSet<>(
                        Arrays.asList(Privilege.SELECT))), eq(new PrestoPrincipal(PrincipalType.USER, "name")),
                eq(false));
    }

    @Test
    public void testListTablePrivileges() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTablePrefix prefix = new SchemaTablePrefix("schemaName", "tableName");
        final List<GrantInfo> expectedResult = Arrays.asList(
                new GrantInfo(new PrivilegeInfo(Privilege.SELECT, false), new PrestoPrincipal(
                        PrincipalType.USER, "name"), new SchemaTableName("schemaName", "tableName"),
                        Optional.of(new PrestoPrincipal(
                                PrincipalType.USER, "name")), Optional.of(false)));

        // Configure ConnectorMetadata.listTablePrivileges(...).
        final List<GrantInfo> grantInfos = Arrays.asList(
                new GrantInfo(new PrivilegeInfo(Privilege.SELECT, false), new PrestoPrincipal(
                        PrincipalType.USER, "name"), new SchemaTableName("schemaName", "tableName"),
                        Optional.of(new PrestoPrincipal(
                                PrincipalType.USER, "name")), Optional.of(false)));
        when(mockDelegate.listTablePrivileges(any(ConnectorSession.class),
                eq(new SchemaTablePrefix("schemaName", "tableName")))).thenReturn(grantInfos);

        // Run the test
        final List<GrantInfo> result = classLoaderSafeConnectorMetadataUnderTest.listTablePrivileges(session, prefix);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTablePrivileges_ConnectorMetadataReturnsNoItems() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTablePrefix prefix = new SchemaTablePrefix("schemaName", "tableName");
        when(mockDelegate.listTablePrivileges(any(ConnectorSession.class),
                eq(new SchemaTablePrefix("schemaName", "tableName")))).thenReturn(Collections.emptyList());

        // Run the test
        final List<GrantInfo> result = classLoaderSafeConnectorMetadataUnderTest.listTablePrivileges(session, prefix);

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testUsesLegacyTableLayouts() throws Exception
    {
        // Setup
        when(mockDelegate.usesLegacyTableLayouts()).thenReturn(false);

        // Run the test
        final boolean result = classLoaderSafeConnectorMetadataUnderTest.usesLegacyTableLayouts();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetTableProperties() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;
        final ConnectorTableProperties expectedResult = new ConnectorTableProperties(
                TupleDomain.withColumnDomains(new HashMap<>()),
                Optional.of(new ConnectorTablePartitioning(null, Arrays.asList())), Optional.of(new HashSet<>()),
                Optional.of(new DiscretePredicates(
                        Arrays.asList(), Arrays.asList(TupleDomain.withColumnDomains(new HashMap<>())))),
                Arrays.asList());

        // Configure ConnectorMetadata.getTableProperties(...).
        final ConnectorTableProperties connectorTableProperties = new ConnectorTableProperties(
                TupleDomain.withColumnDomains(new HashMap<>()),
                Optional.of(new ConnectorTablePartitioning(null, Arrays.asList())), Optional.of(new HashSet<>()),
                Optional.of(new DiscretePredicates(
                        Arrays.asList(), Arrays.asList(TupleDomain.withColumnDomains(new HashMap<>())))),
                Arrays.asList());
        when(mockDelegate.getTableProperties(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(connectorTableProperties);

        // Run the test
        final ConnectorTableProperties result = classLoaderSafeConnectorMetadataUnderTest.getTableProperties(session,
                table);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testApplyLimit() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;

        // Configure ConnectorMetadata.applyLimit(...).
        final Optional<LimitApplicationResult<ConnectorTableHandle>> connectorTableHandleLimitApplicationResult = Optional.of(
                new LimitApplicationResult<>(null, false));
        when(mockDelegate.applyLimit(any(ConnectorSession.class), any(ConnectorTableHandle.class), eq(0L)))
                .thenReturn(connectorTableHandleLimitApplicationResult);

        // Run the test
        final Optional<LimitApplicationResult<ConnectorTableHandle>> result = classLoaderSafeConnectorMetadataUnderTest.applyLimit(
                session, table, 0L);

        // Verify the results
    }

    @Test
    public void testApplyLimit_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;
        when(mockDelegate.applyLimit(any(ConnectorSession.class), any(ConnectorTableHandle.class), eq(0L)))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<LimitApplicationResult<ConnectorTableHandle>> result = classLoaderSafeConnectorMetadataUnderTest.applyLimit(
                session, table, 0L);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testApplyFilter1() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });

        // Configure ConnectorMetadata.applyFilter(...).
        final Optional<ConstraintApplicationResult<ConnectorTableHandle>> connectorTableHandleConstraintApplicationResult = Optional.of(
                new ConstraintApplicationResult<>(null, TupleDomain.withColumnDomains(new HashMap<>())));
        when(mockDelegate.applyFilter(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(Constraint.class))).thenReturn(connectorTableHandleConstraintApplicationResult);

        // Run the test
        final Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = classLoaderSafeConnectorMetadataUnderTest.applyFilter(
                session, table, constraint);

        // Verify the results
    }

    @Test
    public void testApplyFilter1_ConnectorMetadataReturnsAbsent()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });
        when(mockDelegate.applyFilter(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(Constraint.class))).thenReturn(Optional.empty());

        // Run the test
        final Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = classLoaderSafeConnectorMetadataUnderTest.applyFilter(
                session, table, constraint);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testApplyFilter2() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });
        final List<Constraint> disjuctConstaints = Arrays.asList(
                new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
                    return false;
                }));
        final Set<ColumnHandle> allColumnHandles = new HashSet<>();

        // Configure ConnectorMetadata.applyFilter(...).
        final Optional<ConstraintApplicationResult<ConnectorTableHandle>> connectorTableHandleConstraintApplicationResult = Optional.of(
                new ConstraintApplicationResult<>(null, TupleDomain.withColumnDomains(new HashMap<>())));
        when(mockDelegate.applyFilter(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(Constraint.class),
                eq(Arrays.asList(new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
                    return false;
                }))), eq(new HashSet<>()), eq(false)))
                .thenReturn(connectorTableHandleConstraintApplicationResult);

        // Run the test
        final Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = classLoaderSafeConnectorMetadataUnderTest.applyFilter(
                session, table, constraint, disjuctConstaints, allColumnHandles, false);

        // Verify the results
    }

    @Test
    public void testApplyFilter2_ConnectorMetadataReturnsAbsent()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });
        final List<Constraint> disjuctConstaints = Arrays.asList(
                new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
                    return false;
                }));
        final Set<ColumnHandle> allColumnHandles = new HashSet<>();
        when(mockDelegate.applyFilter(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(Constraint.class),
                eq(Arrays.asList(new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
                    return false;
                }))), eq(new HashSet<>()), eq(false))).thenReturn(Optional.empty());

        // Run the test
        final Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = classLoaderSafeConnectorMetadataUnderTest.applyFilter(
                session, table, constraint, disjuctConstaints, allColumnHandles, false);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testApplyProjection() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        final List<ConnectorExpression> projections = Arrays.asList();
        final Map<String, ColumnHandle> assignments = new HashMap<>();

        // Configure ConnectorMetadata.applyProjection(...).
        final Optional<ProjectionApplicationResult<ConnectorTableHandle>> connectorTableHandleProjectionApplicationResult = Optional.of(
                new ProjectionApplicationResult<>(null, Arrays.asList(),
                        Arrays.asList(new ProjectionApplicationResult.Assignment("variable", null, null))));
        when(mockDelegate.applyProjection(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(Arrays.asList()), eq(new HashMap<>()))).thenReturn(connectorTableHandleProjectionApplicationResult);

        // Run the test
        final Optional<ProjectionApplicationResult<ConnectorTableHandle>> result = classLoaderSafeConnectorMetadataUnderTest.applyProjection(
                session, handle, projections, assignments);

        // Verify the results
    }

    @Test
    public void testApplyProjection_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        final List<ConnectorExpression> projections = Arrays.asList();
        final Map<String, ColumnHandle> assignments = new HashMap<>();
        when(mockDelegate.applyProjection(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(Arrays.asList()), eq(new HashMap<>()))).thenReturn(Optional.empty());

        // Run the test
        final Optional<ProjectionApplicationResult<ConnectorTableHandle>> result = classLoaderSafeConnectorMetadataUnderTest.applyProjection(
                session, handle, projections, assignments);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testApplySample() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        when(mockDelegate.applySample(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(SampleType.SYSTEM), eq(0.0))).thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorTableHandle> result = classLoaderSafeConnectorMetadataUnderTest.applySample(session,
                handle,
                SampleType.SYSTEM, 0.0);

        // Verify the results
    }

    @Test
    public void testApplySample_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        when(mockDelegate.applySample(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(SampleType.SYSTEM), eq(0.0))).thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorTableHandle> result = classLoaderSafeConnectorMetadataUnderTest.applySample(session,
                handle,
                SampleType.SYSTEM, 0.0);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetTableModificationTime()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.getTableModificationTime(any(ConnectorSession.class),
                any(ConnectorTableHandle.class))).thenReturn(0L);

        // Run the test
        final long result = classLoaderSafeConnectorMetadataUnderTest.getTableModificationTime(session, tableHandle);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testIsExecutionPlanCacheSupported() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        when(mockDelegate.isExecutionPlanCacheSupported(any(ConnectorSession.class),
                any(ConnectorTableHandle.class))).thenReturn(false);

        // Run the test
        final boolean result = classLoaderSafeConnectorMetadataUnderTest.isExecutionPlanCacheSupported(session, handle);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsHeuristicIndexSupported()
    {
        // Setup
        when(mockDelegate.isHeuristicIndexSupported()).thenReturn(false);

        // Run the test
        final boolean result = classLoaderSafeConnectorMetadataUnderTest.isHeuristicIndexSupported();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetTablesForVacuum()
    {
        // Setup
        // Configure ConnectorMetadata.getTablesForVacuum(...).
        final List<ConnectorVacuumTableInfo> connectorVacuumTableInfos = Arrays.asList(
                new ConnectorVacuumTableInfo("schemaTableName", false));
        when(mockDelegate.getTablesForVacuum()).thenReturn(connectorVacuumTableInfos);

        // Run the test
        final List<ConnectorVacuumTableInfo> result = classLoaderSafeConnectorMetadataUnderTest.getTablesForVacuum();

        // Verify the results
    }

    @Test
    public void testGetTablesForVacuum_ConnectorMetadataReturnsNoItems()
    {
        // Setup
        when(mockDelegate.getTablesForVacuum()).thenReturn(Collections.emptyList());

        // Run the test
        final List<ConnectorVacuumTableInfo> result = classLoaderSafeConnectorMetadataUnderTest.getTablesForVacuum();

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testIsPreAggregationSupported() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.isPreAggregationSupported(any(ConnectorSession.class))).thenReturn(false);

        // Run the test
        final boolean result = classLoaderSafeConnectorMetadataUnderTest.isPreAggregationSupported(session);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsSnapshotSupportedAsInput() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;
        when(mockDelegate.isSnapshotSupportedAsInput(any(ConnectorSession.class),
                any(ConnectorTableHandle.class))).thenReturn(false);

        // Run the test
        final boolean result = classLoaderSafeConnectorMetadataUnderTest.isSnapshotSupportedAsInput(session, table);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsSnapshotSupportedAsOutput() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle table = null;
        when(mockDelegate.isSnapshotSupportedAsOutput(any(ConnectorSession.class),
                any(ConnectorTableHandle.class))).thenReturn(false);

        // Run the test
        final boolean result = classLoaderSafeConnectorMetadataUnderTest.isSnapshotSupportedAsOutput(session, table);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsSnapshotSupportedAsNewTable()
    {
        // Setup
        final ConnectorSession session = null;
        final Map<String, Object> tableProperties = new HashMap<>();
        when(mockDelegate.isSnapshotSupportedAsNewTable(any(ConnectorSession.class), eq(new HashMap<>())))
                .thenReturn(false);

        // Run the test
        final boolean result = classLoaderSafeConnectorMetadataUnderTest.isSnapshotSupportedAsNewTable(session,
                tableProperties);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testResetInsertForRerun() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorInsertTableHandle tableHandle = null;
        final OptionalLong snapshotIndex = OptionalLong.of(0);

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.resetInsertForRerun(session, tableHandle, snapshotIndex);

        // Verify the results
        verify(mockDelegate).resetInsertForRerun(any(ConnectorSession.class), any(ConnectorInsertTableHandle.class),
                eq(OptionalLong.of(0)));
    }

    @Test
    public void testResetCreateForRerun() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorOutputTableHandle tableHandle = null;
        final OptionalLong snapshotIndex = OptionalLong.of(0);

        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.resetCreateForRerun(session, tableHandle, snapshotIndex);

        // Verify the results
        verify(mockDelegate).resetCreateForRerun(any(ConnectorSession.class), any(ConnectorOutputTableHandle.class),
                eq(OptionalLong.of(0)));
    }

    @Test
    public void testValidateAndGetSortAggregationType() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;

        // Configure ConnectorMetadata.validateAndGetSortAggregationType(...).
        final PartialAndFinalAggregationType partialAndFinalAggregationType = new PartialAndFinalAggregationType();
        partialAndFinalAggregationType.setSortAggregation(false);
        partialAndFinalAggregationType.setPartialAsSortAndFinalAsHashAggregation(false);
        when(mockDelegate.validateAndGetSortAggregationType(any(ConnectorSession.class),
                any(ConnectorTableHandle.class),
                eq(Arrays.asList("value")))).thenReturn(partialAndFinalAggregationType);

        // Run the test
        final PartialAndFinalAggregationType result = classLoaderSafeConnectorMetadataUnderTest.validateAndGetSortAggregationType(
                session, tableHandle,
                Arrays.asList("value"));

        // Verify the results
    }

    @Test
    public void testRefreshMetadataCache() throws Exception
    {
        // Setup
        // Run the test
        classLoaderSafeConnectorMetadataUnderTest.refreshMetadataCache();

        // Verify the results
        verify(mockDelegate).refreshMetadataCache();
    }
}
