/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.spi.connector;

import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.prestosql.spi.PrestoException;
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
import java.util.concurrent.TimeUnit;

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

public class CachedConnectorMetadataTest
{
    @Mock
    private ConnectorMetadata mockDelegate;

    private CachedConnectorMetadata cachedConnectorMetadataUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        cachedConnectorMetadataUnderTest = new CachedConnectorMetadata(mockDelegate,
                new Duration(0.0, TimeUnit.MILLISECONDS), 0L);
    }

    @Test
    public void testLogAndDelegate() throws Exception
    {
        // Setup
        final CachedConnectorMetadata.WrapDelegateMethod<Object> m = null;

        // Run the test
        cachedConnectorMetadataUnderTest.logAndDelegate("delegateInfo", m);

        // Verify the results
    }

    @Test
    public void testSchemaExists()
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.schemaExists(any(ConnectorSession.class), eq("schemaName"))).thenReturn(false);

        // Run the test
        final boolean result = cachedConnectorMetadataUnderTest.schemaExists(session, "schemaName");

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
        final List<String> result = cachedConnectorMetadataUnderTest.listSchemaNames(session);

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
        final List<String> result = cachedConnectorMetadataUnderTest.listSchemaNames(session);

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
        final ConnectorTableHandle result = cachedConnectorMetadataUnderTest.getTableHandle(session, tableName);

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
        final ConnectorTableHandle result = cachedConnectorMetadataUnderTest.getTableHandle(session, tableName);

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
        final ConnectorTableHandle result = cachedConnectorMetadataUnderTest.getTableHandleForStatisticsCollection(
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
        final ConnectorTableHandle result = cachedConnectorMetadataUnderTest.getTableHandleForStatisticsCollection(
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
        final Optional<SystemTable> result = cachedConnectorMetadataUnderTest.getSystemTable(session, tableName);

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
        final Optional<SystemTable> result = cachedConnectorMetadataUnderTest.getSystemTable(session, tableName);

        // Verify the results
        assertEquals(Optional.empty(), result);
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
                any(Constraint.class),
                eq(Optional.of(new HashSet<>())))).thenReturn(connectorTableLayoutResults);

        // Run the test
        final List<ConnectorTableLayoutResult> result = cachedConnectorMetadataUnderTest.getTableLayouts(session, table,
                constraint, desiredColumns);

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
        final List<ConnectorTableLayoutResult> result = cachedConnectorMetadataUnderTest.getTableLayouts(session, table,
                constraint, desiredColumns);

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
        final ConnectorTableLayout result = cachedConnectorMetadataUnderTest.getTableLayout(session, handle);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testMakeCompatiblePartitioning1()
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableLayoutHandle tableLayoutHandle = null;
        final ConnectorPartitioningHandle partitioningHandle = null;
        when(mockDelegate.makeCompatiblePartitioning(any(ConnectorSession.class), any(ConnectorTableLayoutHandle.class),
                any(ConnectorPartitioningHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorTableLayoutHandle result = cachedConnectorMetadataUnderTest.makeCompatiblePartitioning(session,
                tableLayoutHandle, partitioningHandle);

        // Verify the results
    }

    @Test
    public void testMakeCompatiblePartitioning2() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ConnectorPartitioningHandle partitioningHandle = null;
        when(mockDelegate.makeCompatiblePartitioning(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(ConnectorPartitioningHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorTableHandle result = cachedConnectorMetadataUnderTest.makeCompatiblePartitioning(session,
                tableHandle, partitioningHandle);

        // Verify the results
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
        final Optional<ConnectorPartitioningHandle> result = cachedConnectorMetadataUnderTest.getCommonPartitioningHandle(
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
                any(ConnectorPartitioningHandle.class), any(ConnectorPartitioningHandle.class)))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorPartitioningHandle> result = cachedConnectorMetadataUnderTest.getCommonPartitioningHandle(
                session, left, right);

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
        final ConnectorTableMetadata result = cachedConnectorMetadataUnderTest.getTableMetadata(session, table);

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
        assertThrows(RuntimeException.class, () -> cachedConnectorMetadataUnderTest.getTableMetadata(session, table));
    }

    @Test
    public void testGetInfo1()
    {
        // Setup
        final ConnectorTableLayoutHandle layoutHandle = null;
        when(mockDelegate.getInfo(any(ConnectorTableLayoutHandle.class))).thenReturn(Optional.of("value"));

        // Run the test
        final Optional<Object> result = cachedConnectorMetadataUnderTest.getInfo(layoutHandle);

        // Verify the results
    }

    @Test
    public void testGetInfo1_ConnectorMetadataReturnsAbsent()
    {
        // Setup
        final ConnectorTableLayoutHandle layoutHandle = null;
        when(mockDelegate.getInfo(any(ConnectorTableLayoutHandle.class))).thenReturn(Optional.empty());

        // Run the test
        final Optional<Object> result = cachedConnectorMetadataUnderTest.getInfo(layoutHandle);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetInfo1_ConnectorMetadataThrowsRuntimeException()
    {
        // Setup
        final ConnectorTableLayoutHandle layoutHandle = null;
        when(mockDelegate.getInfo(any(ConnectorTableLayoutHandle.class))).thenThrow(RuntimeException.class);

        // Run the test
        assertThrows(RuntimeException.class, () -> cachedConnectorMetadataUnderTest.getInfo(layoutHandle));
    }

    @Test
    public void testGetInfo2()
    {
        // Setup
        final ConnectorTableHandle table = null;
        when(mockDelegate.getInfo(any(ConnectorTableHandle.class))).thenReturn(Optional.of("value"));

        // Run the test
        final Optional<Object> result = cachedConnectorMetadataUnderTest.getInfo(table);

        // Verify the results
    }

    @Test
    public void testGetInfo2_ConnectorMetadataReturnsAbsent()
    {
        // Setup
        final ConnectorTableHandle table = null;
        when(mockDelegate.getInfo(any(ConnectorTableHandle.class))).thenReturn(Optional.empty());

        // Run the test
        final Optional<Object> result = cachedConnectorMetadataUnderTest.getInfo(table);

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
        final List<SchemaTableName> result = cachedConnectorMetadataUnderTest.listTables(session, Optional.of("value"));

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
        final List<SchemaTableName> result = cachedConnectorMetadataUnderTest.listTables(session, Optional.of("value"));

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
        final Map<String, ColumnHandle> result = cachedConnectorMetadataUnderTest.getColumnHandles(session,
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
                () -> cachedConnectorMetadataUnderTest.getColumnHandles(session, tableHandle));
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
        final ColumnMetadata result = cachedConnectorMetadataUnderTest.getColumnMetadata(session, tableHandle,
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
                () -> cachedConnectorMetadataUnderTest.getColumnMetadata(session, tableHandle, columnHandle));
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
        final Map<SchemaTableName, List<ColumnMetadata>> result = cachedConnectorMetadataUnderTest.listTableColumns(
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
        final TableStatistics result = cachedConnectorMetadataUnderTest.getTableStatistics(session, tableHandle,
                constraint, false);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCreateSchema() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final Map<String, Object> properties = new HashMap<>();

        // Run the test
        cachedConnectorMetadataUnderTest.createSchema(session, "schemaName", properties);

        // Verify the results
        verify(mockDelegate).createSchema(any(ConnectorSession.class), eq("schemaName"), eq(new HashMap<>()));
    }

    @Test
    public void testDropSchema() throws Exception
    {
        // Setup
        final ConnectorSession session = null;

        // Run the test
        cachedConnectorMetadataUnderTest.dropSchema(session, "schemaName");

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
        assertThrows(PrestoException.class, () -> cachedConnectorMetadataUnderTest.dropSchema(session, "schemaName"));
    }

    @Test
    public void testRenameSchema() throws Exception
    {
        // Setup
        final ConnectorSession session = null;

        // Run the test
        cachedConnectorMetadataUnderTest.renameSchema(session, "source", "target");

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
        cachedConnectorMetadataUnderTest.createTable(session, tableMetadata, false);

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
                () -> cachedConnectorMetadataUnderTest.createTable(session, tableMetadata, false));
    }

    @Test
    public void testDropTable() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;

        // Run the test
        cachedConnectorMetadataUnderTest.dropTable(session, tableHandle);

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
        assertThrows(RuntimeException.class, () -> cachedConnectorMetadataUnderTest.dropTable(session, tableHandle));
    }

    @Test
    public void testRenameTable() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final SchemaTableName newTableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        cachedConnectorMetadataUnderTest.renameTable(session, tableHandle, newTableName);

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
        cachedConnectorMetadataUnderTest.setTableComment(session, tableHandle, Optional.of("value"));

        // Verify the results
        verify(mockDelegate).setTableComment(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(Optional.of("value")));
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
        cachedConnectorMetadataUnderTest.addColumn(session, tableHandle, column);

        // Verify the results
        verify(mockDelegate).addColumn(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)));
    }

    @Test
    public void testRenameColumn() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ColumnHandle source = null;

        // Run the test
        cachedConnectorMetadataUnderTest.renameColumn(session, tableHandle, source, "target");

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
        cachedConnectorMetadataUnderTest.dropColumn(session, tableHandle, column);

        // Verify the results
        verify(mockDelegate).dropColumn(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(ColumnHandle.class));
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
        final Optional<ConnectorNewTableLayout> result = cachedConnectorMetadataUnderTest.getNewTableLayout(session,
                tableMetadata);

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
        final Optional<ConnectorNewTableLayout> result = cachedConnectorMetadataUnderTest.getNewTableLayout(session,
                tableMetadata);

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
        final Optional<ConnectorNewTableLayout> result = cachedConnectorMetadataUnderTest.getInsertLayout(session,
                tableHandle);

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
        final Optional<ConnectorNewTableLayout> result = cachedConnectorMetadataUnderTest.getInsertLayout(session,
                tableHandle);

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
        final Optional<ConnectorNewTableLayout> result = cachedConnectorMetadataUnderTest.getUpdateLayout(session,
                tableHandle);

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
        final Optional<ConnectorNewTableLayout> result = cachedConnectorMetadataUnderTest.getUpdateLayout(session,
                tableHandle);

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
        final TableStatisticsMetadata result = cachedConnectorMetadataUnderTest.getStatisticsCollectionMetadataForWrite(
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
        final TableStatisticsMetadata result = cachedConnectorMetadataUnderTest.getStatisticsCollectionMetadata(session,
                tableMetadata);

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
        final ConnectorTableHandle result = cachedConnectorMetadataUnderTest.beginStatisticsCollection(session,
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
        cachedConnectorMetadataUnderTest.finishStatisticsCollection(session, tableHandle, computedStatistics);

        // Verify the results
        verify(mockDelegate).finishStatisticsCollection(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null))));
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
        final ConnectorOutputTableHandle result = cachedConnectorMetadataUnderTest.beginCreateTable(session,
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
        when(mockDelegate.finishCreateTable(any(ConnectorSession.class), any(ConnectorOutputTableHandle.class),
                eq(Arrays.asList()),
                eq(Arrays.asList(ComputedStatistics.restoreComputedStatistics("state", null)))))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorOutputMetadata> result = cachedConnectorMetadataUnderTest.finishCreateTable(session,
                tableHandle, fragments, computedStatistics);

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
        final Optional<ConnectorOutputMetadata> result = cachedConnectorMetadataUnderTest.finishCreateTable(session,
                tableHandle, fragments, computedStatistics);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testBeginQuery() throws Exception
    {
        // Setup
        final ConnectorSession session = null;

        // Run the test
        cachedConnectorMetadataUnderTest.beginQuery(session);

        // Verify the results
        verify(mockDelegate).beginQuery(any(ConnectorSession.class));
    }

    @Test
    public void testCleanupQuery()
    {
        // Setup
        final ConnectorSession session = null;

        // Run the test
        cachedConnectorMetadataUnderTest.cleanupQuery(session);

        // Verify the results
        verify(mockDelegate).cleanupQuery(any(ConnectorSession.class));
    }

    @Test
    public void testBeginInsert() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.beginInsert(any(ConnectorSession.class), any(ConnectorTableHandle.class))).thenReturn(null);

        // Run the test
        final ConnectorInsertTableHandle result = cachedConnectorMetadataUnderTest.beginInsert(session, tableHandle);

        // Verify the results
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
        final Optional<ConnectorOutputMetadata> result = cachedConnectorMetadataUnderTest.finishInsert(session,
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
        final Optional<ConnectorOutputMetadata> result = cachedConnectorMetadataUnderTest.finishInsert(session,
                insertHandle, fragments, computedStatistics);

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
        final ColumnHandle result = cachedConnectorMetadataUnderTest.getDeleteRowIdColumnHandle(session, tableHandle);

        // Verify the results
    }

    @Test
    public void testGetUpdateRowIdColumnHandle() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final List<ColumnHandle> updatedColumns = Arrays.asList();
        when(mockDelegate.getUpdateRowIdColumnHandle(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(Arrays.asList()))).thenReturn(null);

        // Run the test
        final ColumnHandle result = cachedConnectorMetadataUnderTest.getUpdateRowIdColumnHandle(session, tableHandle,
                updatedColumns);

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
        final ConnectorTableHandle result = cachedConnectorMetadataUnderTest.beginDelete(session, tableHandle);

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
        cachedConnectorMetadataUnderTest.finishDelete(session, tableHandle, fragments);

        // Verify the results
        verify(mockDelegate).finishDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(Arrays.asList()));
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
        final ConnectorTableHandle result = cachedConnectorMetadataUnderTest.beginUpdate(session, tableHandle,
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
        cachedConnectorMetadataUnderTest.finishUpdate(session, tableHandle, fragments);

        // Verify the results
        verify(mockDelegate).finishUpdate(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                eq(Arrays.asList()));
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
        cachedConnectorMetadataUnderTest.createView(session, viewName, definition, false);

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
        cachedConnectorMetadataUnderTest.dropView(session, viewName);

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
        final List<SchemaTableName> result = cachedConnectorMetadataUnderTest.listViews(session, Optional.of("value"));

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
        final List<SchemaTableName> result = cachedConnectorMetadataUnderTest.listViews(session, Optional.of("value"));

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
        final Map<SchemaTableName, ConnectorViewDefinition> result = cachedConnectorMetadataUnderTest.getViews(session,
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
        final Optional<ConnectorViewDefinition> result = cachedConnectorMetadataUnderTest.getView(session, viewName);

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
        final Optional<ConnectorViewDefinition> result = cachedConnectorMetadataUnderTest.getView(session, viewName);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testSupportsMetadataDelete() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        final ConnectorTableLayoutHandle tableLayoutHandle = null;
        when(mockDelegate.supportsMetadataDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(ConnectorTableLayoutHandle.class))).thenReturn(false);

        // Run the test
        final boolean result = cachedConnectorMetadataUnderTest.supportsMetadataDelete(session, tableHandle,
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
        final OptionalLong result = cachedConnectorMetadataUnderTest.metadataDelete(session, tableHandle,
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
        final OptionalLong result = cachedConnectorMetadataUnderTest.metadataDelete(session, tableHandle,
                tableLayoutHandle);

        // Verify the results
        assertNull(result);
    }

    @Test
    public void testApplyDelete() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        when(mockDelegate.applyDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorTableHandle> result = cachedConnectorMetadataUnderTest.applyDelete(session, handle);

        // Verify the results
    }

    @Test
    public void testApplyDelete_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        when(mockDelegate.applyDelete(any(ConnectorSession.class), any(ConnectorTableHandle.class)))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<ConnectorTableHandle> result = cachedConnectorMetadataUnderTest.applyDelete(session, handle);

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
        final OptionalLong result = cachedConnectorMetadataUnderTest.executeDelete(session, handle);

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
        final OptionalLong result = cachedConnectorMetadataUnderTest.executeDelete(session, handle);

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
        final OptionalLong result = cachedConnectorMetadataUnderTest.executeUpdate(session, handle);

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
        final OptionalLong result = cachedConnectorMetadataUnderTest.executeUpdate(session, handle);

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
        final Optional<ConnectorResolvedIndex> result = cachedConnectorMetadataUnderTest.resolveIndex(session,
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
        final Optional<ConnectorResolvedIndex> result = cachedConnectorMetadataUnderTest.resolveIndex(session,
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
        cachedConnectorMetadataUnderTest.createRole(session, "role", grantor);

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
        cachedConnectorMetadataUnderTest.dropRole(session, "role");

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
        final Set<String> result = cachedConnectorMetadataUnderTest.listRoles(session);

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
        final Set<String> result = cachedConnectorMetadataUnderTest.listRoles(session);

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
        final Set<RoleGrant> result = cachedConnectorMetadataUnderTest.listRoleGrants(session, principal);

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
        final Set<RoleGrant> result = cachedConnectorMetadataUnderTest.listRoleGrants(session, principal);

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
        cachedConnectorMetadataUnderTest.grantRoles(connectorSession, new HashSet<>(Arrays.asList("value")), grantees,
                false, grantor);

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
        cachedConnectorMetadataUnderTest.revokeRoles(connectorSession, new HashSet<>(Arrays.asList("value")), grantees,
                false, grantor);

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
        final Set<RoleGrant> result = cachedConnectorMetadataUnderTest.listApplicableRoles(session, principal);

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
        final Set<RoleGrant> result = cachedConnectorMetadataUnderTest.listApplicableRoles(session, principal);

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
        final Set<String> result = cachedConnectorMetadataUnderTest.listEnabledRoles(session);

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
        final Set<String> result = cachedConnectorMetadataUnderTest.listEnabledRoles(session);

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
        cachedConnectorMetadataUnderTest.grantTablePrivileges(session, tableName,
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
        cachedConnectorMetadataUnderTest.revokeTablePrivileges(session, tableName,
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
        final List<GrantInfo> result = cachedConnectorMetadataUnderTest.listTablePrivileges(session, prefix);

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
        final List<GrantInfo> result = cachedConnectorMetadataUnderTest.listTablePrivileges(session, prefix);

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testUsesLegacyTableLayouts() throws Exception
    {
        // Setup
        when(mockDelegate.usesLegacyTableLayouts()).thenReturn(false);

        // Run the test
        final boolean result = cachedConnectorMetadataUnderTest.usesLegacyTableLayouts();

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
        final ConnectorTableProperties result = cachedConnectorMetadataUnderTest.getTableProperties(session, table);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testApplyLimit() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;

        // Configure ConnectorMetadata.applyLimit(...).
        final Optional<LimitApplicationResult<ConnectorTableHandle>> connectorTableHandleLimitApplicationResult = Optional.of(
                new LimitApplicationResult<>(null, false));
        when(mockDelegate.applyLimit(any(ConnectorSession.class), any(ConnectorTableHandle.class), eq(0L)))
                .thenReturn(connectorTableHandleLimitApplicationResult);

        // Run the test
        final Optional<LimitApplicationResult<ConnectorTableHandle>> result = cachedConnectorMetadataUnderTest.applyLimit(
                session, handle, 0L);

        // Verify the results
    }

    @Test
    public void testApplyLimit_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        when(mockDelegate.applyLimit(any(ConnectorSession.class), any(ConnectorTableHandle.class), eq(0L)))
                .thenReturn(Optional.empty());

        // Run the test
        final Optional<LimitApplicationResult<ConnectorTableHandle>> result = cachedConnectorMetadataUnderTest.applyLimit(
                session, handle, 0L);

        // Verify the results
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testApplyFilter() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });

        // Configure ConnectorMetadata.applyFilter(...).
        final Optional<ConstraintApplicationResult<ConnectorTableHandle>> connectorTableHandleConstraintApplicationResult = Optional.of(
                new ConstraintApplicationResult<>(null, TupleDomain.withColumnDomains(new HashMap<>())));
        when(mockDelegate.applyFilter(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(Constraint.class))).thenReturn(connectorTableHandleConstraintApplicationResult);

        // Run the test
        final Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = cachedConnectorMetadataUnderTest.applyFilter(
                session, handle, constraint);

        // Verify the results
    }

    @Test
    public void testApplyFilter_ConnectorMetadataReturnsAbsent() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle handle = null;
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });
        when(mockDelegate.applyFilter(any(ConnectorSession.class), any(ConnectorTableHandle.class),
                any(Constraint.class))).thenReturn(Optional.empty());

        // Run the test
        final Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = cachedConnectorMetadataUnderTest.applyFilter(
                session, handle, constraint);

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
        final Optional<ProjectionApplicationResult<ConnectorTableHandle>> result = cachedConnectorMetadataUnderTest.applyProjection(
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
        final Optional<ProjectionApplicationResult<ConnectorTableHandle>> result = cachedConnectorMetadataUnderTest.applyProjection(
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
        final Optional<ConnectorTableHandle> result = cachedConnectorMetadataUnderTest.applySample(session, handle,
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
        final Optional<ConnectorTableHandle> result = cachedConnectorMetadataUnderTest.applySample(session, handle,
                SampleType.SYSTEM, 0.0);

        // Verify the results
        assertEquals(Optional.empty(), result);
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
        final boolean result = cachedConnectorMetadataUnderTest.isExecutionPlanCacheSupported(session, handle);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetTableModificationTime() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final ConnectorTableHandle tableHandle = null;
        when(mockDelegate.getTableModificationTime(any(ConnectorSession.class),
                any(ConnectorTableHandle.class))).thenReturn(0L);

        // Run the test
        final long result = cachedConnectorMetadataUnderTest.getTableModificationTime(session, tableHandle);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testIsPreAggregationSupported() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        when(mockDelegate.isPreAggregationSupported(any(ConnectorSession.class))).thenReturn(false);

        // Run the test
        final boolean result = cachedConnectorMetadataUnderTest.isPreAggregationSupported(session);

        // Verify the results
        assertTrue(result);
    }
}
