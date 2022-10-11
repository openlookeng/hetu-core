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
package io.hetu.core.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.ConcurrentModificationException;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateDatabaseResult;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateTableResult;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseResult;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteTableResult;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.GlueEncryptionException;
import com.amazonaws.services.glue.model.InternalServiceException;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.OperationTimeoutException;
import com.amazonaws.services.glue.model.ResourceNumberLimitExceededException;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateTableResult;
import io.hetu.core.plugin.iceberg.UnknownTableTypeException;
import io.hetu.core.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalog;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.metastore.glue.GlueMetastoreApiStats;
import io.prestosql.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.types.Types;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TrinoGlueCatalogTest
{
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;
    @Mock
    private IcebergTableOperationsProvider mockTableOperationsProvider;
    @Mock
    private AWSGlueAsync mockGlueClient;
    @Mock
    private GlueMetastoreStats mockStats;

    private TrinoGlueCatalog trinoGlueCatalogUnderTest;

    @BeforeMethod
    public void setUp()
    {
        initMocks(this);
        trinoGlueCatalogUnderTest = new TrinoGlueCatalog(mockHdfsEnvironment, mockTableOperationsProvider,
                "trinoVersion", mockGlueClient, mockStats,
                Optional.of("value"), false);
    }

    @Test
    public void testListNamespaces()
    {
        // Setup
        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<String> result = trinoGlueCatalogUnderTest.listNamespaces(null);

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testListNamespaces_AWSGlueAsyncThrowsInvalidInputException()
    {
        // Setup
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<String> result = trinoGlueCatalogUnderTest.listNamespaces(null);

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testListNamespaces_AWSGlueAsyncThrowsInternalServiceException()
    {
        // Setup
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<String> result = trinoGlueCatalogUnderTest.listNamespaces(null);

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testListNamespaces_AWSGlueAsyncThrowsOperationTimeoutException()
    {
        // Setup
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<String> result = trinoGlueCatalogUnderTest.listNamespaces(null);

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testListNamespaces_AWSGlueAsyncThrowsGlueEncryptionException()
    {
        // Setup
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<String> result = trinoGlueCatalogUnderTest.listNamespaces(null);

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testDropNamespace()
    {
        // Setup
        when(mockStats.getDeleteDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteDatabase(new DeleteDatabaseRequest())).thenReturn(new DeleteDatabaseResult());

        // Run the test
        trinoGlueCatalogUnderTest.dropNamespace(null, "namespace");

        // Verify the results
    }

    @Test
    public void testDropNamespace_AWSGlueAsyncThrowsEntityNotFoundException()
    {
        // Setup
        when(mockStats.getDeleteDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteDatabase(new DeleteDatabaseRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        trinoGlueCatalogUnderTest.dropNamespace(null, "namespace");

        // Verify the results
    }

    @Test
    public void testDropNamespace_AWSGlueAsyncThrowsInvalidInputException()
    {
        // Setup
        when(mockStats.getDeleteDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteDatabase(new DeleteDatabaseRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        trinoGlueCatalogUnderTest.dropNamespace(null, "namespace");

        // Verify the results
    }

    @Test
    public void testDropNamespace_AWSGlueAsyncThrowsInternalServiceException()
    {
        // Setup
        when(mockStats.getDeleteDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteDatabase(new DeleteDatabaseRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        trinoGlueCatalogUnderTest.dropNamespace(null, "namespace");

        // Verify the results
    }

    @Test
    public void testDropNamespace_AWSGlueAsyncThrowsOperationTimeoutException()
    {
        // Setup
        when(mockStats.getDeleteDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteDatabase(new DeleteDatabaseRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        trinoGlueCatalogUnderTest.dropNamespace(null, "namespace");

        // Verify the results
    }

    @Test
    public void testLoadNamespaceMetadata()
    {
        // Setup
        when(mockStats.getGetDatabase()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getDatabase(...).
        final GetDatabaseResult getDatabaseResult = new GetDatabaseResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabaseResult.setDatabase(database);
        when(mockGlueClient.getDatabase(new GetDatabaseRequest())).thenReturn(getDatabaseResult);

        // Run the test
        final Map<String, Object> result = trinoGlueCatalogUnderTest.loadNamespaceMetadata(null, "namespace");

        // Verify the results
    }

    @Test
    public void testLoadNamespaceMetadata_AWSGlueAsyncThrowsInvalidInputException()
    {
        // Setup
        when(mockStats.getGetDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getDatabase(new GetDatabaseRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        final Map<String, Object> result = trinoGlueCatalogUnderTest.loadNamespaceMetadata(null, "namespace");

        // Verify the results
    }

    @Test
    public void testLoadNamespaceMetadata_AWSGlueAsyncThrowsEntityNotFoundException()
    {
        // Setup
        when(mockStats.getGetDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getDatabase(new GetDatabaseRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        final Map<String, Object> result = trinoGlueCatalogUnderTest.loadNamespaceMetadata(null, "namespace");

        // Verify the results
    }

    @Test
    public void testLoadNamespaceMetadata_AWSGlueAsyncThrowsInternalServiceException()
    {
        // Setup
        when(mockStats.getGetDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getDatabase(new GetDatabaseRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        final Map<String, Object> result = trinoGlueCatalogUnderTest.loadNamespaceMetadata(null, "namespace");

        // Verify the results
    }

    @Test
    public void testLoadNamespaceMetadata_AWSGlueAsyncThrowsOperationTimeoutException()
    {
        // Setup
        when(mockStats.getGetDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getDatabase(new GetDatabaseRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        final Map<String, Object> result = trinoGlueCatalogUnderTest.loadNamespaceMetadata(null, "namespace");

        // Verify the results
    }

    @Test
    public void testLoadNamespaceMetadata_AWSGlueAsyncThrowsGlueEncryptionException()
    {
        // Setup
        when(mockStats.getGetDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getDatabase(new GetDatabaseRequest())).thenThrow(GlueEncryptionException.class);

        // Run the test
        final Map<String, Object> result = trinoGlueCatalogUnderTest.loadNamespaceMetadata(null, "namespace");

        // Verify the results
    }

    @Test
    public void testGetNamespacePrincipal()
    {
        // Setup
        final Optional<PrestoPrincipal> expectedResult = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        final Optional<PrestoPrincipal> result = trinoGlueCatalogUnderTest.getNamespacePrincipal(null, "namespace");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCreateNamespace()
    {
        // Setup
        final ConnectorSession session = null;
        final Map<String, Object> properties = new HashMap<>();
        final PrestoPrincipal owner = new PrestoPrincipal(PrincipalType.USER, "name");
        when(mockStats.getCreateDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createDatabase(new CreateDatabaseRequest())).thenReturn(new CreateDatabaseResult());

        // Run the test
        trinoGlueCatalogUnderTest.createNamespace(session, "namespace", properties, owner);

        // Verify the results
    }

    @Test
    public void testCreateNamespace_AWSGlueAsyncThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final Map<String, Object> properties = new HashMap<>();
        final PrestoPrincipal owner = new PrestoPrincipal(PrincipalType.USER, "name");
        when(mockStats.getCreateDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createDatabase(new CreateDatabaseRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createNamespace(session, "namespace", properties, owner);

        // Verify the results
    }

    @Test
    public void testCreateNamespace_AWSGlueAsyncThrowsAlreadyExistsException()
    {
        // Setup
        final ConnectorSession session = null;
        final Map<String, Object> properties = new HashMap<>();
        final PrestoPrincipal owner = new PrestoPrincipal(PrincipalType.USER, "name");
        when(mockStats.getCreateDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createDatabase(new CreateDatabaseRequest())).thenThrow(AlreadyExistsException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createNamespace(session, "namespace", properties, owner);

        // Verify the results
    }

    @Test
    public void testCreateNamespace_AWSGlueAsyncThrowsResourceNumberLimitExceededException()
    {
        // Setup
        final ConnectorSession session = null;
        final Map<String, Object> properties = new HashMap<>();
        final PrestoPrincipal owner = new PrestoPrincipal(PrincipalType.USER, "name");
        when(mockStats.getCreateDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createDatabase(new CreateDatabaseRequest()))
                .thenThrow(ResourceNumberLimitExceededException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createNamespace(session, "namespace", properties, owner);

        // Verify the results
    }

    @Test
    public void testCreateNamespace_AWSGlueAsyncThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final Map<String, Object> properties = new HashMap<>();
        final PrestoPrincipal owner = new PrestoPrincipal(PrincipalType.USER, "name");
        when(mockStats.getCreateDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createDatabase(new CreateDatabaseRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createNamespace(session, "namespace", properties, owner);

        // Verify the results
    }

    @Test
    public void testCreateNamespace_AWSGlueAsyncThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final Map<String, Object> properties = new HashMap<>();
        final PrestoPrincipal owner = new PrestoPrincipal(PrincipalType.USER, "name");
        when(mockStats.getCreateDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createDatabase(new CreateDatabaseRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createNamespace(session, "namespace", properties, owner);

        // Verify the results
    }

    @Test
    public void testCreateNamespace_AWSGlueAsyncThrowsGlueEncryptionException()
    {
        // Setup
        final ConnectorSession session = null;
        final Map<String, Object> properties = new HashMap<>();
        final PrestoPrincipal owner = new PrestoPrincipal(PrincipalType.USER, "name");
        when(mockStats.getCreateDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createDatabase(new CreateDatabaseRequest())).thenThrow(GlueEncryptionException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createNamespace(session, "namespace", properties, owner);

        // Verify the results
    }

    @Test
    public void testSetNamespacePrincipal()
    {
        // Setup
        final PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        trinoGlueCatalogUnderTest.setNamespacePrincipal(null, "namespace", principal);

        // Verify the results
    }

    @Test
    public void testRenameNamespace()
    {
        // Setup
        // Run the test
        trinoGlueCatalogUnderTest.renameNamespace(null, "source", "target");

        // Verify the results
    }

    @Test
    public void testListTables()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTables(...).
        final GetTablesResult getTablesResult = new GetTablesResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTablesResult.setTableList(Arrays.asList(table));
        getTablesResult.setNextToken("nextToken");
        when(mockGlueClient.getTables(new GetTablesRequest())).thenReturn(getTablesResult);

        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listTables(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTables_AWSGlueAsyncGetDatabasesThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listTables(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTables_AWSGlueAsyncGetDatabasesThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listTables(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTables_AWSGlueAsyncGetDatabasesThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listTables(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTables_AWSGlueAsyncGetDatabasesThrowsGlueEncryptionException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listTables(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTables_AWSGlueAsyncGetTablesThrowsEntityNotFoundException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTables(new GetTablesRequest())).thenThrow(EntityNotFoundException.class);
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listTables(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTables_AWSGlueAsyncGetTablesThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTables(new GetTablesRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listTables(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTables_AWSGlueAsyncGetTablesThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTables(new GetTablesRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listTables(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTables_AWSGlueAsyncGetTablesThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTables(new GetTablesRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listTables(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTables_AWSGlueAsyncGetTablesThrowsGlueEncryptionException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTables(new GetTablesRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listTables(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testLoadTable()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName table = new SchemaTableName("schemaName", "tableName");
        when(mockTableOperationsProvider.createTableOperations(any(TrinoCatalog.class),
                any(ConnectorSession.class), eq("schemaName"), eq("tableName"), eq(Optional.of("value")),
                eq(Optional.of("value")))).thenReturn(null);

        // Run the test
        final org.apache.iceberg.Table result = trinoGlueCatalogUnderTest.loadTable(session, table);

        // Verify the results
    }

    @Test
    public void testLoadTable_ThrowsUnknownTableTypeException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName table = new SchemaTableName("schemaName", "tableName");
        when(mockTableOperationsProvider.createTableOperations(any(TrinoCatalog.class),
                any(ConnectorSession.class), eq("schemaName"), eq("tableName"), eq(Optional.of("value")),
                eq(Optional.of("value")))).thenReturn(null);

        // Run the test
        assertThrows(UnknownTableTypeException.class, () -> trinoGlueCatalogUnderTest.loadTable(session, table));
    }

    @Test
    public void testDropTable()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        when(mockTableOperationsProvider.createTableOperations(any(TrinoCatalog.class),
                any(ConnectorSession.class), eq("schemaName"), eq("tableName"), eq(Optional.of("value")),
                eq(Optional.of("value")))).thenReturn(null);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenReturn(new DeleteTableResult());

        // Run the test
        trinoGlueCatalogUnderTest.dropTable(session, schemaTableName);

        // Verify the results
    }

    @Test
    public void testDropTable_AWSGlueAsyncThrowsEntityNotFoundException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        when(mockTableOperationsProvider.createTableOperations(any(TrinoCatalog.class),
                any(ConnectorSession.class), eq("schemaName"), eq("tableName"), eq(Optional.of("value")),
                eq(Optional.of("value")))).thenReturn(null);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        trinoGlueCatalogUnderTest.dropTable(session, schemaTableName);

        // Verify the results
    }

    @Test
    public void testDropTable_AWSGlueAsyncThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        when(mockTableOperationsProvider.createTableOperations(any(TrinoCatalog.class),
                any(ConnectorSession.class), eq("schemaName"), eq("tableName"), eq(Optional.of("value")),
                eq(Optional.of("value")))).thenReturn(null);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        trinoGlueCatalogUnderTest.dropTable(session, schemaTableName);

        // Verify the results
    }

    @Test
    public void testDropTable_AWSGlueAsyncThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        when(mockTableOperationsProvider.createTableOperations(any(TrinoCatalog.class),
                any(ConnectorSession.class), eq("schemaName"), eq("tableName"), eq(Optional.of("value")),
                eq(Optional.of("value")))).thenReturn(null);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        trinoGlueCatalogUnderTest.dropTable(session, schemaTableName);

        // Verify the results
    }

    @Test
    public void testDropTable_AWSGlueAsyncThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        when(mockTableOperationsProvider.createTableOperations(any(TrinoCatalog.class),
                any(ConnectorSession.class), eq("schemaName"), eq("tableName"), eq(Optional.of("value")),
                eq(Optional.of("value")))).thenReturn(null);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        trinoGlueCatalogUnderTest.dropTable(session, schemaTableName);

        // Verify the results
    }

    @Test
    public void testNewCreateTableTransaction()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        final Schema schema = new Schema(0, Arrays.asList(Types.NestedField.optional(0, "name", null)), new HashMap<>(),
                new HashSet<>(
                        Arrays.asList(0)));
        final PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
        final Map<String, String> properties = new HashMap<>();
        when(mockTableOperationsProvider.createTableOperations(any(TrinoCatalog.class),
                any(ConnectorSession.class), eq("schemaName"), eq("tableName"), eq(Optional.of("value")),
                eq(Optional.of("value")))).thenReturn(null);

        // Run the test
        final Transaction result = trinoGlueCatalogUnderTest.newCreateTableTransaction(session, schemaTableName, schema,
                partitionSpec, "location", properties);

        // Verify the results
    }

    @Test
    public void testRenameTable()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenReturn(new CreateTableResult());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenReturn(new DeleteTableResult());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncGetTableThrowsEntityNotFoundException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(EntityNotFoundException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncGetTableThrowsInvalidInputException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncGetTableThrowsInternalServiceException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncGetTableThrowsOperationTimeoutException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncGetTableThrowsGlueEncryptionException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncCreateTableThrowsAlreadyExistsException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(AlreadyExistsException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncCreateTableThrowsInvalidInputException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncCreateTableThrowsEntityNotFoundException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(EntityNotFoundException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncCreateTableThrowsResourceNumberLimitExceededException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest()))
                .thenThrow(ResourceNumberLimitExceededException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncCreateTableThrowsInternalServiceException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncCreateTableThrowsOperationTimeoutException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncCreateTableThrowsGlueEncryptionException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncDeleteTableThrowsEntityNotFoundException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenReturn(new CreateTableResult());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncDeleteTableThrowsInvalidInputException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenReturn(new CreateTableResult());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncDeleteTableThrowsInternalServiceException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenReturn(new CreateTableResult());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testRenameTable_AWSGlueAsyncDeleteTableThrowsOperationTimeoutException()
    {
        // Setup
        final SchemaTableName from = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName to = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenReturn(new CreateTableResult());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        trinoGlueCatalogUnderTest.renameTable(null, from, to);

        // Verify the results
    }

    @Test
    public void testDefaultTableLocation()
    {
        // Setup
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetDatabase()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getDatabase(...).
        final GetDatabaseResult getDatabaseResult = new GetDatabaseResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabaseResult.setDatabase(database);
        when(mockGlueClient.getDatabase(new GetDatabaseRequest())).thenReturn(getDatabaseResult);

        // Run the test
        final String result = trinoGlueCatalogUnderTest.defaultTableLocation(null, schemaTableName);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testDefaultTableLocation_AWSGlueAsyncThrowsInvalidInputException()
    {
        // Setup
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getDatabase(new GetDatabaseRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        final String result = trinoGlueCatalogUnderTest.defaultTableLocation(null, schemaTableName);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testDefaultTableLocation_AWSGlueAsyncThrowsEntityNotFoundException()
    {
        // Setup
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getDatabase(new GetDatabaseRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        final String result = trinoGlueCatalogUnderTest.defaultTableLocation(null, schemaTableName);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testDefaultTableLocation_AWSGlueAsyncThrowsInternalServiceException()
    {
        // Setup
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getDatabase(new GetDatabaseRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        final String result = trinoGlueCatalogUnderTest.defaultTableLocation(null, schemaTableName);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testDefaultTableLocation_AWSGlueAsyncThrowsOperationTimeoutException()
    {
        // Setup
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getDatabase(new GetDatabaseRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        final String result = trinoGlueCatalogUnderTest.defaultTableLocation(null, schemaTableName);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testDefaultTableLocation_AWSGlueAsyncThrowsGlueEncryptionException()
    {
        // Setup
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetDatabase()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getDatabase(new GetDatabaseRequest())).thenThrow(GlueEncryptionException.class);

        // Run the test
        final String result = trinoGlueCatalogUnderTest.defaultTableLocation(null, schemaTableName);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testSetTablePrincipal()
    {
        // Setup
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        trinoGlueCatalogUnderTest.setTablePrincipal(null, schemaTableName, principal);

        // Verify the results
    }

    @Test
    public void testCreateView()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenReturn(new UpdateTableResult());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenReturn(new CreateTableResult());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncGetTableThrowsEntityNotFoundException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(EntityNotFoundException.class);
        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncGetTableThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncGetTableThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncGetTableThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncGetTableThrowsGlueEncryptionException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncUpdateTableThrowsEntityNotFoundException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenThrow(EntityNotFoundException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncUpdateTableThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncUpdateTableThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncUpdateTableThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncUpdateTableThrowsConcurrentModificationException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenThrow(ConcurrentModificationException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncUpdateTableThrowsResourceNumberLimitExceededException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest()))
                .thenThrow(ResourceNumberLimitExceededException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncUpdateTableThrowsGlueEncryptionException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncCreateTableThrowsAlreadyExistsException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenReturn(new UpdateTableResult());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(AlreadyExistsException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncCreateTableThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenReturn(new UpdateTableResult());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncCreateTableThrowsEntityNotFoundException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenReturn(new UpdateTableResult());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncCreateTableThrowsResourceNumberLimitExceededException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenReturn(new UpdateTableResult());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest()))
                .thenThrow(ResourceNumberLimitExceededException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncCreateTableThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenReturn(new UpdateTableResult());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncCreateTableThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenReturn(new UpdateTableResult());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testCreateView_AWSGlueAsyncCreateTableThrowsGlueEncryptionException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                                new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"),
                false);
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getUpdateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.updateTable(new UpdateTableRequest())).thenReturn(new UpdateTableResult());
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(GlueEncryptionException.class);

        // Run the test
        trinoGlueCatalogUnderTest.createView(session, schemaViewName, definition, false);

        // Verify the results
    }

    @Test
    public void testRenameView()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenReturn(new CreateTableResult());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenReturn(new DeleteTableResult());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncGetTableThrowsEntityNotFoundException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(EntityNotFoundException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncGetTableThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncGetTableThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncGetTableThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncGetTableThrowsGlueEncryptionException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncCreateTableThrowsAlreadyExistsException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(AlreadyExistsException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncCreateTableThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncCreateTableThrowsEntityNotFoundException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(EntityNotFoundException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncCreateTableThrowsResourceNumberLimitExceededException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest()))
                .thenThrow(ResourceNumberLimitExceededException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncCreateTableThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncCreateTableThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncCreateTableThrowsGlueEncryptionException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncDeleteTableThrowsEntityNotFoundException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenReturn(new CreateTableResult());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncDeleteTableThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenReturn(new CreateTableResult());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncDeleteTableThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenReturn(new CreateTableResult());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testRenameView_AWSGlueAsyncDeleteTableThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getCreateTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.createTable(new CreateTableRequest())).thenReturn(new CreateTableResult());
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        trinoGlueCatalogUnderTest.renameView(session, source, target);

        // Verify the results
    }

    @Test
    public void testSetViewPrincipal()
    {
        // Setup
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        trinoGlueCatalogUnderTest.setViewPrincipal(null, schemaViewName, principal);

        // Verify the results
    }

    @Test
    public void testDropView()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenReturn(new DeleteTableResult());

        // Run the test
        trinoGlueCatalogUnderTest.dropView(session, schemaViewName);

        // Verify the results
    }

    @Test
    public void testDropView_AWSGlueAsyncGetTableThrowsEntityNotFoundException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(EntityNotFoundException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.dropView(session, schemaViewName);

        // Verify the results
    }

    @Test
    public void testDropView_AWSGlueAsyncGetTableThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.dropView(session, schemaViewName);

        // Verify the results
    }

    @Test
    public void testDropView_AWSGlueAsyncGetTableThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.dropView(session, schemaViewName);

        // Verify the results
    }

    @Test
    public void testDropView_AWSGlueAsyncGetTableThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.dropView(session, schemaViewName);

        // Verify the results
    }

    @Test
    public void testDropView_AWSGlueAsyncGetTableThrowsGlueEncryptionException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        trinoGlueCatalogUnderTest.dropView(session, schemaViewName);

        // Verify the results
    }

    @Test
    public void testDropView_AWSGlueAsyncDeleteTableThrowsEntityNotFoundException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        trinoGlueCatalogUnderTest.dropView(session, schemaViewName);

        // Verify the results
    }

    @Test
    public void testDropView_AWSGlueAsyncDeleteTableThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        trinoGlueCatalogUnderTest.dropView(session, schemaViewName);

        // Verify the results
    }

    @Test
    public void testDropView_AWSGlueAsyncDeleteTableThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        trinoGlueCatalogUnderTest.dropView(session, schemaViewName);

        // Verify the results
    }

    @Test
    public void testDropView_AWSGlueAsyncDeleteTableThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        when(mockStats.getDeleteTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.deleteTable(new DeleteTableRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        trinoGlueCatalogUnderTest.dropView(session, schemaViewName);

        // Verify the results
    }

    @Test
    public void testListViews()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTables(...).
        final GetTablesResult getTablesResult = new GetTablesResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTablesResult.setTableList(Arrays.asList(table));
        getTablesResult.setNextToken("nextToken");
        when(mockGlueClient.getTables(new GetTablesRequest())).thenReturn(getTablesResult);

        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listViews(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListViews_AWSGlueAsyncGetDatabasesThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listViews(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListViews_AWSGlueAsyncGetDatabasesThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listViews(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListViews_AWSGlueAsyncGetDatabasesThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listViews(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListViews_AWSGlueAsyncGetDatabasesThrowsGlueEncryptionException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listViews(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListViews_AWSGlueAsyncGetTablesThrowsEntityNotFoundException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTables(new GetTablesRequest())).thenThrow(EntityNotFoundException.class);
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listViews(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListViews_AWSGlueAsyncGetTablesThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTables(new GetTablesRequest())).thenThrow(InvalidInputException.class);
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listViews(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListViews_AWSGlueAsyncGetTablesThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTables(new GetTablesRequest())).thenThrow(OperationTimeoutException.class);
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listViews(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListViews_AWSGlueAsyncGetTablesThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTables(new GetTablesRequest())).thenThrow(InternalServiceException.class);
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listViews(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListViews_AWSGlueAsyncGetTablesThrowsGlueEncryptionException()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Configure AWSGlueAsync.getDatabases(...).
        final GetDatabasesResult getDatabasesResult = new GetDatabasesResult();
        final Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("locationUri");
        database.setParameters(new HashMap<>());
        database.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        getDatabasesResult.setDatabaseList(Arrays.asList(database));
        getDatabasesResult.setNextToken("nextToken");
        when(mockGlueClient.getDatabases(new GetDatabasesRequest())).thenReturn(getDatabasesResult);

        when(mockStats.getGetDatabases()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTables(new GetTablesRequest())).thenThrow(GlueEncryptionException.class);
        when(mockStats.getGetTables()).thenReturn(new GlueMetastoreApiStats());

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listViews(session, Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetView()
    {
        // Setup
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        // Run the test
        final Optional<ConnectorViewDefinition> result = trinoGlueCatalogUnderTest.getView(null, viewName);

        // Verify the results
    }

    @Test
    public void testGetView_AWSGlueAsyncThrowsEntityNotFoundException()
    {
        // Setup
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        final Optional<ConnectorViewDefinition> result = trinoGlueCatalogUnderTest.getView(null, viewName);

        // Verify the results
    }

    @Test
    public void testGetView_AWSGlueAsyncThrowsInvalidInputException()
    {
        // Setup
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        final Optional<ConnectorViewDefinition> result = trinoGlueCatalogUnderTest.getView(null, viewName);

        // Verify the results
    }

    @Test
    public void testGetView_AWSGlueAsyncThrowsInternalServiceException()
    {
        // Setup
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        final Optional<ConnectorViewDefinition> result = trinoGlueCatalogUnderTest.getView(null, viewName);

        // Verify the results
    }

    @Test
    public void testGetView_AWSGlueAsyncThrowsOperationTimeoutException()
    {
        // Setup
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        final Optional<ConnectorViewDefinition> result = trinoGlueCatalogUnderTest.getView(null, viewName);

        // Verify the results
    }

    @Test
    public void testGetView_AWSGlueAsyncThrowsGlueEncryptionException()
    {
        // Setup
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(GlueEncryptionException.class);

        // Run the test
        final Optional<ConnectorViewDefinition> result = trinoGlueCatalogUnderTest.getView(null, viewName);

        // Verify the results
    }

    @Test
    public void testListMaterializedViews()
    {
        // Setup
        final List<SchemaTableName> expectedResult = Arrays.asList(new SchemaTableName("schemaName", "tableName"));

        // Run the test
        final List<SchemaTableName> result = trinoGlueCatalogUnderTest.listMaterializedViews(null,
                Optional.of("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCreateMaterializedView()
    {
        // Setup
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final ConnectorMaterializedViewDefinition definition = new ConnectorMaterializedViewDefinition("originalSql",
                Optional.of(new CatalogSchemaTableName("catalogName", "schemaName", "tableName")), Optional.of("value"),
                Optional.of("value"),
                Arrays.asList(new ConnectorMaterializedViewDefinition.Column("name", TypeId.of("id"))),
                Optional.of("value"), Optional.of("value"), new HashMap<>());

        // Run the test
        trinoGlueCatalogUnderTest.createMaterializedView(null, schemaViewName, definition, false, false);

        // Verify the results
    }

    @Test
    public void testDropMaterializedView()
    {
        // Setup
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        trinoGlueCatalogUnderTest.dropMaterializedView(null, schemaViewName);

        // Verify the results
    }

    @Test
    public void testGetMaterializedView()
    {
        // Setup
        final SchemaTableName schemaViewName = new SchemaTableName("schemaName", "tableName");
        final Optional<ConnectorMaterializedViewDefinition> expectedResult = Optional.of(
                new ConnectorMaterializedViewDefinition("originalSql",
                        Optional.of(new CatalogSchemaTableName("catalogName", "schemaName", "tableName")),
                        Optional.of("value"), Optional.of("value"),
                        Arrays.asList(new ConnectorMaterializedViewDefinition.Column("name", TypeId.of("id"))),
                        Optional.of("value"), Optional.of("value"), new HashMap<>()));

        // Run the test
        final Optional<ConnectorMaterializedViewDefinition> result = trinoGlueCatalogUnderTest.getMaterializedView(null,
                schemaViewName);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testRenameMaterializedView()
    {
        // Setup
        final SchemaTableName source = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName target = new SchemaTableName("schemaName", "tableName");

        // Run the test
        trinoGlueCatalogUnderTest.renameMaterializedView(null, source, target);

        // Verify the results
    }

    @Test
    public void testRedirectTable()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final Optional<CatalogSchemaTableName> expectedResult = Optional.of(
                new CatalogSchemaTableName("catalogName", "schemaName", "tableName"));
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());

        // Configure AWSGlueAsync.getTable(...).
        final GetTableResult getTableResult = new GetTableResult();
        final Table table = new Table();
        table.setName("tableName");
        table.setDatabaseName("databaseName");
        table.setDescription("description");
        table.setOwner("owner");
        table.setCreateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setUpdateTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        table.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        storageDescriptor.setColumns(Arrays.asList(column));
        table.setStorageDescriptor(storageDescriptor);
        table.setViewOriginalText("viewOriginalText");
        table.setTableType("tableType");
        table.setParameters(new HashMap<>());
        getTableResult.setTable(table);
        when(mockGlueClient.getTable(new GetTableRequest())).thenReturn(getTableResult);

        // Run the test
        final Optional<CatalogSchemaTableName> result = trinoGlueCatalogUnderTest.redirectTable(session, tableName);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testRedirectTable_AWSGlueAsyncThrowsEntityNotFoundException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final Optional<CatalogSchemaTableName> expectedResult = Optional.of(
                new CatalogSchemaTableName("catalogName", "schemaName", "tableName"));
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(EntityNotFoundException.class);

        // Run the test
        final Optional<CatalogSchemaTableName> result = trinoGlueCatalogUnderTest.redirectTable(session, tableName);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testRedirectTable_AWSGlueAsyncThrowsInvalidInputException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final Optional<CatalogSchemaTableName> expectedResult = Optional.of(
                new CatalogSchemaTableName("catalogName", "schemaName", "tableName"));
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InvalidInputException.class);

        // Run the test
        final Optional<CatalogSchemaTableName> result = trinoGlueCatalogUnderTest.redirectTable(session, tableName);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testRedirectTable_AWSGlueAsyncThrowsInternalServiceException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final Optional<CatalogSchemaTableName> expectedResult = Optional.of(
                new CatalogSchemaTableName("catalogName", "schemaName", "tableName"));
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(InternalServiceException.class);

        // Run the test
        final Optional<CatalogSchemaTableName> result = trinoGlueCatalogUnderTest.redirectTable(session, tableName);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testRedirectTable_AWSGlueAsyncThrowsOperationTimeoutException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final Optional<CatalogSchemaTableName> expectedResult = Optional.of(
                new CatalogSchemaTableName("catalogName", "schemaName", "tableName"));
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(OperationTimeoutException.class);

        // Run the test
        final Optional<CatalogSchemaTableName> result = trinoGlueCatalogUnderTest.redirectTable(session, tableName);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testRedirectTable_AWSGlueAsyncThrowsGlueEncryptionException()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final Optional<CatalogSchemaTableName> expectedResult = Optional.of(
                new CatalogSchemaTableName("catalogName", "schemaName", "tableName"));
        when(mockStats.getGetTable()).thenReturn(new GlueMetastoreApiStats());
        when(mockGlueClient.getTable(new GetTableRequest())).thenThrow(GlueEncryptionException.class);

        // Run the test
        final Optional<CatalogSchemaTableName> result = trinoGlueCatalogUnderTest.redirectTable(session, tableName);

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
