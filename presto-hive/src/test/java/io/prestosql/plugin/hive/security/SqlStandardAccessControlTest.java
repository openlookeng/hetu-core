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
package io.prestosql.plugin.hive.security;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveCatalogName;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.HiveTransactionHandle;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.plugin.hive.util.FieldSetterFactoryTest;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.spi.security.ViewExpression;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class SqlStandardAccessControlTest
{
    @Mock
    private HiveCatalogName mockCatalogName;

    private SqlStandardAccessControl sqlStandardAccessControlUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        HiveConfig hiveConfig = new HiveConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveConfig), ImmutableSet.of());
        FileHiveMetastore name = FileHiveMetastore.createTestingFileHiveMetastore(new File("name"));
        sqlStandardAccessControlUnderTest = new SqlStandardAccessControl(mockCatalogName, val -> {
            return new SemiTransactionalHiveMetastore(
                    new HdfsEnvironment(hdfsConfiguration, new HiveConfig(), new NoHdfsAuthentication()),
                    name,
                    MoreExecutors.directExecutor(),
                    io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE,
                    new Duration(0.0, TimeUnit.MILLISECONDS),
                    false,
                    false,
                    Optional.of(new Duration(0.0, TimeUnit.MILLISECONDS)),
                    io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE,
                    io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE,
                    0);
        });
    }

    @Test
    public void testCheckCanCreateSchema()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanCreateSchema(new HiveTransactionHandle(true), identity, "schemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanCreateSchema_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanCreateSchema(transaction, identity, "schemaName"));
    }

    @Test
    public void testCheckCanDropSchema()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanDropSchema(transaction, identity, "schemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanDropSchema_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanDropSchema(transaction, identity, "schemaName"));
    }

    @Test
    public void testCheckCanRenameSchema()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanRenameSchema(transaction, identity, "schemaName", "newSchemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanRenameSchema_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanRenameSchema(transaction, identity, "schemaName",
                        "newSchemaName"));
    }

    @Test
    public void testCheckCanShowSchemas()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanShowSchemas(null, identity);

        // Verify the results
    }

    @Test
    public void testCheckCanShowSchemas_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanShowSchemas(null, identity));
    }

    @Test
    public void testFilterSchemas()
    {
        sqlStandardAccessControlUnderTest.filterSchemas(null, new ConnectorIdentity("name", new HashSet<>(
                        Arrays.asList("value")), Optional.empty(),
                        Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>()),
                new HashSet<>(
                        Arrays.asList("value")));
    }

    @Test
    public void testCheckCanCreateTable()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanCreateTable(transaction, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanCreateTable(transaction, identity, tableName));
    }

    @Test
    public void testCheckCanDropTable()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanDropTable(transaction, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanDropTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanDropTable(transaction, identity, tableName));
    }

    @Test
    public void testCheckCanRenameTable()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName newTableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanRenameTable(transaction, identity, tableName, newTableName);

        // Verify the results
    }

    @Test
    public void testCheckCanRenameTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName newTableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanRenameTable(transaction, identity, tableName,
                        newTableName));
    }

    @Test
    public void testCheckCanSetTableComment()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanSetTableComment(transaction, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanSetTableComment_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanSetTableComment(transaction, identity, tableName));
    }

    @Test
    public void testCheckCanShowTablesMetadata()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanShowTablesMetadata(null, identity, "schemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowTablesMetadata_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanShowTablesMetadata(null, identity, "schemaName"));
    }

    @Test
    public void testFilterTables()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final Set<SchemaTableName> tableNames = new HashSet<>(
                Arrays.asList(new SchemaTableName("schemaName", "tableName")));
        final Set<SchemaTableName> expectedResult = new HashSet<>(
                Arrays.asList(new SchemaTableName("schemaName", "tableName")));

        // Run the test
        final Set<SchemaTableName> result = sqlStandardAccessControlUnderTest.filterTables(null, identity, tableNames);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCheckCanShowColumnsMetadata()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanShowColumnsMetadata(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanShowColumnsMetadata_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanShowColumnsMetadata(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testFilterColumns() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("name", new FieldSetterFactoryTest.Type(), false, "comment", "extraInfo", false, new HashMap<>(), false));
        final List<ColumnMetadata> expectedResult = Arrays.asList(
                new ColumnMetadata("name", new FieldSetterFactoryTest.Type(), false, "comment", "extraInfo", false, new HashMap<>(), false));

        // Run the test
        final List<ColumnMetadata> result = sqlStandardAccessControlUnderTest.filterColumns(transactionHandle, identity,
                tableName, columns);
    }

    @Test
    public void testCheckCanAddColumn()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanAddColumn(transaction, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanAddColumn_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanAddColumn(transaction, identity, tableName));
    }

    @Test
    public void testCheckCanDropColumn()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanDropColumn(transaction, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanDropColumn_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanDropColumn(transaction, identity, tableName));
    }

    @Test
    public void testCheckCanRenameColumn()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanRenameColumn(transaction, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanRenameColumn_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanRenameColumn(transaction, identity, tableName));
    }

    @Test
    public void testCheckCanSelectFromColumns()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanSelectFromColumns(transaction, identity, tableName,
                new HashSet<>(Arrays.asList("value")));

        // Verify the results
    }

    @Test
    public void testCheckCanSelectFromColumns_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanSelectFromColumns(transaction, identity, tableName,
                        new HashSet<>(
                                Arrays.asList("value"))));
    }

    @Test
    public void testCheckCanInsertIntoTable()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanInsertIntoTable(transaction, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanInsertIntoTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanInsertIntoTable(transaction, identity, tableName));
    }

    @Test
    public void testCheckCanDeleteFromTable()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanDeleteFromTable(transaction, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanDeleteFromTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanDeleteFromTable(transaction, identity, tableName));
    }

    @Test
    public void testCheckCanCreateView()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanCreateView(transaction, identity, viewName);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateView_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanCreateView(transaction, identity, viewName));
    }

    @Test
    public void testCheckCanDropView()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanDropView(transaction, identity, viewName);

        // Verify the results
    }

    @Test
    public void testCheckCanDropView_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanDropView(transaction, identity, viewName));
    }

    @Test
    public void testCheckCanCreateViewWithSelectFromColumns()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanCreateViewWithSelectFromColumns(transaction, identity, tableName,
                new HashSet<>(
                        Arrays.asList("value")));

        // Verify the results
    }

    @Test
    public void testCheckCanCreateViewWithSelectFromColumns_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanCreateViewWithSelectFromColumns(transaction, identity,
                        tableName, new HashSet<>(
                                Arrays.asList("value"))));
    }

    @Test
    public void testCheckCanSetCatalogSessionProperty()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanSetCatalogSessionProperty(transaction, identity, "propertyName");

        // Verify the results
    }

    @Test
    public void testCheckCanSetCatalogSessionProperty_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanSetCatalogSessionProperty(transaction, identity,
                        "propertyName"));
    }

    @Test
    public void testCheckCanGrantTablePrivilege()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal grantee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanGrantTablePrivilege(transaction, identity, Privilege.SELECT,
                tableName, grantee, false);

        // Verify the results
    }

    @Test
    public void testCheckCanGrantTablePrivilege_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal grantee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanGrantTablePrivilege(transaction, identity,
                        Privilege.SELECT, tableName, grantee, false));
    }

    @Test
    public void testCheckCanRevokeTablePrivilege()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal revokee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanRevokeTablePrivilege(transaction, identity, Privilege.SELECT,
                tableName, revokee, false);

        // Verify the results
    }

    @Test
    public void testCheckCanRevokeTablePrivilege_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal revokee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanRevokeTablePrivilege(transaction, identity,
                        Privilege.SELECT, tableName, revokee, false));
    }

    @Test
    public void testCheckCanCreateRole()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final Optional<PrestoPrincipal> grantor = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanCreateRole(transactionHandle, identity, "role", grantor);

        // Verify the results
    }

    @Test
    public void testCheckCanDropRole()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanDropRole(transactionHandle, identity, "role");

        // Verify the results
    }

    @Test
    public void testCheckCanGrantRoles()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final Set<PrestoPrincipal> grantees = new HashSet<>(
                Arrays.asList(new PrestoPrincipal(PrincipalType.USER, "name")));
        final Optional<PrestoPrincipal> grantor = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanGrantRoles(transactionHandle, identity,
                new HashSet<>(Arrays.asList("value")), grantees, false, grantor, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanRevokeRoles()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final Set<PrestoPrincipal> grantees = new HashSet<>(
                Arrays.asList(new PrestoPrincipal(PrincipalType.USER, "name")));
        final Optional<PrestoPrincipal> grantor = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanRevokeRoles(transactionHandle, identity,
                new HashSet<>(Arrays.asList("value")), grantees, false, grantor, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanSetRole()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanSetRole(transaction, identity, "role", "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowRoles()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanShowRoles(transactionHandle, identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowRoles_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanShowRoles(transactionHandle, identity, "catalogName"));
    }

    @Test
    public void testCheckCanShowCurrentRoles()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanShowCurrentRoles(null, identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowCurrentRoles_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanShowCurrentRoles(null, identity, "catalogName"));
    }

    @Test
    public void testCheckCanShowRoleGrants()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanShowRoleGrants(null, identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowRoleGrants_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanShowRoleGrants(null, identity, "catalogName"));
    }

    @Test
    public void testCheckCanUpdateTable()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanUpdateTable(transaction, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanUpdateTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transaction = null;
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanUpdateTable(transaction, identity, tableName));
    }

    @Test
    public void testGetRowFilter()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final Optional<ViewExpression> result = sqlStandardAccessControlUnderTest.getRowFilter(null, identity,
                tableName);

        // Verify the results
    }

    @Test
    public void testGetColumnMask()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final Optional<ViewExpression> result = sqlStandardAccessControlUnderTest.getColumnMask(null, identity,
                tableName, "columnName", null);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateIndex()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanCreateIndex(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanCreateIndex(null, identity, tableName);
    }

    @Test
    public void testCheckCanDropIndex()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanDropIndex(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanDropIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanDropIndex(null, identity, tableName));
    }

    @Test
    public void testCheckCanRenameIndex()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName newIndexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanRenameIndex(null, identity, indexName, newIndexName);

        // Verify the results
    }

    @Test
    public void testCheckCanRenameIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName newIndexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanRenameIndex(null, identity, indexName, newIndexName));
    }

    @Test
    public void testCheckCanUpdateIndex()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanUpdateIndex(null, identity, indexName);

        // Verify the results
    }

    @Test
    public void testCheckCanUpdateIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanUpdateIndex(null, identity, indexName));
    }

    @Test
    public void testCheckCanShowIndex()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        sqlStandardAccessControlUnderTest.checkCanShowIndex(null, identity, indexName);

        // Verify the results
    }

    @Test
    public void testCheckCanShowIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("name", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> sqlStandardAccessControlUnderTest.checkCanShowIndex(null, identity, indexName));
    }
}
