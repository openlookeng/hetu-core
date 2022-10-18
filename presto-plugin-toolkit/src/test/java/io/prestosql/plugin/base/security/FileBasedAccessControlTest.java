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
package io.prestosql.plugin.base.security;

import io.prestosql.spi.connector.ColumnMetadata;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class FileBasedAccessControlTest
{
    @Mock
    private FileBasedAccessControlConfig mockConfig;

    private FileBasedAccessControl fileBasedAccessControlUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        String path = this.getClass().getClassLoader().getResource("schema.json").getPath();
        mockConfig.setConfigFile(path);
        fileBasedAccessControlUnderTest = new FileBasedAccessControl(mockConfig);
    }

    @Test
    public void testCheckCanCreateSchema()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        fileBasedAccessControlUnderTest.checkCanCreateSchema(null, identity, "schemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanCreateSchema_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanCreateSchema(null, identity, "schemaName"));
    }

    @Test
    public void testCheckCanDropSchema()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        fileBasedAccessControlUnderTest.checkCanDropSchema(null, identity, "schemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanDropSchema_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanDropSchema(null, identity, "schemaName"));
    }

    @Test
    public void testCheckCanRenameSchema()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        fileBasedAccessControlUnderTest.checkCanRenameSchema(null, identity, "schemaName", "newSchemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanRenameSchema_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanRenameSchema(null, identity, "schemaName",
                        "newSchemaName"));
    }

    @Test
    public void testCheckCanShowSchemas()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        fileBasedAccessControlUnderTest.checkCanShowSchemas(null, identity);

        // Verify the results
    }

    @Test
    public void testCheckCanShowSchemas_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(
                AccessDeniedException.class, () -> fileBasedAccessControlUnderTest.checkCanShowSchemas(null, identity));
    }

    @Test
    public void testFilterSchemas()
    {
        assertEquals(new HashSet<>(Arrays.asList("value")),
                fileBasedAccessControlUnderTest.filterSchemas(null, new ConnectorIdentity("user", new HashSet<>(
                                Arrays.asList("value")), Optional.empty(),
                                Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>()),
                        new HashSet<>(
                                Arrays.asList("value"))));
        assertEquals(Collections.emptySet(),
                fileBasedAccessControlUnderTest.filterSchemas(null, new ConnectorIdentity("user", new HashSet<>(
                                Arrays.asList("value")), Optional.empty(),
                                Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>()),
                        new HashSet<>(
                                Arrays.asList("value"))));
    }

    @Test
    public void testCheckCanCreateTable()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanCreateTable(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanCreateTable(null, identity, tableName));
    }

    @Test
    public void testCheckCanDropTable()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanDropTable(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanDropTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanDropTable(null, identity, tableName));
    }

    @Test
    public void testCheckCanShowTablesMetadata()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        fileBasedAccessControlUnderTest.checkCanShowTablesMetadata(null, identity, "schemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowTablesMetadata_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanShowTablesMetadata(null, identity, "schemaName"));
    }

    @Test
    public void testFilterTables() throws Exception
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final Set<SchemaTableName> tableNames = new HashSet<>(
                Arrays.asList(new SchemaTableName("schemaName", "tableName")));
        final Set<SchemaTableName> expectedResult = new HashSet<>(
                Arrays.asList(new SchemaTableName("schemaName", "tableName")));

        // Run the test
        final Set<SchemaTableName> result = fileBasedAccessControlUnderTest.filterTables(null, identity, tableNames);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCheckCanShowColumnsMetadata()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanShowColumnsMetadata(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanShowColumnsMetadata_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanShowColumnsMetadata(null, identity, tableName));
    }

    @Test
    public void testFilterColumns() throws Exception
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false));
        final List<ColumnMetadata> expectedResult = Arrays.asList(
                new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false));

        // Run the test
        final List<ColumnMetadata> result = fileBasedAccessControlUnderTest.filterColumns(null, identity, tableName,
                columns);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCheckCanRenameTable()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName newTableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanRenameTable(null, identity, tableName, newTableName);

        // Verify the results
    }

    @Test
    public void testCheckCanRenameTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName newTableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanRenameTable(null, identity, tableName, newTableName));
    }

    @Test
    public void testCheckCanSetTableComment()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanSetTableComment(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanSetTableComment_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanSetTableComment(null, identity, tableName));
    }

    @Test
    public void testCheckCanAddColumn()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanAddColumn(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanAddColumn_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanAddColumn(null, identity, tableName));
    }

    @Test
    public void testCheckCanDropColumn()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanDropColumn(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanDropColumn_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanDropColumn(null, identity, tableName));
    }

    @Test
    public void testCheckCanRenameColumn()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanRenameColumn(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanRenameColumn_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanRenameColumn(null, identity, tableName));
    }

    @Test
    public void testCheckCanSelectFromColumns()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanSelectFromColumns(null, identity, tableName,
                new HashSet<>(Arrays.asList("value")));

        // Verify the results
    }

    @Test
    public void testCheckCanSelectFromColumns_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanSelectFromColumns(null, identity, tableName,
                        new HashSet<>(
                                Arrays.asList("value"))));
    }

    @Test
    public void testCheckCanInsertIntoTable()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanInsertIntoTable(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanInsertIntoTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanInsertIntoTable(null, identity, tableName));
    }

    @Test
    public void testCheckCanDeleteFromTable()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanDeleteFromTable(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanDeleteFromTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanDeleteFromTable(null, identity, tableName));
    }

    @Test
    public void testCheckCanUpdateTable()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanUpdateTable(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanUpdateTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanUpdateTable(null, identity, tableName));
    }

    @Test
    public void testCheckCanCreateView()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanCreateView(null, identity, viewName);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateView_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanCreateView(null, identity, viewName));
    }

    @Test
    public void testCheckCanDropView()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanDropView(null, identity, viewName);

        // Verify the results
    }

    @Test
    public void testCheckCanDropView_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanDropView(null, identity, viewName));
    }

    @Test
    public void testCheckCanCreateViewWithSelectFromColumns()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanCreateViewWithSelectFromColumns(null, identity, tableName,
                new HashSet<>(Arrays.asList("value")));

        // Verify the results
    }

    @Test
    public void testCheckCanCreateViewWithSelectFromColumns_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanCreateViewWithSelectFromColumns(null, identity, tableName,
                        new HashSet<>(
                                Arrays.asList("value"))));
    }

    @Test
    public void testCheckCanSetCatalogSessionProperty()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        fileBasedAccessControlUnderTest.checkCanSetCatalogSessionProperty(null, identity, "propertyName");

        // Verify the results
    }

    @Test
    public void testCheckCanSetCatalogSessionProperty_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanSetCatalogSessionProperty(null, identity,
                        "propertyName"));
    }

    @Test
    public void testCheckCanGrantTablePrivilege()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal grantee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanGrantTablePrivilege(null, identity, Privilege.SELECT, tableName,
                grantee, false);

        // Verify the results
    }

    @Test
    public void testCheckCanGrantTablePrivilege_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal grantee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanGrantTablePrivilege(null, identity,
                        Privilege.SELECT, tableName, grantee, false));
    }

    @Test
    public void testCheckCanRevokeTablePrivilege()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal revokee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanRevokeTablePrivilege(null, identity, Privilege.SELECT, tableName,
                revokee, false);

        // Verify the results
    }

    @Test
    public void testCheckCanRevokeTablePrivilege_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal revokee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanRevokeTablePrivilege(null, identity,
                        Privilege.SELECT, tableName, revokee, false));
    }

    @Test
    public void testCheckCanCreateRole()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final Optional<PrestoPrincipal> grantor = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        fileBasedAccessControlUnderTest.checkCanCreateRole(null, identity, "role", grantor);

        // Verify the results
    }

    @Test
    public void testCheckCanDropRole()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        fileBasedAccessControlUnderTest.checkCanDropRole(null, identity, "role");

        // Verify the results
    }

    @Test
    public void testCheckCanGrantRoles()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final Set<PrestoPrincipal> grantees = new HashSet<>(
                Arrays.asList(new PrestoPrincipal(PrincipalType.USER, "name")));
        final Optional<PrestoPrincipal> grantor = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        fileBasedAccessControlUnderTest.checkCanGrantRoles(null, identity, new HashSet<>(Arrays.asList("value")),
                grantees, false, grantor, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanRevokeRoles()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final Set<PrestoPrincipal> grantees = new HashSet<>(
                Arrays.asList(new PrestoPrincipal(PrincipalType.USER, "name")));
        final Optional<PrestoPrincipal> grantor = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        fileBasedAccessControlUnderTest.checkCanRevokeRoles(null, identity, new HashSet<>(Arrays.asList("value")),
                grantees, false, grantor, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanSetRole()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        fileBasedAccessControlUnderTest.checkCanSetRole(null, identity, "role", "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowRoles()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        fileBasedAccessControlUnderTest.checkCanShowRoles(null, identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowRoles_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanShowRoles(null, identity, "catalogName"));
    }

    @Test
    public void testCheckCanShowCurrentRoles()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        fileBasedAccessControlUnderTest.checkCanShowCurrentRoles(null, identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowCurrentRoles_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanShowCurrentRoles(null, identity, "catalogName"));
    }

    @Test
    public void testCheckCanShowRoleGrants()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        fileBasedAccessControlUnderTest.checkCanShowRoleGrants(null, identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowRoleGrants_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanShowRoleGrants(null, identity, "catalogName"));
    }

    @Test
    public void testCheckCanCreateIndex()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanCreateIndex(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanCreateIndex(null, identity, tableName));
    }

    @Test
    public void testCheckCanDropIndex()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanDropIndex(null, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanDropIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanDropIndex(null, identity, tableName));
    }

    @Test
    public void testCheckCanRenameIndex()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName newIndexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanRenameIndex(null, identity, indexName, newIndexName);

        // Verify the results
    }

    @Test
    public void testCheckCanRenameIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName newIndexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanRenameIndex(null, identity, indexName, newIndexName));
    }

    @Test
    public void testCheckCanUpdateIndex()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanUpdateIndex(null, identity, indexName);

        // Verify the results
    }

    @Test
    public void testCheckCanUpdateIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanUpdateIndex(null, identity, indexName));
    }

    @Test
    public void testCheckCanShowIndex()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        fileBasedAccessControlUnderTest.checkCanShowIndex(null, identity, indexName);

        // Verify the results
    }

    @Test
    public void testCheckCanShowIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(
                AccessDeniedException.class,
                () -> fileBasedAccessControlUnderTest.checkCanShowIndex(null, identity, indexName));
    }

    @Test
    public void testGetRowFilter()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final Optional<ViewExpression> result = fileBasedAccessControlUnderTest.getRowFilter(null, identity, tableName);

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
        final Optional<ViewExpression> result = fileBasedAccessControlUnderTest.getColumnMask(null, identity, tableName,
                "columnName", null);

        // Verify the results
    }
}
