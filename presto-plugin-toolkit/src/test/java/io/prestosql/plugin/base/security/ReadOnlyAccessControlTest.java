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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class ReadOnlyAccessControlTest
{
    private ReadOnlyAccessControl readOnlyAccessControlUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        readOnlyAccessControlUnderTest = new ReadOnlyAccessControl();
    }

    @Test
    public void testCheckCanShowSchemas()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        readOnlyAccessControlUnderTest.checkCanShowSchemas(null, identity);

        // Verify the results
    }

    @Test
    public void testFilterSchemas()
    {
        readOnlyAccessControlUnderTest.filterSchemas(null, new ConnectorIdentity("user", new HashSet<>(
                        Arrays.asList("value")), Optional.empty(),
                        Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>()),
                new HashSet<>(
                        Arrays.asList("value")));
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
        readOnlyAccessControlUnderTest.checkCanAddColumn(null, identity, tableName);

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
                () -> readOnlyAccessControlUnderTest.checkCanAddColumn(null, identity, tableName));
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
        readOnlyAccessControlUnderTest.checkCanDropColumn(null, identity, tableName);

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
                () -> readOnlyAccessControlUnderTest.checkCanDropColumn(null, identity, tableName));
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
        readOnlyAccessControlUnderTest.checkCanCreateTable(null, identity, tableName);

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
                () -> readOnlyAccessControlUnderTest.checkCanCreateTable(null, identity, tableName));
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
        readOnlyAccessControlUnderTest.checkCanDropTable(null, identity, tableName);

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
                () -> readOnlyAccessControlUnderTest.checkCanDropTable(null, identity, tableName));
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
        readOnlyAccessControlUnderTest.checkCanRenameTable(null, identity, tableName, newTableName);

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
                () -> readOnlyAccessControlUnderTest.checkCanRenameTable(null, identity, tableName, newTableName));
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
        readOnlyAccessControlUnderTest.checkCanSetTableComment(null, identity, tableName);

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
                () -> readOnlyAccessControlUnderTest.checkCanSetTableComment(null, identity, tableName));
    }

    @Test
    public void testCheckCanShowTablesMetadata()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        readOnlyAccessControlUnderTest.checkCanShowTablesMetadata(null, identity, "schemaName");

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
                () -> readOnlyAccessControlUnderTest.checkCanShowTablesMetadata(null, identity, "schemaName"));
    }

    @Test
    public void testFilterTables()
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
        final Set<SchemaTableName> result = readOnlyAccessControlUnderTest.filterTables(null, identity, tableNames);

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
        readOnlyAccessControlUnderTest.checkCanShowColumnsMetadata(null, identity, tableName);

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
                () -> readOnlyAccessControlUnderTest.checkCanShowColumnsMetadata(null, identity, tableName));
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
        final List<ColumnMetadata> result = readOnlyAccessControlUnderTest.filterColumns(null, identity, tableName,
                columns);

        // Verify the results
        assertEquals(expectedResult, result);
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
        readOnlyAccessControlUnderTest.checkCanRenameColumn(null, identity, tableName);

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
        assertThrows(
                AccessDeniedException.class,
                () -> readOnlyAccessControlUnderTest.checkCanRenameColumn(null, identity, tableName));
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
        readOnlyAccessControlUnderTest.checkCanSelectFromColumns(null, identity, tableName,
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
                () -> readOnlyAccessControlUnderTest.checkCanSelectFromColumns(null, identity, tableName, new HashSet<>(
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
        readOnlyAccessControlUnderTest.checkCanInsertIntoTable(null, identity, tableName);

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
                () -> readOnlyAccessControlUnderTest.checkCanInsertIntoTable(null, identity, tableName));
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
        readOnlyAccessControlUnderTest.checkCanDeleteFromTable(null, identity, tableName);

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
                () -> readOnlyAccessControlUnderTest.checkCanDeleteFromTable(null, identity, tableName));
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
        readOnlyAccessControlUnderTest.checkCanCreateView(null, identity, viewName);

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
                () -> readOnlyAccessControlUnderTest.checkCanCreateView(null, identity, viewName));
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
        readOnlyAccessControlUnderTest.checkCanDropView(null, identity, viewName);

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
                () -> readOnlyAccessControlUnderTest.checkCanDropView(null, identity, viewName));
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
        readOnlyAccessControlUnderTest.checkCanCreateViewWithSelectFromColumns(null, identity, tableName,
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
                () -> readOnlyAccessControlUnderTest.checkCanCreateViewWithSelectFromColumns(null, identity, tableName,
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
        readOnlyAccessControlUnderTest.checkCanSetCatalogSessionProperty(null, identity, "propertyName");

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
                () -> readOnlyAccessControlUnderTest.checkCanSetCatalogSessionProperty(null, identity, "propertyName"));
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
        readOnlyAccessControlUnderTest.checkCanGrantTablePrivilege(null, identity, Privilege.SELECT, tableName, grantee,
                false);

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
                () -> readOnlyAccessControlUnderTest.checkCanGrantTablePrivilege(null, identity,
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
        readOnlyAccessControlUnderTest.checkCanRevokeTablePrivilege(null, identity, Privilege.SELECT, tableName,
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
                () -> readOnlyAccessControlUnderTest.checkCanRevokeTablePrivilege(null, identity,
                        Privilege.SELECT, tableName, revokee, false));
    }

    @Test
    public void testCheckCanShowRoles()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        readOnlyAccessControlUnderTest.checkCanShowRoles(null, identity, "catalogName");

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
                () -> readOnlyAccessControlUnderTest.checkCanShowRoles(null, identity, "catalogName"));
    }

    @Test
    public void testCheckCanShowCurrentRoles()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        readOnlyAccessControlUnderTest.checkCanShowCurrentRoles(null, identity, "catalogName");

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
                () -> readOnlyAccessControlUnderTest.checkCanShowCurrentRoles(null, identity, "catalogName"));
    }

    @Test
    public void testCheckCanShowRoleGrants()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        readOnlyAccessControlUnderTest.checkCanShowRoleGrants(null, identity, "catalogName");

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
                () -> readOnlyAccessControlUnderTest.checkCanShowRoleGrants(null, identity, "catalogName"));
    }

    @Test
    public void testGetRowFilter()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final Optional<ViewExpression> result = readOnlyAccessControlUnderTest.getRowFilter(null, identity, tableName);

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
        final Optional<ViewExpression> result = readOnlyAccessControlUnderTest.getColumnMask(null, identity, tableName,
                "columnName", null);

        // Verify the results
    }
}
