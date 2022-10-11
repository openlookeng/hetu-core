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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class AllowAllAccessControlTest
{
    private AllowAllAccessControl allowAllAccessControlUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        allowAllAccessControlUnderTest = new AllowAllAccessControl();
    }

    @Test
    public void testCheckCanCreateSchema()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        allowAllAccessControlUnderTest.checkCanCreateSchema(null, identity, "schemaName");

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
        assertThrows(
                AccessDeniedException.class,
                () -> allowAllAccessControlUnderTest.checkCanCreateSchema(null, identity, "schemaName"));
    }

    @Test
    public void testCheckCanDropSchema()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        allowAllAccessControlUnderTest.checkCanDropSchema(null, identity, "schemaName");

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
                () -> allowAllAccessControlUnderTest.checkCanDropSchema(null, identity, "schemaName"));
    }

    @Test
    public void testCheckCanRenameSchema()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        allowAllAccessControlUnderTest.checkCanRenameSchema(null, identity, "schemaName", "newSchemaName");

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
        assertThrows(
                AccessDeniedException.class,
                () -> allowAllAccessControlUnderTest.checkCanRenameSchema(null, identity, "schemaName",
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
        allowAllAccessControlUnderTest.checkCanShowSchemas(null, identity);

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
                AccessDeniedException.class, () -> allowAllAccessControlUnderTest.checkCanShowSchemas(null, identity));
    }

    @Test
    public void testFilterSchemas()
    {
        assertEquals(new HashSet<>(Arrays.asList("value")),
                allowAllAccessControlUnderTest.filterSchemas(null, new ConnectorIdentity("user", new HashSet<>(
                                Arrays.asList("value")), Optional.empty(),
                                Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>()),
                        new HashSet<>(
                                Arrays.asList("value"))));
        assertEquals(Collections.emptySet(),
                allowAllAccessControlUnderTest.filterSchemas(null, new ConnectorIdentity("user", new HashSet<>(
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
        allowAllAccessControlUnderTest.checkCanCreateTable(null, identity, tableName);

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
                () -> allowAllAccessControlUnderTest.checkCanCreateTable(null, identity, tableName));
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
        allowAllAccessControlUnderTest.checkCanDropTable(null, identity, tableName);

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
                () -> allowAllAccessControlUnderTest.checkCanDropTable(null, identity, tableName));
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
        allowAllAccessControlUnderTest.checkCanRenameTable(null, identity, tableName, newTableName);

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
                () -> allowAllAccessControlUnderTest.checkCanRenameTable(null, identity, tableName, newTableName));
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
        allowAllAccessControlUnderTest.checkCanSetTableComment(null, identity, tableName);

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
                () -> allowAllAccessControlUnderTest.checkCanSetTableComment(null, identity, tableName));
    }

    @Test
    public void testCheckCanShowTablesMetadata()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        allowAllAccessControlUnderTest.checkCanShowTablesMetadata(null, identity, "schemaName");

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
                () -> allowAllAccessControlUnderTest.checkCanShowTablesMetadata(null, identity, "schemaName"));
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
        final Set<SchemaTableName> result = allowAllAccessControlUnderTest.filterTables(null, identity, tableNames);

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
        final SchemaTableName table = new SchemaTableName("schemaName", "tableName");

        // Run the test
        allowAllAccessControlUnderTest.checkCanShowColumnsMetadata(null, identity, table);

        // Verify the results
    }

    @Test
    public void testCheckCanShowColumnsMetadata_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName table = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> allowAllAccessControlUnderTest.checkCanShowColumnsMetadata(null, identity, table));
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
        final List<ColumnMetadata> result = allowAllAccessControlUnderTest.filterColumns(null, identity, tableName,
                columns);

        // Verify the results
        assertEquals(expectedResult, result);
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
        allowAllAccessControlUnderTest.checkCanAddColumn(null, identity, tableName);

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
                () -> allowAllAccessControlUnderTest.checkCanAddColumn(null, identity, tableName));
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
        allowAllAccessControlUnderTest.checkCanDropColumn(null, identity, tableName);

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
                () -> allowAllAccessControlUnderTest.checkCanDropColumn(null, identity, tableName));
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
        allowAllAccessControlUnderTest.checkCanRenameColumn(null, identity, tableName);

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
                () -> allowAllAccessControlUnderTest.checkCanRenameColumn(null, identity, tableName));
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
        allowAllAccessControlUnderTest.checkCanSelectFromColumns(null, identity, tableName,
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
                () -> allowAllAccessControlUnderTest.checkCanSelectFromColumns(null, identity, tableName, new HashSet<>(
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
        allowAllAccessControlUnderTest.checkCanInsertIntoTable(null, identity, tableName);

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
                () -> allowAllAccessControlUnderTest.checkCanInsertIntoTable(null, identity, tableName));
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
        allowAllAccessControlUnderTest.checkCanDeleteFromTable(null, identity, tableName);

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
                () -> allowAllAccessControlUnderTest.checkCanDeleteFromTable(null, identity, tableName));
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
        allowAllAccessControlUnderTest.checkCanUpdateTable(null, identity, tableName);

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
                () -> allowAllAccessControlUnderTest.checkCanUpdateTable(null, identity, tableName));
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
        allowAllAccessControlUnderTest.checkCanCreateView(null, identity, viewName);

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
                () -> allowAllAccessControlUnderTest.checkCanCreateView(null, identity, viewName));
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
        allowAllAccessControlUnderTest.checkCanDropView(null, identity, viewName);

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
                () -> allowAllAccessControlUnderTest.checkCanDropView(null, identity, viewName));
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
        allowAllAccessControlUnderTest.checkCanCreateViewWithSelectFromColumns(null, identity, tableName,
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
                () -> allowAllAccessControlUnderTest.checkCanCreateViewWithSelectFromColumns(null, identity, tableName,
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
        allowAllAccessControlUnderTest.checkCanSetCatalogSessionProperty(null, identity, "propertyName");

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
                () -> allowAllAccessControlUnderTest.checkCanSetCatalogSessionProperty(null, identity, "propertyName"));
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
        allowAllAccessControlUnderTest.checkCanGrantTablePrivilege(null, identity, Privilege.SELECT, tableName, grantee,
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
                () -> allowAllAccessControlUnderTest.checkCanGrantTablePrivilege(null, identity,
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
        allowAllAccessControlUnderTest.checkCanRevokeTablePrivilege(null, identity, Privilege.SELECT, tableName,
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
                () -> allowAllAccessControlUnderTest.checkCanRevokeTablePrivilege(null, identity,
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
        allowAllAccessControlUnderTest.checkCanCreateRole(null, identity, "role", grantor);

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
        allowAllAccessControlUnderTest.checkCanDropRole(null, identity, "role");

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
        allowAllAccessControlUnderTest.checkCanGrantRoles(null, identity, new HashSet<>(Arrays.asList("value")),
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
        allowAllAccessControlUnderTest.checkCanRevokeRoles(null, identity, new HashSet<>(Arrays.asList("value")),
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
        allowAllAccessControlUnderTest.checkCanSetRole(null, identity, "role", "catalogName");

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
        allowAllAccessControlUnderTest.checkCanShowRoles(null, identity, "catalogName");

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
                () -> allowAllAccessControlUnderTest.checkCanShowRoles(null, identity, "catalogName"));
    }

    @Test
    public void testCheckCanShowCurrentRoles()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        allowAllAccessControlUnderTest.checkCanShowCurrentRoles(null, identity, "catalogName");

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
                () -> allowAllAccessControlUnderTest.checkCanShowCurrentRoles(null, identity, "catalogName"));
    }

    @Test
    public void testCheckCanShowRoleGrants()
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        allowAllAccessControlUnderTest.checkCanShowRoleGrants(null, identity, "catalogName");

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
                () -> allowAllAccessControlUnderTest.checkCanShowRoleGrants(null, identity, "catalogName"));
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
        allowAllAccessControlUnderTest.checkCanCreateIndex(null, identity, tableName);

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
                () -> allowAllAccessControlUnderTest.checkCanCreateIndex(null, identity, tableName));
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
        allowAllAccessControlUnderTest.checkCanDropIndex(null, identity, tableName);

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
                () -> allowAllAccessControlUnderTest.checkCanDropIndex(null, identity, tableName));
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
        allowAllAccessControlUnderTest.checkCanRenameIndex(null, identity, indexName, newIndexName);

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
                () -> allowAllAccessControlUnderTest.checkCanRenameIndex(null, identity, indexName, newIndexName));
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
        allowAllAccessControlUnderTest.checkCanUpdateIndex(null, identity, indexName);

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
                () -> allowAllAccessControlUnderTest.checkCanUpdateIndex(null, identity, indexName));
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
        allowAllAccessControlUnderTest.checkCanShowIndex(null, identity, indexName);

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
                () -> allowAllAccessControlUnderTest.checkCanShowIndex(null, identity, indexName));
    }

    @Test
    public void testGetRowFilter()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final Optional<ViewExpression> result = allowAllAccessControlUnderTest.getRowFilter(null, identity, tableName);

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
        final Optional<ViewExpression> result = allowAllAccessControlUnderTest.getColumnMask(null, identity, tableName,
                "columnName", null);

        // Verify the results
    }
}
