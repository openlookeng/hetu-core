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
import io.prestosql.spi.connector.ConnectorAccessControl;
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
import io.prestosql.spi.type.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class ForwardingConnectorAccessControlTest
{
    private ForwardingConnectorAccessControl forwardingConnectorAccessControlUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        forwardingConnectorAccessControlUnderTest = new ForwardingConnectorAccessControl()
        {
            @Override
            protected ConnectorAccessControl delegate()
            {
                return null;
            }
        };
    }

    @Test
    public void testCheckCanCreateSchema()
    {
        // Setup
        ConnectorTransactionHandle connectorTransactionHandle = new ConnectorTransactionHandle()
        {
        };

        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanCreateSchema(null, identity, "schemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanCreateSchema_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanCreateSchema(transactionHandle, identity,
                        "schemaName"));
    }

    @Test
    public void testCheckCanDropSchema()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanDropSchema(transactionHandle, identity, "schemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanDropSchema_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanDropSchema(transactionHandle, identity,
                        "schemaName"));
    }

    @Test
    public void testCheckCanRenameSchema()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanRenameSchema(transactionHandle, identity, "schemaName",
                "newSchemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanRenameSchema_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanRenameSchema(transactionHandle, identity,
                        "schemaName", "newSchemaName"));
    }

    @Test
    public void testCheckCanShowSchemas()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanShowSchemas(transactionHandle, identity);

        // Verify the results
    }

    @Test
    public void testCheckCanShowSchemas_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanShowSchemas(transactionHandle, identity));
    }

    @Test
    public void testFilterSchemas()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        final Set<String> result = forwardingConnectorAccessControlUnderTest.filterSchemas(transactionHandle, identity,
                new HashSet<>(
                        Arrays.asList("value")));

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList("value")), result);
    }

    @Test
    public void testCheckCanCreateTable()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanCreateTable(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanCreateTable(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testCheckCanDropTable()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanDropTable(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanDropTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanDropTable(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testCheckCanRenameTable()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName newTableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanRenameTable(transactionHandle, identity, tableName,
                newTableName);

        // Verify the results
    }

    @Test
    public void testCheckCanRenameTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName newTableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanRenameTable(transactionHandle, identity,
                        tableName, newTableName));
    }

    @Test
    public void testCheckCanSetTableComment()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanSetTableComment(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanSetTableComment_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanSetTableComment(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testCheckCanShowTablesMetadata()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanShowTablesMetadata(transactionHandle, identity, "schemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowTablesMetadata_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanShowTablesMetadata(transactionHandle, identity,
                        "schemaName"));
    }

    @Test
    public void testFilterTables() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final Set<SchemaTableName> tableNames = new HashSet<>(
                Arrays.asList(new SchemaTableName("schemaName", "tableName")));
        final Set<SchemaTableName> expectedResult = new HashSet<>(
                Arrays.asList(new SchemaTableName("schemaName", "tableName")));

        // Run the test
        final Set<SchemaTableName> result = forwardingConnectorAccessControlUnderTest.filterTables(transactionHandle,
                identity, tableNames);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCheckCanShowColumnsMetadata()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanShowColumnsMetadata(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanShowColumnsMetadata_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanShowColumnsMetadata(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testFilterColumns() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false));
        final List<ColumnMetadata> expectedResult = Arrays.asList(
                new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false));

        // Run the test
        final List<ColumnMetadata> result = forwardingConnectorAccessControlUnderTest.filterColumns(transactionHandle,
                identity, tableName, columns);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCheckCanAddColumn()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanAddColumn(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanAddColumn_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanAddColumn(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testCheckCanDropColumn()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanDropColumn(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanDropColumn_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanDropColumn(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testCheckCanRenameColumn()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanRenameColumn(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanRenameColumn_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanRenameColumn(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testCheckCanSelectFromColumns()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanSelectFromColumns(transactionHandle, identity, tableName,
                new HashSet<>(
                        Arrays.asList("value")));

        // Verify the results
    }

    @Test
    public void testCheckCanSelectFromColumns_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanSelectFromColumns(transactionHandle, identity,
                        tableName, new HashSet<>(
                                Arrays.asList("value"))));
    }

    @Test
    public void testCheckCanInsertIntoTable()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanInsertIntoTable(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanInsertIntoTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanInsertIntoTable(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testCheckCanDeleteFromTable()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanDeleteFromTable(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanDeleteFromTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanDeleteFromTable(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testCheckCanUpdateTable()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanUpdateTable(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanUpdateTable_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanUpdateTable(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testCheckCanCreateView()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanCreateView(transactionHandle, identity, viewName);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateView_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanCreateView(transactionHandle, identity,
                        viewName));
    }

    @Test
    public void testCheckCanDropView()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanDropView(transactionHandle, identity, viewName);

        // Verify the results
    }

    @Test
    public void testCheckCanDropView_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanDropView(transactionHandle, identity,
                        viewName));
    }

    @Test
    public void testCheckCanCreateViewWithSelectFromColumns()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanCreateViewWithSelectFromColumns(transactionHandle, identity,
                tableName, new HashSet<>(
                        Arrays.asList("value")));

        // Verify the results
    }

    @Test
    public void testCheckCanCreateViewWithSelectFromColumns_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanCreateViewWithSelectFromColumns(
                        transactionHandle, identity, tableName, new HashSet<>(
                                Arrays.asList("value"))));
    }

    @Test
    public void testCheckCanSetCatalogSessionProperty()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanSetCatalogSessionProperty(transactionHandle, identity,
                "propertyName");

        // Verify the results
    }

    @Test
    public void testCheckCanSetCatalogSessionProperty_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanSetCatalogSessionProperty(transactionHandle,
                        identity, "propertyName"));
    }

    @Test
    public void testCheckCanGrantTablePrivilege()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal grantee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanGrantTablePrivilege(transactionHandle, identity,
                Privilege.SELECT, tableName, grantee, false);

        // Verify the results
    }

    @Test
    public void testCheckCanGrantTablePrivilege_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal grantee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanGrantTablePrivilege(transactionHandle, identity,
                        Privilege.SELECT, tableName, grantee, false));
    }

    @Test
    public void testCheckCanRevokeTablePrivilege()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal revokee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanRevokeTablePrivilege(transactionHandle, identity,
                Privilege.SELECT, tableName, revokee, false);

        // Verify the results
    }

    @Test
    public void testCheckCanRevokeTablePrivilege_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final PrestoPrincipal revokee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanRevokeTablePrivilege(transactionHandle,
                        identity,
                        Privilege.SELECT, tableName, revokee, false));
    }

    @Test
    public void testCheckCanCreateRole()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final Optional<PrestoPrincipal> grantor = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanCreateRole(transactionHandle, identity, "role", grantor);

        // Verify the results
    }

    @Test
    public void testCheckCanDropRole()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanDropRole(transactionHandle, identity, "role");

        // Verify the results
    }

    @Test
    public void testCheckCanGrantRoles()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final Set<PrestoPrincipal> grantees = new HashSet<>(
                Arrays.asList(new PrestoPrincipal(PrincipalType.USER, "name")));
        final Optional<PrestoPrincipal> grantor = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanGrantRoles(transactionHandle, identity,
                new HashSet<>(Arrays.asList("value")), grantees, false, grantor, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanRevokeRoles()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final Set<PrestoPrincipal> grantees = new HashSet<>(
                Arrays.asList(new PrestoPrincipal(PrincipalType.USER, "name")));
        final Optional<PrestoPrincipal> grantor = Optional.of(new PrestoPrincipal(PrincipalType.USER, "name"));

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanRevokeRoles(transactionHandle, identity,
                new HashSet<>(Arrays.asList("value")), grantees, false, grantor, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanSetRole()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanSetRole(transactionHandle, identity, "role", "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowRoles()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanShowRoles(transactionHandle, identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowRoles_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanShowRoles(transactionHandle, identity,
                        "catalogName"));
    }

    @Test
    public void testCheckCanShowCurrentRoles()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanShowCurrentRoles(transactionHandle, identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowCurrentRoles_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanShowCurrentRoles(transactionHandle, identity,
                        "catalogName"));
    }

    @Test
    public void testCheckCanShowRoleGrants()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanShowRoleGrants(transactionHandle, identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowRoleGrants_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanShowRoleGrants(transactionHandle, identity,
                        "catalogName"));
    }

    @Test
    public void testCheckCanCreateIndex()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanCreateIndex(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanCreateIndex(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testCheckCanDropIndex()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanDropIndex(transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanDropIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanDropIndex(transactionHandle, identity,
                        tableName));
    }

    @Test
    public void testCheckCanRenameIndex()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName newIndexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanRenameIndex(transactionHandle, identity, indexName,
                newIndexName);

        // Verify the results
    }

    @Test
    public void testCheckCanRenameIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");
        final SchemaTableName newIndexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanRenameIndex(transactionHandle, identity,
                        indexName, newIndexName));
    }

    @Test
    public void testCheckCanUpdateIndex()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanUpdateIndex(transactionHandle, identity, indexName);

        // Verify the results
    }

    @Test
    public void testCheckCanUpdateIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanUpdateIndex(transactionHandle, identity,
                        indexName));
    }

    @Test
    public void testCheckCanShowIndex()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        forwardingConnectorAccessControlUnderTest.checkCanShowIndex(transactionHandle, identity, indexName);

        // Verify the results
    }

    @Test
    public void testCheckCanShowIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName indexName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingConnectorAccessControlUnderTest.checkCanShowIndex(transactionHandle, identity,
                        indexName));
    }

    @Test
    public void testGetRowFilter()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final Optional<ViewExpression> result = forwardingConnectorAccessControlUnderTest.getRowFilter(
                transactionHandle, identity, tableName);

        // Verify the results
    }

    @Test
    public void testGetColumnMask()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final Type type = null;

        // Run the test
        final Optional<ViewExpression> result = forwardingConnectorAccessControlUnderTest.getColumnMask(
                transactionHandle, identity, tableName, "columnName", type);

        // Verify the results
    }

    @Test
    public void testOf() throws Exception
    {
        // Setup
        final Supplier<ConnectorAccessControl> connectorAccessControlSupplier = () -> null;

        // Run the test
        final ConnectorAccessControl result = ForwardingConnectorAccessControl.of(connectorAccessControlSupplier);

        // Verify the results
    }
}
