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

import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class ForwardingSystemAccessControlTest
{
    private ForwardingSystemAccessControl forwardingSystemAccessControlUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        forwardingSystemAccessControlUnderTest = new ForwardingSystemAccessControl()
        {
            @Override
            protected SystemAccessControl delegate()
            {
                return null;
            }
        };
    }

    @Test
    public void testCheckCanSetUser()
    {
        // Setup
        //final Optional<Principal> principal = Optional.empty();
        Principal principal = new Principal()
        {
            @Override
            public boolean equals(Object another)
            {
                return false;
            }

            @Override
            public String toString()
            {
                return "principal";
            }

            @Override
            public int hashCode()
            {
                return 0;
            }

            @Override
            public String getName()
            {
                return "principal";
            }
        };
        Optional<Principal> admin = Optional.of(principal);

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanSetUser(admin, "userName");

        // Verify the results
    }

    @Test
    public void testCheckCanSetUser_ThrowsAccessDeniedException()
    {
        // Setup
        final Optional<Principal> principal = Optional.empty();

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanSetUser(principal, "userName"));
    }

    @Test
    public void testCheckCanImpersonateUser()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanImpersonateUser(identity, "userName");

        // Verify the results
    }

    @Test
    public void testCheckCanImpersonateUser_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanImpersonateUser(identity, "userName"));
    }

    @Test
    public void testCheckCanSetSystemSessionProperty()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanSetSystemSessionProperty(identity, "propertyName");

        // Verify the results
    }

    @Test
    public void testCheckCanSetSystemSessionProperty_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanSetSystemSessionProperty(identity,
                        "propertyName"));
    }

    @Test
    public void testCheckCanAccessCatalog()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanAccessCatalog(identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanAccessCatalog_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanAccessCatalog(identity, "catalogName"));
    }

    @Test
    public void testFilterCatalogs()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        final Set<String> result = forwardingSystemAccessControlUnderTest.filterCatalogs(identity,
                new HashSet<>(Arrays.asList("value")));

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList("value")), result);
    }

    @Test
    public void testCheckCanShowCatalogs()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanShowCatalogs(identity);

        // Verify the results
    }

    @Test
    public void testCheckCanShowCatalogs_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanShowCatalogs(identity));
    }

    @Test
    public void testCheckCanCreateCatalog()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanCreateCatalog(identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanCreateCatalog_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanCreateCatalog(identity, "catalogName"));
    }

    @Test
    public void testCheckCanDropCatalog()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanDropCatalog(identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanDropCatalog_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanDropCatalog(identity, "catalogName"));
    }

    @Test
    public void testCheckCanUpdateCatalog()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanUpdateCatalog(identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanUpdateCatalog_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanUpdateCatalog(identity, "catalogName"));
    }

    @Test
    public void testCheckCanCreateSchema()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaName schema = new CatalogSchemaName("catalogName", "schemaName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanCreateSchema(identity, schema);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateSchema_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaName schema = new CatalogSchemaName("catalogName", "schemaName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanCreateSchema(identity, schema));
    }

    @Test
    public void testCheckCanDropSchema()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaName schema = new CatalogSchemaName("catalogName", "schemaName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanDropSchema(identity, schema);

        // Verify the results
    }

    @Test
    public void testCheckCanDropSchema_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaName schema = new CatalogSchemaName("catalogName", "schemaName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanDropSchema(identity, schema));
    }

    @Test
    public void testCheckCanRenameSchema()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaName schema = new CatalogSchemaName("catalogName", "schemaName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanRenameSchema(identity, schema, "newSchemaName");

        // Verify the results
    }

    @Test
    public void testCheckCanRenameSchema_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaName schema = new CatalogSchemaName("catalogName", "schemaName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanRenameSchema(identity, schema, "newSchemaName"));
    }

    @Test
    public void testCheckCanShowSchemas()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanShowSchemas(identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowSchemas_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanShowSchemas(identity, "catalogName"));
    }

    @Test
    public void testFilterSchemas()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        final Set<String> result = forwardingSystemAccessControlUnderTest.filterSchemas(identity, "catalogName",
                new HashSet<>(
                        Arrays.asList("value")));

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList("value")), result);
    }

    @Test
    public void testCheckCanCreateTable()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanCreateTable(identity, table);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateTable_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanCreateTable(identity, table));
    }

    @Test
    public void testCheckCanDropTable()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanDropTable(identity, table);

        // Verify the results
    }

    @Test
    public void testCheckCanDropTable_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanDropTable(identity, table));
    }

    @Test
    public void testCheckCanRenameTable()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");
        final CatalogSchemaTableName newTable = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanRenameTable(identity, table, newTable);

        // Verify the results
    }

    @Test
    public void testCheckCanRenameTable_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");
        final CatalogSchemaTableName newTable = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanRenameTable(identity, table, newTable));
    }

    @Test
    public void testCheckCanSetTableComment()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanSetTableComment(identity, table);

        // Verify the results
    }

    @Test
    public void testCheckCanSetTableComment_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanSetTableComment(identity, table));
    }

    @Test
    public void testCheckCanShowTablesMetadata()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaName schema = new CatalogSchemaName("catalogName", "schemaName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanShowTablesMetadata(identity, schema);

        // Verify the results
    }

    @Test
    public void testCheckCanShowTablesMetadata_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaName schema = new CatalogSchemaName("catalogName", "schemaName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanShowTablesMetadata(identity, schema));
    }

    @Test
    public void testFilterTables() throws Exception
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final Set<SchemaTableName> tableNames = new HashSet<>(
                Arrays.asList(new SchemaTableName("schemaName", "tableName")));
        final Set<SchemaTableName> expectedResult = new HashSet<>(
                Arrays.asList(new SchemaTableName("schemaName", "tableName")));

        // Run the test
        final Set<SchemaTableName> result = forwardingSystemAccessControlUnderTest.filterTables(identity, "catalogName",
                tableNames);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCheckCanShowColumnsMetadata()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName tableName = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanShowColumnsMetadata(identity, tableName);

        // Verify the results
    }

    @Test
    public void testCheckCanShowColumnsMetadata_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName tableName = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanShowColumnsMetadata(identity, tableName));
    }

    @Test
    public void testFilterColumns() throws Exception
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName tableName = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");
        final List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false));
        final List<ColumnMetadata> expectedResult = Arrays.asList(
                new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false));

        // Run the test
        final List<ColumnMetadata> result = forwardingSystemAccessControlUnderTest.filterColumns(identity, tableName,
                columns);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCheckCanAddColumn()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanAddColumn(identity, table);

        // Verify the results
    }

    @Test
    public void testCheckCanAddColumn_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanAddColumn(identity, table));
    }

    @Test
    public void testCheckCanDropColumn()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanDropColumn(identity, table);

        // Verify the results
    }

    @Test
    public void testCheckCanDropColumn_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanDropColumn(identity, table));
    }

    @Test
    public void testCheckCanRenameColumn()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanRenameColumn(identity, table);

        // Verify the results
    }

    @Test
    public void testCheckCanRenameColumn_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanRenameColumn(identity, table));
    }

    @Test
    public void testCheckCanSelectFromColumns()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanSelectFromColumns(identity, table,
                new HashSet<>(Arrays.asList("value")));

        // Verify the results
    }

    @Test
    public void testCheckCanSelectFromColumns_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanSelectFromColumns(identity, table, new HashSet<>(
                        Arrays.asList("value"))));
    }

    @Test
    public void testCheckCanInsertIntoTable()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanInsertIntoTable(identity, table);

        // Verify the results
    }

    @Test
    public void testCheckCanInsertIntoTable_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanInsertIntoTable(identity, table));
    }

    @Test
    public void testCheckCanDeleteFromTable()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanDeleteFromTable(identity, table);

        // Verify the results
    }

    @Test
    public void testCheckCanDeleteFromTable_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanDeleteFromTable(identity, table));
    }

    @Test
    public void testCheckCanUpdateTable()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanUpdateTable(identity, table);

        // Verify the results
    }

    @Test
    public void testCheckCanUpdateTable_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanUpdateTable(identity, table));
    }

    @Test
    public void testCheckCanCreateView()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName view = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanCreateView(identity, view);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateView_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName view = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanCreateView(identity, view));
    }

    @Test
    public void testCheckCanDropView()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName view = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanDropView(identity, view);

        // Verify the results
    }

    @Test
    public void testCheckCanDropView_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName view = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanDropView(identity, view));
    }

    @Test
    public void testCheckCanCreateViewWithSelectFromColumns()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanCreateViewWithSelectFromColumns(identity, table,
                new HashSet<>(Arrays.asList("value")));

        // Verify the results
    }

    @Test
    public void testCheckCanCreateViewWithSelectFromColumns_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanCreateViewWithSelectFromColumns(identity, table,
                        new HashSet<>(
                                Arrays.asList("value"))));
    }

    @Test
    public void testCheckCanSetCatalogSessionProperty()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanSetCatalogSessionProperty(identity, "catalogName",
                "propertyName");

        // Verify the results
    }

    @Test
    public void testCheckCanSetCatalogSessionProperty_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanSetCatalogSessionProperty(identity, "catalogName",
                        "propertyName"));
    }

    @Test
    public void testCheckCanGrantTablePrivilege()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");
        final PrestoPrincipal grantee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanGrantTablePrivilege(identity, Privilege.SELECT, table, grantee,
                false);

        // Verify the results
    }

    @Test
    public void testCheckCanGrantTablePrivilege_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");
        final PrestoPrincipal grantee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanGrantTablePrivilege(identity,
                        Privilege.SELECT, table, grantee, false));
    }

    @Test
    public void testCheckCanRevokeTablePrivilege()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");
        final PrestoPrincipal revokee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanRevokeTablePrivilege(identity, Privilege.SELECT, table, revokee,
                false);

        // Verify the results
    }

    @Test
    public void testCheckCanRevokeTablePrivilege_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName table = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");
        final PrestoPrincipal revokee = new PrestoPrincipal(PrincipalType.USER, "name");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanRevokeTablePrivilege(identity,
                        Privilege.SELECT, table, revokee, false));
    }

    @Test
    public void testCheckCanShowRoles()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanShowRoles(identity, "catalogName");

        // Verify the results
    }

    @Test
    public void testCheckCanShowRoles_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanShowRoles(identity, "catalogName"));
    }

    @Test
    public void testCheckCanAccessNodeInfo()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanAccessNodeInfo(identity);

        // Verify the results
    }

    @Test
    public void testCheckCanAccessNodeInfo_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanAccessNodeInfo(identity));
    }

    @Test
    public void testGetRowFilter()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName tableName = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        final Optional<ViewExpression> result = forwardingSystemAccessControlUnderTest.getRowFilter(identity,
                tableName);

        // Verify the results
    }

    @Test
    public void testGetColumnMask()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName tableName = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");
        final Type type = null;

        // Run the test
        final Optional<ViewExpression> result = forwardingSystemAccessControlUnderTest.getColumnMask(identity,
                tableName, "columnName", type);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateIndex()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName index = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanCreateIndex(identity, index);

        // Verify the results
    }

    @Test
    public void testCheckCanCreateIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName index = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanCreateIndex(identity, index));
    }

    @Test
    public void testCheckCanDropIndex()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName index = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanDropIndex(identity, index);

        // Verify the results
    }

    @Test
    public void testCheckCanDropIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName index = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanDropIndex(identity, index));
    }

    @Test
    public void testCheckCanRenameIndex()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName index = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");
        final CatalogSchemaTableName newIndex = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanRenameIndex(identity, index, newIndex);

        // Verify the results
    }

    @Test
    public void testCheckCanRenameIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName index = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");
        final CatalogSchemaTableName newIndex = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanRenameIndex(identity, index, newIndex));
    }

    @Test
    public void testCheckCanUpdateIndex()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName index = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanUpdateIndex(identity, index);

        // Verify the results
    }

    @Test
    public void testCheckCanUpdateIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName index = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanUpdateIndex(identity, index));
    }

    @Test
    public void testCheckCanShowIndex()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName index = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        forwardingSystemAccessControlUnderTest.checkCanShowIndex(identity, index);

        // Verify the results
    }

    @Test
    public void testCheckCanShowIndex_ThrowsAccessDeniedException()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final CatalogSchemaTableName index = new CatalogSchemaTableName("catalogName", "schemaName", "tableName");

        // Run the test
        assertThrows(AccessDeniedException.class,
                () -> forwardingSystemAccessControlUnderTest.checkCanShowIndex(identity, index));
    }

    @Test
    public void testOf() throws Exception
    {
        // Setup
        final Supplier<SystemAccessControl> systemAccessControlSupplier = () -> null;

        // Run the test
        final SystemAccessControl result = ForwardingSystemAccessControl.of(systemAccessControlSupplier);

        // Verify the results
    }
}
