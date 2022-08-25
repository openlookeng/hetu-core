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

import io.prestosql.connector.system.GlobalSystemTransactionHandle;
import io.prestosql.plugin.hive.HiveTransactionHandle;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.util.FieldSetterFactoryTest;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.Identity;
import io.prestosql.transaction.TransactionId;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.prestosql.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestLegacyAccessControl
{
    @Mock
    private Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> function;
    private LegacyAccessControl legacyAccessControlTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        legacyAccessControlTest = new LegacyAccessControl(function, new LegacySecurityConfig());
    }

    @Test
    public void test()
    {
        HashSet<String> strings = new HashSet<>();
        Set<String> strings1 = legacyAccessControlTest.filterSchemas(null, null, strings);
        legacyAccessControlTest.checkCanCreateSchema(null, null, null);
        legacyAccessControlTest.checkCanDropSchema(null, null, null);
        legacyAccessControlTest.checkCanRenameSchema(null, null, null, null);
        legacyAccessControlTest.checkCanShowSchemas(null, null);
        legacyAccessControlTest.checkCanCreateTable(null, null, null);
        legacyAccessControlTest.checkCanShowTablesMetadata(null, null, null);
        legacyAccessControlTest.checkCanShowColumnsMetadata(null, null, null);
        legacyAccessControlTest.checkCanSelectFromColumns(null, null, null, null);
        legacyAccessControlTest.checkCanInsertIntoTable(null, null, null);
        legacyAccessControlTest.checkCanDeleteFromTable(null, null, null);
        legacyAccessControlTest.checkCanCreateView(null, null, null);
        legacyAccessControlTest.checkCanDropView(null, null, null);
        legacyAccessControlTest.checkCanCreateViewWithSelectFromColumns(null, null, null, null);
        legacyAccessControlTest.checkCanSetCatalogSessionProperty(null, null, null);
        legacyAccessControlTest.checkCanGrantTablePrivilege(null, null, null, null, null, true);
        legacyAccessControlTest.checkCanRevokeTablePrivilege(null, null, null, null, null, true);
        legacyAccessControlTest.checkCanCreateRole(null, null, null, null);
        legacyAccessControlTest.checkCanDropRole(null, null, null);
        legacyAccessControlTest.checkCanGrantRoles(null, null, null, null, true, null, null);
        legacyAccessControlTest.checkCanRevokeRoles(null, null, null, null, true, null, null);
        legacyAccessControlTest.checkCanSetRole(null, null, null, null);
        legacyAccessControlTest.checkCanShowRoles(null, null, null);
        legacyAccessControlTest.checkCanShowCurrentRoles(null, null, null);
        legacyAccessControlTest.checkCanShowRoleGrants(null, null, null);
        legacyAccessControlTest.checkCanUpdateTable(null, null, null);
        legacyAccessControlTest.checkCanCreateIndex(null, null, null);
        legacyAccessControlTest.checkCanDropIndex(null, null, null);
        legacyAccessControlTest.checkCanRenameIndex(null, null, null, null);
        legacyAccessControlTest.checkCanUpdateIndex(null, null, null);
        legacyAccessControlTest.checkCanShowIndex(null, null, null);
        legacyAccessControlTest.checkCanDropTable(new GlobalSystemTransactionHandle(TransactionId.create()), ConnectorIdentity.ofUser("user"), SchemaTableName.schemaTableName("schemaname", "tablename"));
    }

    @Test
    public void testCheckCanDropTable()
    {
        LegacySecurityConfig legacySecurityConfig = new LegacySecurityConfig().setAllowDropTable(true);
        LegacyAccessControl legacyAccessControl = new LegacyAccessControl(function, legacySecurityConfig);
        legacyAccessControl.checkCanDropTable(new HiveTransactionHandle(true), ConnectorIdentity.ofUser("user"), SchemaTableName.schemaTableName("schemaname", "tablename"));
    }

    @Test
    public void testCheckCanDropTable2()
    {
        legacyAccessControlTest.checkCanDropTable(new GlobalSystemTransactionHandle(TransactionId.create()), ConnectorIdentity.ofUser("user"), SchemaTableName.schemaTableName("schemaname", "tablename"));
    }

    @Test
    public void testCheckCanRenameTable()
    {
        legacyAccessControlTest.checkCanRenameTable(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                ConnectorIdentity.ofUser("user"),
                SchemaTableName.schemaTableName("schemaname", "tablename"),
                SchemaTableName.schemaTableName("schemaname", "tablename"));
    }

    @Test
    public void testCheckCanSetTableComment()
    {
        legacyAccessControlTest.checkCanSetTableComment(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                ConnectorIdentity.ofUser("user"),
                SchemaTableName.schemaTableName("schemaname", "tablename"));
    }

    @Test
    public void testFilterTables()
    {
        legacyAccessControlTest.filterTables(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                ConnectorIdentity.ofUser("user"),
                new HashSet<>());
    }

    @Test
    public void testFilterColumns()
    {
        ColumnMetadata name = new ColumnMetadata("name", new FieldSetterFactoryTest.Type());
        legacyAccessControlTest.filterColumns(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                ConnectorIdentity.ofUser("user"),
                SchemaTableName.schemaTableName("schemaname", "tablename"),
                Arrays.asList(name));
    }

    @Test
    public void testCheckCanAddColumn()
    {
        legacyAccessControlTest.checkCanAddColumn(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                ConnectorIdentity.ofUser("user"),
                SchemaTableName.schemaTableName("schemaname", "tablename"));
    }

    @Test
    public void testCheckCanDropColumn()
    {
        legacyAccessControlTest.checkCanDropColumn(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                ConnectorIdentity.ofUser("user"),
                SchemaTableName.schemaTableName("schemaname", "tablename"));
    }

    @Test
    public void testCheckCanRenameColumn()
    {
        legacyAccessControlTest.checkCanRenameColumn(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                ConnectorIdentity.ofUser("user"),
                SchemaTableName.schemaTableName("schemaname", "tablename"));
    }

    @Test
    public void testGetRowFilter()
    {
        legacyAccessControlTest.getRowFilter(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                new Identity("user", Optional.empty()),
                SchemaTableName.schemaTableName("schemaname", "tablename"));
    }

    @Test
    public void testGetColumnMask()
    {
        legacyAccessControlTest.getColumnMask(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                new Identity("user", Optional.empty()),
                SchemaTableName.schemaTableName("schemaname", "tablename"),
                "columnName",
                new FieldSetterFactoryTest.Type());
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorAccessControl.class, LegacyAccessControl.class);
    }
}
