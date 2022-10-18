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
package io.prestosql.spi.security;

import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class AccessDeniedExceptionTest
{
    @Test
    public void testDenyImpersonateUser1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyImpersonateUser("originalUser", "newUser");

        // Verify the results
    }

    @Test
    public void testDenyImpersonateUser2()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyImpersonateUser("originalUser", "newUser", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenySetUser1() throws Exception
    {
        // Setup
        final Optional<Principal> principal = Optional.empty();

        // Run the test
        AccessDeniedException.denySetUser(principal, "userName");

        // Verify the results
    }

    @Test
    public void testDenySetUser2() throws Exception
    {
        // Setup
        final Optional<Principal> principal = Optional.empty();

        // Run the test
        AccessDeniedException.denySetUser(principal, "userName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyCatalogAccess1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCatalogAccess();

        // Verify the results
    }

    @Test
    public void testDenyCatalogAccess2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCatalogAccess("catalogName");

        // Verify the results
    }

    @Test
    public void testDenyCatalogAccess3() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCatalogAccess("catalogName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyCreateSchema1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCreateSchema("schemaName");

        // Verify the results
    }

    @Test
    public void testDenyCreateSchema2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCreateSchema("schemaName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyDropSchema1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropSchema("schemaName");

        // Verify the results
    }

    @Test
    public void testDenyDropSchema2()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropSchema("schemaName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyRenameSchema1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyRenameSchema("schemaName", "newSchemaName");

        // Verify the results
    }

    @Test
    public void testDenyRenameSchema2()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyRenameSchema("schemaName", "newSchemaName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyShowSchemas1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyShowSchemas();

        // Verify the results
    }

    @Test
    public void testDenyShowSchemas2()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyShowSchemas("extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyCreateTable1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCreateTable("tableName");

        // Verify the results
    }

    @Test
    public void testDenyCreateTable2()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCreateTable("tableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyDropTable1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropTable("tableName");

        // Verify the results
    }

    @Test
    public void testDenyDropTable2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropTable("tableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyRenameTable1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyRenameTable("tableName", "newTableName");

        // Verify the results
    }

    @Test
    public void testDenyRenameTable2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyRenameTable("tableName", "newTableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyCommentTable1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCommentTable("tableName");

        // Verify the results
    }

    @Test
    public void testDenyCommentTable2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCommentTable("tableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyShowTablesMetadata1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyShowTablesMetadata("schemaName");

        // Verify the results
    }

    @Test
    public void testDenyShowTablesMetadata2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyShowTablesMetadata("schemaName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyShowColumnsMetadata() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyShowColumnsMetadata("tableName");

        // Verify the results
    }

    @Test
    public void testDenyAddColumn1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyAddColumn("tableName");

        // Verify the results
    }

    @Test
    public void testDenyAddColumn2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyAddColumn("tableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyDropColumn1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropColumn("tableName");

        // Verify the results
    }

    @Test
    public void testDenyDropColumn2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropColumn("tableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyRenameColumn1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyRenameColumn("tableName");

        // Verify the results
    }

    @Test
    public void testDenyRenameColumn2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyRenameColumn("tableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenySelectTable1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denySelectTable("tableName");

        // Verify the results
    }

    @Test
    public void testDenySelectTable2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denySelectTable("tableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyInsertTable1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyInsertTable("tableName");

        // Verify the results
    }

    @Test
    public void testDenyInsertTable2()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyInsertTable("tableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyDeleteTable1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDeleteTable("tableName");

        // Verify the results
    }

    @Test
    public void testDenyUpdateTable1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyUpdateTable("tableName");

        // Verify the results
    }

    @Test
    public void testDenyUpdateTable2()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyUpdateTable("tableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyDeleteTable2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDeleteTable("tableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyCreateIndex1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCreateIndex();

        // Verify the results
    }

    @Test
    public void testDenyCreateIndex2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCreateIndex("tableName");

        // Verify the results
    }

    @Test
    public void testDenyCreateIndex3() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCreateIndex("indexName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyDropIndex1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropIndex();

        // Verify the results
    }

    @Test
    public void testDenyDropIndex2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropIndex("indexName");

        // Verify the results
    }

    @Test
    public void testDenyDropIndex3() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropIndex("indexName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyRenameIndex1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyRenameIndex();

        // Verify the results
    }

    @Test
    public void testDenyRenameIndex2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyRenameIndex("indexName", "newIndexName");

        // Verify the results
    }

    @Test
    public void testDenyRenameIndex3() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyRenameIndex("indexName", "newIndexName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyUpdateIndex1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyUpdateIndex();

        // Verify the results
    }

    @Test
    public void testDenyUpdateIndex2()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyUpdateIndex("indexName");

        // Verify the results
    }

    @Test
    public void testDenyUpdateIndex3()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyUpdateIndex("indexName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyShowIndex1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyShowIndex();

        // Verify the results
    }

    @Test
    public void testDenyShowIndex2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyShowIndex("indexName");

        // Verify the results
    }

    @Test
    public void testDenyShowIndex3() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyShowIndex("indexName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyCreateView1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCreateView("viewName");

        // Verify the results
    }

    @Test
    public void testDenyCreateView2()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCreateView("viewName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyCreateViewWithSelect1()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        AccessDeniedException.denyCreateViewWithSelect("sourceName", identity);

        // Verify the results
    }

    @Test
    public void testDenyCreateViewWithSelect2() throws Exception
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))),
                new HashMap<>());

        // Run the test
        AccessDeniedException.denyCreateViewWithSelect("sourceName", identity);

        // Verify the results
    }

    @Test
    public void testDenyCreateViewWithSelect3() throws Exception
    {
        // Setup
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))),
                new HashMap<>());

        // Run the test
        AccessDeniedException.denyCreateViewWithSelect("sourceName", identity, "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyDropView1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropView("viewName");

        // Verify the results
    }

    @Test
    public void testDenyDropView2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropView("viewName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenySelectView1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denySelectView("viewName");

        // Verify the results
    }

    @Test
    public void testDenySelectView2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denySelectView("viewName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyGrantTablePrivilege1()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyGrantTablePrivilege("privilege", "tableName");

        // Verify the results
    }

    @Test
    public void testDenyGrantTablePrivilege2()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyGrantTablePrivilege("privilege", "tableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyRevokeTablePrivilege1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyRevokeTablePrivilege("privilege", "tableName");

        // Verify the results
    }

    @Test
    public void testDenyRevokeTablePrivilege2()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyRevokeTablePrivilege("privilege", "tableName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyShowRoles() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyShowRoles("catalogName");

        // Verify the results
    }

    @Test
    public void testDenyShowCurrentRoles() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyShowCurrentRoles("catalogName");

        // Verify the results
    }

    @Test
    public void testDenyShowRoleGrants() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyShowRoleGrants("catalogName");

        // Verify the results
    }

    @Test
    public void testDenySetSystemSessionProperty1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denySetSystemSessionProperty("propertyName");

        // Verify the results
    }

    @Test
    public void testDenySetSystemSessionProperty2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denySetSystemSessionProperty("propertyName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenySetCatalogSessionProperty1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denySetCatalogSessionProperty("catalogName", "propertyName");

        // Verify the results
    }

    @Test
    public void testDenySetCatalogSessionProperty2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denySetCatalogSessionProperty("catalogName", "propertyName", "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenySetCatalogSessionProperty3() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denySetCatalogSessionProperty("propertyName");

        // Verify the results
    }

    @Test
    public void testDenySelectColumns1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denySelectColumns("tableName", Arrays.asList("value"));

        // Verify the results
    }

    @Test
    public void testDenySelectColumns2() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denySelectColumns("tableName", Arrays.asList("value"), "extraInfo");

        // Verify the results
    }

    @Test
    public void testDenyCreateRole() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCreateRole("roleName");

        // Verify the results
    }

    @Test
    public void testDenyDropRole() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropRole("roleName");

        // Verify the results
    }

    @Test
    public void testDenyGrantRoles()
    {
        // Setup
        final Set<PrestoPrincipal> grantees = new HashSet<>(
                Arrays.asList(new PrestoPrincipal(PrincipalType.USER, "name")));

        // Run the test
        AccessDeniedException.denyGrantRoles(new HashSet<>(Arrays.asList("value")), grantees);

        // Verify the results
    }

    @Test
    public void testDenyRevokeRoles() throws Exception
    {
        // Setup
        final Set<PrestoPrincipal> grantees = new HashSet<>(
                Arrays.asList(new PrestoPrincipal(PrincipalType.USER, "name")));

        // Run the test
        AccessDeniedException.denyRevokeRoles(new HashSet<>(Arrays.asList("value")), grantees);

        // Verify the results
    }

    @Test
    public void testDenySetRole() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denySetRole("role");

        // Verify the results
    }

    @Test
    public void testDenyDropCatalog() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyDropCatalog("catalogName");

        // Verify the results
    }

    @Test
    public void testDenyCreateCatalog() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyCreateCatalog("catalogName");

        // Verify the results
    }

    @Test
    public void testDenyUpdateCatalog()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyUpdateCatalog("catalogName");

        // Verify the results
    }

    @Test
    public void testDenyAccessNodeInfo1() throws Exception
    {
        // Setup
        // Run the test
        AccessDeniedException.denyAccessNodeInfo();

        // Verify the results
    }

    @Test
    public void testDenyAccessNodeInfo2()
    {
        // Setup
        // Run the test
        AccessDeniedException.denyAccessNodeInfo("extraInfo");

        // Verify the results
    }
}
