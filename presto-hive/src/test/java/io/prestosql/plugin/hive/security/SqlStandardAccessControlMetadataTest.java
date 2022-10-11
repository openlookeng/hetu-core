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

import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.GrantInfo;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.PrivilegeInfo;
import io.prestosql.spi.security.RoleGrant;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class SqlStandardAccessControlMetadataTest
{
    @Mock
    private SemiTransactionalHiveMetastore mockMetastore;

    private SqlStandardAccessControlMetadata sqlStandardAccessControlMetadataUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        sqlStandardAccessControlMetadataUnderTest = new SqlStandardAccessControlMetadata(mockMetastore);
    }

    @Test
    public void testCreateRole() throws Exception
    {
        // Setup
        final Optional<HivePrincipal> grantor = Optional.of(new HivePrincipal(PrincipalType.USER, "name"));

        // Run the test
        sqlStandardAccessControlMetadataUnderTest.createRole(null, "role", grantor);
    }

    @Test
    public void testDropRole()
    {
        // Setup
        // Run the test
        sqlStandardAccessControlMetadataUnderTest.dropRole(null, "role");

        // Verify the results
        verify(mockMetastore).dropRole("role");
    }

    @Test
    public void testListRoles()
    {
        // Setup
        when(mockMetastore.listRoles()).thenReturn(new HashSet<>(Arrays.asList("value")));

        // Run the test
        final Set<String> result = sqlStandardAccessControlMetadataUnderTest.listRoles(null);

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList("value")), result);
    }

    @Test
    public void testListRoles_SemiTransactionalHiveMetastoreReturnsNoItems()
    {
        // Setup
        when(mockMetastore.listRoles()).thenReturn(Collections.emptySet());

        // Run the test
        final Set<String> result = sqlStandardAccessControlMetadataUnderTest.listRoles(null);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testListRoleGrants()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        final Set<RoleGrant> expectedResult = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "name", false)));

        // Configure SemiTransactionalHiveMetastore.listRoleGrants(...).
        final Set<RoleGrant> roleGrants = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "name", false)));
        when(mockMetastore.listRoleGrants(new HivePrincipal(PrincipalType.USER, "name"))).thenReturn(roleGrants);

        // Run the test
        final Set<RoleGrant> result = sqlStandardAccessControlMetadataUnderTest.listRoleGrants(null, principal);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListRoleGrants_SemiTransactionalHiveMetastoreReturnsNoItems()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        when(mockMetastore.listRoleGrants(new HivePrincipal(PrincipalType.USER, "name")))
                .thenReturn(Collections.emptySet());

        // Run the test
        final Set<RoleGrant> result = sqlStandardAccessControlMetadataUnderTest.listRoleGrants(null, principal);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testGrantRoles() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final Set<HivePrincipal> grantees = new HashSet<>(Arrays.asList(new HivePrincipal(PrincipalType.USER, "name")));
        final Optional<HivePrincipal> grantor = Optional.of(new HivePrincipal(PrincipalType.USER, "name"));

        // Run the test
        sqlStandardAccessControlMetadataUnderTest.grantRoles(session, new HashSet<>(Arrays.asList("value")), grantees,
                false, grantor);

        // Verify the results
        verify(mockMetastore).grantRoles(new HashSet<>(Arrays.asList("value")), new HashSet<>(
                        Arrays.asList(new HivePrincipal(PrincipalType.USER, "name"))), false,
                new HivePrincipal(PrincipalType.USER, "name"));
    }

    @Test
    public void testRevokeRoles()
    {
        // Setup
        final ConnectorSession session = null;
        final Set<HivePrincipal> grantees = new HashSet<>(Arrays.asList(new HivePrincipal(PrincipalType.USER, "name")));
        final Optional<HivePrincipal> grantor = Optional.of(new HivePrincipal(PrincipalType.USER, "name"));

        // Run the test
        sqlStandardAccessControlMetadataUnderTest.revokeRoles(session, new HashSet<>(Arrays.asList("value")), grantees,
                false, grantor);

        // Verify the results
        verify(mockMetastore).revokeRoles(new HashSet<>(Arrays.asList("value")), new HashSet<>(
                        Arrays.asList(new HivePrincipal(PrincipalType.USER, "name"))), false,
                new HivePrincipal(PrincipalType.USER, "name"));
    }

    @Test
    public void testListApplicableRoles()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        final Set<RoleGrant> expectedResult = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "name", false)));

        // Configure SemiTransactionalHiveMetastore.listRoleGrants(...).
        final Set<RoleGrant> roleGrants = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "name", false)));
        when(mockMetastore.listRoleGrants(new HivePrincipal(PrincipalType.USER, "name"))).thenReturn(roleGrants);

        // Run the test
        final Set<RoleGrant> result = sqlStandardAccessControlMetadataUnderTest.listApplicableRoles(null, principal);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListApplicableRoles_SemiTransactionalHiveMetastoreReturnsNoItems()
    {
        // Setup
        final HivePrincipal principal = new HivePrincipal(PrincipalType.USER, "name");
        when(mockMetastore.listRoleGrants(new HivePrincipal(PrincipalType.USER, "name")))
                .thenReturn(Collections.emptySet());

        // Run the test
        final Set<RoleGrant> result = sqlStandardAccessControlMetadataUnderTest.listApplicableRoles(null, principal);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testListEnabledRoles()
    {
        // Setup
        final ConnectorSession session = null;

        // Configure SemiTransactionalHiveMetastore.listRoleGrants(...).
        final Set<RoleGrant> roleGrants = new HashSet<>(
                Arrays.asList(new RoleGrant(new PrestoPrincipal(PrincipalType.USER, "name"), "name", false)));
        when(mockMetastore.listRoleGrants(new HivePrincipal(PrincipalType.USER, "name"))).thenReturn(roleGrants);

        // Run the test
        final Set<String> result = sqlStandardAccessControlMetadataUnderTest.listEnabledRoles(session);

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList("value")), result);
    }

    @Test
    public void testListEnabledRoles_SemiTransactionalHiveMetastoreReturnsNoItems()
    {
        // Setup
        final ConnectorSession session = null;
        when(mockMetastore.listRoleGrants(new HivePrincipal(PrincipalType.USER, "name")))
                .thenReturn(Collections.emptySet());

        // Run the test
        final Set<String> result = sqlStandardAccessControlMetadataUnderTest.listEnabledRoles(session);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testGrantTablePrivileges() throws Exception
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        final HivePrincipal grantee = new HivePrincipal(PrincipalType.USER, "name");

        // Run the test
        sqlStandardAccessControlMetadataUnderTest.grantTablePrivileges(session, schemaTableName, new HashSet<>(
                Arrays.asList(Privilege.SELECT)), grantee, false);

        // Verify the results
        verify(mockMetastore).grantTablePrivileges("schemaName", "tableName",
                new HivePrincipal(PrincipalType.USER, "name"), new HashSet<>(
                        Arrays.asList(new HivePrivilegeInfo(
                                HivePrivilegeInfo.HivePrivilege.SELECT, false,
                                new HivePrincipal(PrincipalType.USER, "name"), new HivePrincipal(
                                PrincipalType.USER, "name")))));
    }

    @Test
    public void testRevokeTablePrivileges()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        final HivePrincipal grantee = new HivePrincipal(PrincipalType.USER, "name");

        // Run the test
        sqlStandardAccessControlMetadataUnderTest.revokeTablePrivileges(session, schemaTableName, new HashSet<>(
                Arrays.asList(Privilege.SELECT)), grantee, false);

        // Verify the results
        verify(mockMetastore).revokeTablePrivileges("schemaName", "tableName",
                new HivePrincipal(PrincipalType.USER, "name"), new HashSet<>(
                        Arrays.asList(new HivePrivilegeInfo(
                                HivePrivilegeInfo.HivePrivilege.SELECT, false,
                                new HivePrincipal(PrincipalType.USER, "name"), new HivePrincipal(
                                PrincipalType.USER, "name")))));
    }

    @Test
    public void testListTablePrivileges()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> tableNames = Arrays.asList(new SchemaTableName("schemaName", "tableName"));
        final List<GrantInfo> expectedResult = Arrays.asList(
                new GrantInfo(new PrivilegeInfo(Privilege.SELECT, false), new PrestoPrincipal(
                        PrincipalType.USER, "name"), new SchemaTableName("schemaName", "tableName"),
                        Optional.of(new PrestoPrincipal(
                                PrincipalType.USER, "name")), Optional.of(false)));

        // Configure SemiTransactionalHiveMetastore.listTablePrivileges(...).
        final Set<HivePrivilegeInfo> hivePrivilegeInfos = new HashSet<>(Arrays.asList(new HivePrivilegeInfo(
                HivePrivilegeInfo.HivePrivilege.SELECT, false, new HivePrincipal(PrincipalType.USER, "name"),
                new HivePrincipal(
                        PrincipalType.USER, "name"))));
        when(mockMetastore.listTablePrivileges("schemaName", "tableName",
                new HivePrincipal(PrincipalType.USER, "name"))).thenReturn(hivePrivilegeInfos);

        // Run the test
        final List<GrantInfo> result = sqlStandardAccessControlMetadataUnderTest.listTablePrivileges(session,
                tableNames);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testListTablePrivileges_SemiTransactionalHiveMetastoreReturnsNoItems()
    {
        // Setup
        final ConnectorSession session = null;
        final List<SchemaTableName> tableNames = Arrays.asList(new SchemaTableName("schemaName", "tableName"));
        when(mockMetastore.listTablePrivileges("schemaName", "tableName",
                new HivePrincipal(PrincipalType.USER, "name"))).thenReturn(Collections.emptySet());

        // Run the test
        final List<GrantInfo> result = sqlStandardAccessControlMetadataUnderTest.listTablePrivileges(session,
                tableNames);
    }
}
