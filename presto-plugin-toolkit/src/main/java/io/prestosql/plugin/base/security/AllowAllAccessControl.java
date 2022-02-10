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
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class AllowAllAccessControl
        implements ConnectorAccessControl
{
    @Override
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
    }

    @Override
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
    }

    @Override
    public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName, String newSchemaName)
    {
    }

    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity)
    {
    }

    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
    }

    @Override
    public void checkCanSetTableComment(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String schemaName)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanShowColumnsMetadata(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName table)
    {
    }

    @Override
    public List<ColumnMetadata> filterColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return columns;
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, Set<String> columnNames)
    {
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanUpdateTable(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transaction, ConnectorIdentity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName, Set<String> columnNames)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transaction, ConnectorIdentity identity, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transaction, ConnectorIdentity identity, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
    }

    @Override
    public void checkCanCreateRole(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String role, Optional<PrestoPrincipal> grantor)
    {
    }

    @Override
    public void checkCanDropRole(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String role)
    {
    }

    @Override
    public void checkCanGrantRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
    }

    @Override
    public void checkCanRevokeRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {
    }

    @Override
    public void checkCanSetRole(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String role, String catalogName)
    {
    }

    @Override
    public void checkCanShowRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String catalogName)
    {
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String catalogName)
    {
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, String catalogName)
    {
    }

    @Override
    public void checkCanCreateIndex(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDropIndex(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanRenameIndex(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName indexName, SchemaTableName newIndexName)
    {
    }

    @Override
    public void checkCanUpdateIndex(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName indexName)
    {
    }

    @Override
    public void checkCanShowIndex(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName indexName)
    {
    }

    @Override
    public Optional<ViewExpression> getRowFilter(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName, String columnName, Type type)
    {
        return Optional.empty();
    }

    @Override
    public void checkCanDropPartition(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, SchemaTableName tableName)
    {
    }
}
