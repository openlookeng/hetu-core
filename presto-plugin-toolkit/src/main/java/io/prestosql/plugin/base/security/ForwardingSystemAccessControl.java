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
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;

import java.security.Principal;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public abstract class ForwardingSystemAccessControl
        implements SystemAccessControl
{
    public static SystemAccessControl of(Supplier<SystemAccessControl> systemAccessControlSupplier)
    {
        requireNonNull(systemAccessControlSupplier, "systemAccessControlSupplier is null");
        return new ForwardingSystemAccessControl()
        {
            @Override
            protected SystemAccessControl delegate()
            {
                return systemAccessControlSupplier.get();
            }
        };
    }

    protected abstract SystemAccessControl delegate();

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        delegate().checkCanSetUser(principal, userName);
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        delegate().checkCanImpersonateUser(identity, userName);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        delegate().checkCanSetSystemSessionProperty(identity, propertyName);
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        delegate().checkCanAccessCatalog(identity, catalogName);
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return delegate().filterCatalogs(identity, catalogs);
    }

    @Override
    public void checkCanShowCatalogs(Identity identity)
    {
        delegate().checkCanShowCatalogs(identity);
    }

    @Override
    public void checkCanCreateCatalog(Identity identity, String catalogName)
    {
        delegate().checkCanCreateCatalog(identity, catalogName);
    }

    @Override
    public void checkCanDropCatalog(Identity identity, String catalogName)
    {
        delegate().checkCanDropCatalog(identity, catalogName);
    }

    @Override
    public void checkCanUpdateCatalog(Identity identity, String catalogName)
    {
        delegate().checkCanUpdateCatalog(identity, catalogName);
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
        delegate().checkCanCreateSchema(identity, schema);
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
        delegate().checkCanDropSchema(identity, schema);
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
        delegate().checkCanRenameSchema(identity, schema, newSchemaName);
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
        delegate().checkCanShowSchemas(identity, catalogName);
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        return delegate().filterSchemas(identity, catalogName, schemaNames);
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanCreateTable(identity, table);
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanDropTable(identity, table);
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        delegate().checkCanRenameTable(identity, table, newTable);
    }

    @Override
    public void checkCanSetTableComment(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanSetTableComment(identity, table);
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
        delegate().checkCanShowTablesMetadata(identity, schema);
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        return delegate().filterTables(identity, catalogName, tableNames);
    }

    @Override
    public void checkCanShowColumnsMetadata(Identity identity, CatalogSchemaTableName tableName)
    {
        delegate().checkCanShowColumnsMetadata(identity, tableName);
    }

    @Override
    public List<ColumnMetadata> filterColumns(Identity identity, CatalogSchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return delegate().filterColumns(identity, tableName, columns);
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanAddColumn(identity, table);
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanDropColumn(identity, table);
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanRenameColumn(identity, table);
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        delegate().checkCanSelectFromColumns(identity, table, columns);
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanInsertIntoTable(identity, table);
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanDeleteFromTable(identity, table);
    }

    @Override
    public void checkCanUpdateTable(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanUpdateTable(identity, table);
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        delegate().checkCanCreateView(identity, view);
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        delegate().checkCanDropView(identity, view);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        delegate().checkCanCreateViewWithSelectFromColumns(identity, table, columns);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        delegate().checkCanSetCatalogSessionProperty(identity, catalogName, propertyName);
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption)
    {
        delegate().checkCanGrantTablePrivilege(identity, privilege, table, grantee, withGrantOption);
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        delegate().checkCanRevokeTablePrivilege(identity, privilege, table, revokee, grantOptionFor);
    }

    @Override
    public void checkCanShowRoles(Identity identity, String catalogName)
    {
        delegate().checkCanShowRoles(identity, catalogName);
    }

    @Override
    public void checkCanAccessNodeInfo(Identity identity)
    {
        delegate().checkCanAccessNodeInfo(identity);
    }

    @Override
    public Optional<ViewExpression> getRowFilter(Identity identity, CatalogSchemaTableName tableName)
    {
        return delegate().getRowFilter(identity, tableName);
    }

    @Override
    public Optional<ViewExpression> getColumnMask(Identity identity, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        return delegate().getColumnMask(identity, tableName, columnName, type);
    }

    @Override
    public void checkCanCreateIndex(Identity identity, CatalogSchemaTableName index)
    {
        delegate().checkCanCreateIndex(identity, index);
    }

    @Override
    public void checkCanDropIndex(Identity identity, CatalogSchemaTableName index)
    {
        delegate().checkCanDropIndex(identity, index);
    }

    @Override
    public void checkCanRenameIndex(Identity identity, CatalogSchemaTableName index, CatalogSchemaTableName newIndex)
    {
        delegate().checkCanRenameIndex(identity, index, newIndex);
    }

    @Override
    public void checkCanUpdateIndex(Identity identity, CatalogSchemaTableName index)
    {
        delegate().checkCanUpdateIndex(identity, index);
    }

    @Override
    public void checkCanShowIndex(Identity identity, CatalogSchemaTableName index)
    {
        delegate().checkCanShowIndex(identity, index);
    }

    @Override
    public void checkCanDropPartition(Identity identity, CatalogSchemaTableName table)
    {
        delegate().checkCanDropPartition(identity, table);
    }
}
