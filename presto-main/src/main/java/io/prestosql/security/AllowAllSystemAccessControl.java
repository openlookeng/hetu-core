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
package io.prestosql.security;

import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;

import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AllowAllSystemAccessControl
        implements SystemAccessControl
{
    public static final String NAME = "allow-all";

    private static final AllowAllSystemAccessControl INSTANCE = new AllowAllSystemAccessControl();

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return catalogs;
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
    }

    @Override
    public void checkCanSetTableComment(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanShowColumnsMetadata(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public List<ColumnMetadata> filterColumns(Identity identity, CatalogSchemaTableName tableName, List<ColumnMetadata> columns)
    {
        return columns;
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanCreateIndex(Identity identity, CatalogSchemaTableName index)
    {
    }

    @Override
    public void checkCanDropIndex(Identity identity, CatalogSchemaTableName index)
    {
    }

    @Override
    public void checkCanRenameIndex(Identity identity, CatalogSchemaTableName index, CatalogSchemaTableName newIndex)
    {
    }

    @Override
    public void checkCanUpdateIndex(Identity identity, CatalogSchemaTableName index)
    {
    }

    @Override
    public void checkCanShowIndex(Identity identity, CatalogSchemaTableName index)
    {
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor)
    {
    }

    @Override
    public void checkCanShowRoles(Identity identity, String catalogName)
    {
    }

    @Override
    public void checkCanShowCatalogs(Identity identity)
    {
    }

    @Override
    public void checkCanCreateCatalog(Identity identity, String catalogName)
    {
    }

    @Override
    public void checkCanDropCatalog(Identity identity, String catalogName)
    {
    }

    @Override
    public void checkCanUpdateCatalog(Identity identity, String catalogName)
    {
    }

    @Override
    public void checkCanUpdateTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    public static class Factory
            implements SystemAccessControlFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            requireNonNull(config, "config is null");
            checkArgument(config.isEmpty(), "This access controller does not support any configuration properties");
            return INSTANCE;
        }
    }

    @Override
    public void checkCanAccessNodeInfo(Identity identity)
    {
    }

    @Override
    public Optional<ViewExpression> getRowFilter(Identity identity, CatalogSchemaTableName tableName)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(Identity identity, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        return Optional.empty();
    }

    @Override
    public void checkCanDropPartition(Identity identity, CatalogSchemaTableName table)
    {
    }
}
