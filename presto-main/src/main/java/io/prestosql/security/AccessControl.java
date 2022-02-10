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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.transaction.TransactionId;

import java.security.Principal;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface AccessControl
{
    /**
     * Check if the principal is allowed to be the specified user.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     * @deprecated replaced with user mapping during authentication and {@link #checkCanImpersonateUser}
     */
    @Deprecated
    void checkCanSetUser(Optional<Principal> principal, String userName);

    /**
     * Check if the identity is allowed impersonate the specified user.
     *
     * @throws AccessDeniedException if not allowed
     */
    void checkCanImpersonateUser(Identity identity, String userName);

    /**
     * Filter the list of catalogs to those visible to the identity.
     */
    Set<String> filterCatalogs(Identity identity, Set<String> catalogs);

    /**
    * Check whether identity is allowed to access catalogs
    */
    default void checkCanAccessCatalogs(Identity identity) {}

    /**
     * Check whether identity is allowed to access catalog
     */
    void checkCanAccessCatalog(Identity identity, String catalogName);

    /**
     * Check whether identity is can access the node info
     */
    void checkCanAccessNodeInfo(Identity identity);

    /**
     * Check whether identity is can create a catalog
     */
    default void checkCanCreateCatalog(Identity identity, String catalogName) {}

    /**
     * Check if identity is allowed to drop the specified catalog
     */
    default void checkCanDropCatalog(Identity identity, String catalogName) {}

    /**
     * Check if identity is allowed to update the specified catalog
     */
    default void checkCanUpdateCatalog(Identity identity, String catalogName) {}

    /**
     * Check if identity is allowed to update the specified table
     */
    default void checkCanUpdateTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName) {}

    /**
     * Check if identity is allowed to create the specified schema.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName);

    /**
     * Check if identity is allowed to drop the specified schema.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName);

    /**
     * Check if identity is allowed to rename the specified schema.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRenameSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName, String newSchemaName);

    /**
     * Check if identity is allowed to execute SHOW SCHEMAS in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterSchemas} method must filter all results for unauthorized users,
     * since there are multiple ways to list schemas.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowSchemas(TransactionId transactionId, Identity identity, String catalogName);

    /**
     * Filter the list of schemas in a catalog to those visible to the identity.
     */
    Set<String> filterSchemas(TransactionId transactionId, Identity identity, String catalogName, Set<String> schemaNames);

    /**
     * Check if identity is allowed to create the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to drop the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to rename the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRenameTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, QualifiedObjectName newTableName);

    /**
     * Check if identity is allowed to comment the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetTableComment(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to show metadata of tables by executing SHOW TABLES, SHOW GRANTS etc. in a catalog.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterTables} method must filter all results for unauthorized users,
     * since there are multiple ways to list tables.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowTablesMetadata(TransactionId transactionId, Identity identity, CatalogSchemaName schema);

    /**
     * Filter the list of tables and views to those visible to the identity.
     */
    Set<SchemaTableName> filterTables(TransactionId transactionId, Identity identity, String catalogName, Set<SchemaTableName> tableNames);

    /**
     * Check if identity is allowed to show columns of tables by executing SHOW COLUMNS, DESCRIBE etc.
     * <p>
     * NOTE: This method is only present to give users an error message when listing is not allowed.
     * The {@link #filterColumns} method must filter all results for unauthorized users,
     * since there are multiple ways to list columns.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowColumnsMetadata(TransactionId transactionId, Identity identity, CatalogSchemaTableName table);

    /**
     * Filter the list of columns to those visible to the identity.
     */
    List<ColumnMetadata> filterColumns(TransactionId transactionId, Identity identity, CatalogSchemaTableName tableName, List<ColumnMetadata> columns);

    /**
     * Check if identity is allowed to add columns to the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanAddColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to drop columns from the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropColumn(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to rename a column in the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRenameColumn(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to insert into the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanInsertIntoTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to delete from the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDeleteFromTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to create the specified index.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateIndex(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to drop the specified index.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropIndex(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to rename the specified index.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRenameIndex(TransactionId transactionId, Identity identity, QualifiedObjectName indexName, QualifiedObjectName newIndexName);

    /**
     * Check if identity is allowed to update the specified index.
     */
    default void checkCanUpdateIndex(TransactionId transactionId, Identity identity, QualifiedObjectName tableName) {}

    /**
     * Check if identity is allowed to show the specified index.
     */
    void checkCanShowIndex(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);

    /**
     * Check if identity is allowed to create the specified view.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName);

    /**
     * Check if identity is allowed to drop the specified view.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName);

    /**
     * Check if identity is allowed to create a view that selects from the specified columns.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateViewWithSelectFromColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, Set<String> columnNames);

    /**
     * Check if identity is allowed to grant a privilege to the grantee on the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanGrantTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal grantee, boolean withGrantOption);

    /**
     * Check if identity is allowed to revoke a privilege from the revokee on the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRevokeTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal revokee, boolean grantOptionFor);

    /**
     * Check if identity is allowed to set the specified system property.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetSystemSessionProperty(Identity identity, String propertyName);

    /**
     * Check if identity is allowed to set the specified catalog property.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, String catalogName, String propertyName);

    /**
     * Check if identity is allowed to select from the specified columns.  The column set can be empty.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSelectFromColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, Set<String> columnNames);

    /**
     * Check if identity is allowed to create the specified role.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanCreateRole(TransactionId transactionId, Identity identity, String role, Optional<PrestoPrincipal> grantor, String catalogName);

    /**
     * Check if identity is allowed to drop the specified role.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropRole(TransactionId transactionId, Identity identity, String role, String catalogName);

    /**
     * Check if identity is allowed to grant the specified roles to the specified principals.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanGrantRoles(TransactionId transactionId, Identity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName);

    /**
     * Check if identity is allowed to revoke the specified roles from the specified principals.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanRevokeRoles(TransactionId transactionId, Identity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName);

    /**
     * Check if identity is allowed to set role for specified catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanSetRole(TransactionId requiredTransactionId, Identity identity, String role, String catalog);

    /**
     * Check if identity is allowed to show roles on the specified catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowRoles(TransactionId transactionId, Identity identity, String catalogName);

    /**
     * Check if identity is allowed to show current roles on the specified catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowCurrentRoles(TransactionId transactionId, Identity identity, String catalogName);

    /**
     * Check if identity is allowed to show its own role grants on the specified catalog.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanShowRoleGrants(TransactionId transactionId, Identity identity, String catalogName);

    /**
     * Get a row filter associated with the given table and identity.
     *
     * The filter must be a scalar SQL expression of boolean type over the columns in the table.
     *
     * @return the filter, or {@link Optional#empty()} if not applicable
     */
    default List<ViewExpression> getRowFilters(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        return ImmutableList.of();
    }

    /**
     * Get a column mask associated with the given table, column and identity.
     *
     * The mask must be a scalar SQL expression of a type coercible to the type of the column being masked. The expression
     * must be written in terms of columns in the table.
     *
     * @return the mask, or {@link Optional#empty()} if not applicable
     */
    default List<ViewExpression> getColumnMasks(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, String columnName, Type type)
    {
        return ImmutableList.of();
    }

    /**
     * Check if identity is allowed to drop partitions from the specified table.
     *
     * @throws io.prestosql.spi.security.AccessDeniedException if not allowed
     */
    void checkCanDropPartition(TransactionId transactionId, Identity identity, QualifiedObjectName tableName);
}
