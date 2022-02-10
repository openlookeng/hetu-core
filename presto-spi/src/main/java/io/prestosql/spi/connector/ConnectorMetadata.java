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
package io.prestosql.spi.connector;

import io.airlift.slice.Slice;
import io.prestosql.spi.PartialAndFinalAggregationType;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.GrantInfo;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.statistics.TableStatisticsMetadata;
import io.prestosql.spi.type.Type;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public interface ConnectorMetadata
{
    /**
     * Checks if a schema exists. The connector may have schemas that exist
     * but are not enumerable via {@link #listSchemaNames}.
     */
    default boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return listSchemaNames(session).contains(schemaName);
    }

    /**
     * Returns the schemas provided by this connector.
     */
    default List<String> listSchemaNames(ConnectorSession session)
    {
        return emptyList();
    }

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     */
    @Nullable
    default ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return null;
    }

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     * The returned table handle can contain information in analyzeProperties.
     */
    @Nullable
    default ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support analyze");
    }

    /**
     * Returns the system table for the specified table name, if one exists.
     * The system tables handled via {@link #getSystemTable} differ form those returned by {@link Connector#getSystemTables()}.
     * The former mechanism allows dynamic resolution of system tables, while the latter is
     * based on static list of system tables built during startup.
     */
    default Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return Optional.empty();
    }

    /**
     * Return a list of table layouts that satisfy the given constraint.
     * <p>
     * For each layout, connectors must return an "unenforced constraint" representing the part of the constraint summary that isn't guaranteed by the layout.
     */
    @Deprecated
    default List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        if (usesLegacyTableLayouts()) {
            throw new IllegalStateException("Connector uses legacy Table Layout but doesn't implement getTableLayouts()");
        }

        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Deprecated
    default ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        if (usesLegacyTableLayouts()) {
            throw new IllegalStateException("Connector uses legacy Table Layout but doesn't implement getTableLayout()");
        }

        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Return a table layout handle whose partitioning is converted to the provided partitioning handle,
     * but otherwise identical to the provided table layout handle.
     * The provided table layout handle must be one that the connector can transparently convert to from
     * the original partitioning handle associated with the provided table layout handle,
     * as promised by {@link #getCommonPartitioningHandle}.
     *
     * @deprecated use the version without layouts
     */
    @Deprecated
    default ConnectorTableLayoutHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableLayoutHandle tableLayoutHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getCommonPartitioningHandle() is implemented without makeCompatiblePartitioning()");
    }

    /**
     * Return a table handle whose partitioning is converted to the provided partitioning handle,
     * but otherwise identical to the provided table handle.
     * The provided table handle must be one that the connector can transparently convert to from
     * the original partitioning handle associated with the provided table handle,
     * as promised by {@link #getCommonPartitioningHandle}.
     */
    default ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getCommonPartitioningHandle() is implemented without makeCompatiblePartitioning()");
    }

    /**
     * Return a partitioning handle which the connector can transparently convert both {@code left} and {@code right} into.
     */
    default Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        if (left.equals(right)) {
            return Optional.of(left);
        }
        return Optional.empty();
    }

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    default ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getTableHandle() is implemented without getTableMetadata()");
    }

    /**
     * Return the connector-specific metadata for the specified table layout. This is the object that is passed to the event listener framework.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    @Deprecated
    default Optional<Object> getInfo(ConnectorTableLayoutHandle layoutHandle)
    {
        return Optional.empty();
    }

    default Optional<Object> getInfo(ConnectorTableHandle table)
    {
        return Optional.empty();
    }

    /**
     * List table and view names, possibly filtered by schema. An empty list is returned if none match.
     */
    default List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return emptyList();
    }

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    default Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getTableHandle() is implemented without getColumnHandles()");
    }

    /**
     * Gets the metadata for the specified table column.
     *
     * @throws RuntimeException if table or column handles are no longer valid
     */
    default ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getTableHandle() is implemented without getColumnMetadata()");
    }

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     */
    default Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }

    /**
     * Get statistics for table for given filtering constraint.
     */
    default TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint, boolean includeColumnStatistics)
    {
        return TableStatistics.empty();
    }

    /**
     * Creates a schema.
     */
    default void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    /**
     * Drops the specified schema.
     *
     * @throws PrestoException with {@code SCHEMA_NOT_EMPTY} if the schema is not empty
     */
    default void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    /**
     * Renames the specified schema.
     */
    default void renameSchema(ConnectorSession session, String source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    /**
     * Creates a table using the specified table metadata.
     *
     * @throws PrestoException with {@code ALREADY_EXISTS} if the table already exists and {@param ignoreExisting} is not set
     */
    default void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    default void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    /**
     * Rename the specified table
     */
    default void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    /**
     * Comments to the specified table
     */
    default void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support setting table comments");
    }

    /**
     * Add the specified column
     */
    default void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    /**
     * Rename the specified column
     */
    default void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    /**
     * Drop the specified column
     */
    default void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    /**
     * Returns the modification time of a table if possible.
     */
    default long getTableModificationTime(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "The connector does not support getting table modification time");
    }

    /**
     * Get the physical layout for a new table.
     */
    default Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return Optional.empty();
    }

    /**
     * Get the physical layout for a inserting into an existing table.
     */
    default Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorTableProperties properties = getTableProperties(session, tableHandle);
        return properties.getTablePartitioning()
                .map(partitioning -> {
                    Map<ColumnHandle, String> columnNamesByHandle = getColumnHandles(session, tableHandle).entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
                    List<String> partitionColumns = partitioning.getPartitioningColumns().stream()
                            .map(columnNamesByHandle::get)
                            .collect(toList());

                    return new ConnectorNewTableLayout(partitioning.getPartitioningHandle(), partitionColumns);
                });
    }

    /**
     * Get the physical layout for a inserting into an existing table.
     */
    default Optional<ConnectorNewTableLayout> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getInsertLayout(session, tableHandle);
    }

    /**
     * Describes statistics that must be collected during a write.
     */
    default TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return TableStatisticsMetadata.empty();
    }

    /**
     * Describe statistics that must be collected during a statistics collection
     */
    default TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getTableHandleForStatisticsCollection() is implemented without getStatisticsCollectionMetadata()");
    }

    /**
     * Begin statistics collection
     */
    default ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata getStatisticsCollectionMetadata() is implemented without beginStatisticsCollection()");
    }

    /**
     * Finish statistics collection
     */
    default void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginStatisticsCollection() is implemented without finishStatisticsCollection()");
    }

    /**
     * Begin the atomic creation of a table with data.
     */
    default ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    /**
     * Finish a table creation with data after the data is written.
     */
    default Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginCreateTable() is implemented without finishCreateTable()");
    }

    /**
     * Start a SELECT/UPDATE/INSERT/DELETE query. This notification is triggered after the planning phase completes.
     */
    default void beginQuery(ConnectorSession session) {}

    /**
     * Cleanup after a SELECT/UPDATE/INSERT/DELETE query. This is the very last notification after the query finishes, whether it succeeds or fails.
     * An exception thrown in this method will not affect the result of the query.
     */
    default void cleanupQuery(ConnectorSession session) {}

    /**
     * Begin insert query
     */
    default ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    /**
     * Begin insert query
     */
    default ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, boolean isOverwrite)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support inserts overwrite");
    }

    /**
     * Finish insert query
     */
    default Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginInsert() is implemented without finishInsert()");
    }

    /**
     * Get the column handle that will generate row IDs for the delete operation.
     * These IDs will be passed to the {@code deleteRows()} method of the
     * {@link io.prestosql.spi.connector.UpdatablePageSource} that created them.
     */
    default ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Get the column handle that will generate row IDs for the update operation.
     * These IDs will be passed to the {@code updateRows() method of the
     * {@link io.prestosql.spi.connector.UpdatablePageSource} that created them.
     */
    default ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support updates");
    }

    /**
     * Begin delete query
     */
    default ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Finish delete query
     *
     * @param fragments all fragments returned by {@link io.prestosql.spi.connector.UpdatablePageSource#finish()}
     */
    default void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Begin update query
     */
    default ConnectorUpdateTableHandle beginUpdateAsInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support updates");
    }

    default ConnectorDeleteAsInsertTableHandle beginDeletesAsInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support delete as insert");
    }

    /**
     * Finish Update query as insert
     */
    default Optional<ConnectorOutputMetadata> finishUpdateAsInsert(ConnectorSession session, ConnectorUpdateTableHandle updateHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginUpdate() is implemented without finishUpdate()");
    }

    /**
     * Do whatever is necessary to start an UPDATE query, returning the {@link ConnectorTableHandle}
     * instance that will be passed to split generation, and to the {@link #finishUpdate} method.
     * @param session The session in which to start the update operation.
     * @param tableHandle A ConnectorTableHandle for the table to be updated.
     * @param updatedColumnTypes A list of the ColumnHandles of columns that will be updated by this UPDATE
     * operation, in table column order.
     * @return a ConnectorTableHandle that will be passed to split generation, and to the
     * {@link #finishUpdate} method.
     */
    default ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<Type> updatedColumnTypes)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support updates");
    }

    /**
     * Finish an update query
     *
     * @param fragments all fragments returned by {@link io.prestosql.spi.connector.UpdatablePageSource#finish()}
     */
    default void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support updates");
    }

    /**
     * Finish delete as insert query
     */
    default Optional<ConnectorOutputMetadata> finishDeleteAsInsert(ConnectorSession session, ConnectorDeleteAsInsertTableHandle deleteHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginDeleteAsInsert() is implemented without finishUpdate()");
    }

    /**
     * Begin Vacuum query
     */
    default ConnectorVacuumTableHandle beginVacuum(ConnectorSession session, ConnectorTableHandle tableHandle, boolean full, boolean merge, Optional<String> partition)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support vacuum");
    }

    /**
     * Finish Vacuum query
     */
    default Optional<ConnectorOutputMetadata> finishVacuum(ConnectorSession session, ConnectorVacuumTableHandle updateHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata beginVacuum() is implemented without finishVacuum()");
    }

    /**
     * start vacuum scan
     */
    default List<ConnectorVacuumTableInfo> getTablesForVacuum()
    {
        return null;
    }

    /**
     * Create the specified view. The view definition is intended to
     * be serialized by the connector for permanent storage.
     */
    default void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating views");
    }

    /**
     * Drop the specified view.
     */
    default void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping views");
    }

    default List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return emptyList();
    }

    /**
     * Gets the definitions of views, possibly filtered by schema.
     * This optional method may be implemented by connectors that can support fetching
     * view data in bulk. It is used to implement {@code information_schema.views}.
     */
    default Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();
        for (SchemaTableName name : listViews(session, schemaName)) {
            getView(session, name).ifPresent(view -> views.put(name, view));
        }
        return views;
    }

    /**
     * Gets the view data for the specified view name.
     */
    default Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    /**
     * @return whether delete without table scan is supported
     */
    default boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Delete the provided table layout
     *
     * @return number of rows deleted, or null for unknown
     */
    default OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    /**
     * Attempt to push down a delete operation into the connector. If a connector
     * can execute a delete for the table handle on its own, it should return a
     * table handle, which will be passed back to {@link #executeDelete} during
     * query executing to actually execute the delete.
     */
    default Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.empty();
    }

    /**
     * Attempt to push down a delete operation into the connector. If a connector
     * can execute a delete for the table handle on its own, it should return a
     * table handle, which will be passed back to {@link #executeDelete} during
     * query executing to actually execute the delete.
     * This supports filtering partitions based on constraint.
     */
    default Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        return Optional.empty();
    }

    /**
     * Execute the delete operation on the handle returned from {@link #applyDelete}.
     */
    default OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata applyDelete() is implemented without executeDelete()");
    }

    /**
     * Execute the delete operation on the handle returned from {@link #applyDelete}.
     */
    default OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "ConnectorMetadata applyDelete() is implemented without executeDelete()");
    }

    /**
     * Try to locate a table index that can lookup results by indexableColumns and provide the requested outputColumns.
     */
    default Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        return Optional.empty();
    }

    /**
     * Creates the specified role.
     *
     * @param grantor represents the principal specified by WITH ADMIN statement
     */
    default void createRole(ConnectorSession session, String role, Optional<PrestoPrincipal> grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support create role");
    }

    /**
     * Drops the specified role.
     */
    default void dropRole(ConnectorSession session, String role)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support drop role");
    }

    /**
     * List available roles.
     */
    default Set<String> listRoles(ConnectorSession session)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * List role grants for a given principal, not recursively.
     */
    default Set<RoleGrant> listRoleGrants(ConnectorSession session, PrestoPrincipal principal)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * Grants the specified roles to the specified grantees
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    default void grantRoles(ConnectorSession connectorSession, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * Revokes the specified roles from the specified grantees
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    default void revokeRoles(ConnectorSession connectorSession, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * List applicable roles, including the transitive grants, for the specified principal
     */
    default Set<RoleGrant> listApplicableRoles(ConnectorSession session, PrestoPrincipal principal)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * List applicable roles, including the transitive grants, in given session
     */
    default Set<String> listEnabledRoles(ConnectorSession session)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * Grants the specified privilege to the specified user on the specified table
     */
    default void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support grants");
    }

    /**
     * Revokes the specified privilege on the specified table from the specified user
     */
    default void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support revokes");
    }

    /**
     * List the table privileges granted to the specified grantee for the tables that have the specified prefix considering the selected session role
     */
    default List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyList();
    }

    /**
     * Whether the connector uses the legacy Table Layout feature. If this method returns false,
     * connectors are required to implement the following methods:
     * <ul>
     * <li>{@link #getTableProperties(ConnectorSession session, ConnectorTableHandle table)}</li>
     * <li>{@link #getInfo(ConnectorTableHandle table)} </li>
     * <li>{@link ConnectorSplitManager#getSplits(ConnectorTransactionHandle, ConnectorSession, ConnectorTableHandle, ConnectorSplitManager.SplitSchedulingStrategy)}</li>
     * </ul>
     */
    default boolean usesLegacyTableLayouts()
    {
        return true;
    }

    default ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        if (!usesLegacyTableLayouts()) {
            throw new IllegalStateException("getTableProperties() must be implemented if usesLegacyTableLayouts is false");
        }

        List<ConnectorTableLayoutResult> layouts = getTableLayouts(session, table, Constraint.alwaysTrue(), Optional.empty());

        if (layouts.size() != 1) {
            throw new PrestoException(NOT_SUPPORTED, format("Connector must return a single layout for table %s, but got %s", table, layouts.size()));
        }

        return new ConnectorTableProperties(layouts.get(0).getTableLayout());
    }

    /**
     * Attempt to push down the provided limit into the table.
     * <p>
     * Connectors can indicate whether they don't support limit pushdown or that the action had no effect
     * by returning {@link Optional#empty()}. Connectors should expect this method to be called multiple times
     * during the optimization of a given query.
     * <p>
     * <b>Note</b>: it's critical for connectors to return Optional.empty() if calling this method has no effect for that
     * invocation, even if the connector generally supports limit pushdown. Doing otherwise can cause the optimizer
     * to loop indefinitely.
     * </p>
     * <p>
     * If the connector could benefit from the information but can't guarantee that it will be able to produce
     * fewer rows than the provided limit, it should return a non-empty result containing a new handle for the
     * derived table and the "limit guaranteed" flag set to false.
     * <p>
     * If the connector can guarantee it will produce fewer rows than the provided limit, it should return a
     * non-empty result with the "limit guaranteed" flag set to true.
     */
    default Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        return Optional.empty();
    }

    /**
     * Attempt to push down the provided constraint into the table. This method is provided as replacement to
     * {@link ConnectorMetadata#getTableLayouts(ConnectorSession, ConnectorTableHandle, Constraint, Optional)} to ease
     * migration for the legacy API.
     * <p>
     * Connectors can indicate whether they don't support predicate pushdown or that the action had no effect
     * by returning {@link Optional#empty()}. Connectors should expect this method to be called multiple times
     * during the optimization of a given query.
     * <p>
     * <b>Note</b>: it's critical for connectors to return Optional.empty() if calling this method has no effect for that
     * invocation, even if the connector generally supports pushdown. Doing otherwise can cause the optimizer
     * to loop indefinitely.
     * </p>
     */
    default Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        return Optional.empty();
    }

    /**
     * Attempt to push down the provided constraint into the table. This method is provided as replacement to
     * {@link ConnectorMetadata#getTableLayouts(ConnectorSession, ConnectorTableHandle, Constraint, Optional)} to ease
     * migration for the legacy API.
     * <p>
     * Connectors can indicate whether they don't support predicate pushdown or that the action had no effect
     * by returning {@link Optional#empty()}. Connectors should expect this method to be called multiple times
     * during the optimization of a given query.
     * <p>
     * <b>Note</b>: it's critical for connectors to return Optional.empty() if calling this method has no effect for that
     * invocation, even if the connector generally supports pushdown. Doing otherwise can cause the optimizer
     * to loop indefinitely.
     * </p>
     */
    default Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint, List<Constraint> disjuctConstaints, Set<ColumnHandle> allColumnHandles, boolean pushParitionOnly)
    {
        return applyFilter(session, handle, constraint);
    }

    /**
     * Attempt to push down the provided projections into the table.
     * <p>
     * Connectors can indicate whether they don't support projection pushdown or that the action had no effect
     * by returning {@link Optional#empty()}. Connectors should expect this method to be called multiple times
     * during the optimization of a given query.
     * <p>
     * <b>Note</b>: it's critical for connectors to return Optional.empty() if calling this method has no effect for that
     * invocation, even if the connector generally supports pushdown. Doing otherwise can cause the optimizer
     * to loop indefinitely.
     * </p>
     * <p>
     * If the method returns a result, the list of projections in the result *replaces* the existing ones, and the
     * list of assignments is the new set of columns exposed by the derived table.
     * <p>
     * As an example, given the following plan:
     *
     * <pre>
     * - project
     *     x = f1(a, b)
     *     y = f2(a, b)
     *     z = f3(a, b)
     *   - scan (TH0)
     *       a = CH0
     *       b = CH1
     *       c = CH2
     * </pre>
     * <p>
     * The optimizer would call {@link #applyProjection} with the following arguments:
     *
     * <pre>
     * handle = TH0
     * projections = [
     *     f1(a, b)
     *     f2(a, b)
     *     f3(a, b)
     * ]
     * assignments = [
     *     a = CH0
     *     b = CH1
     *     c = CH2
     * ]
     * </pre>
     * <p>
     * Assuming the connector knows how to handle f1(...) and f2(...), it would return:
     *
     * <pre>
     * handle = TH1
     * projections = [
     *     v2
     *     v3
     *     f3(v0, v1)
     * ]
     * assignments = [
     *     v0 = CH0
     *     v1 = CH1
     *     v2 = CH3  (synthetic column for f1(CH0, CH1))
     *     v3 = CH4  (synthetic column for f2(CH0, CH1))
     * ]
     * </pre>
     */
    default Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return Optional.empty();
    }

    /**
     * Attempt to push down the sampling into the table.
     * <p>
     * Connectors can indicate whether they don't support sample pushdown or that the action had no effect
     * by returning {@link Optional#empty()}. Connectors should expect this method to be called multiple times
     * during the optimization of a given query.
     * <p>
     * <b>Note</b>: it's critical for connectors to return Optional.empty() if calling this method has no effect for that
     * invocation, even if the connector generally supports sample pushdown. Doing otherwise can cause the optimizer
     * to loop indefinitely.
     * </p>
     */
    default Optional<ConnectorTableHandle> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio)
    {
        return Optional.empty();
    }

    /**
     * Hetu can only cache execution plans for supported connectors.
     * By default, caching is not enabled for connectors and must be explicitly overwritten.
     * <p>
     * Other methods to be overwritten:
     * io.prestosql.spi.connector.ColumnHandle#getColumnName()
     * io.prestosql.spi.connector.ConnectorTableHandle#createFrom(io.prestosql.spi.connector.ConnectorTableHandle)
     * io.prestosql.spi.connector.ConnectorTableHandle#getSchemaPrefixedTableName()
     *
     * @param session Hetu session
     * @param handle Connector specific table handle
     */
    default boolean isExecutionPlanCacheSupported(ConnectorSession session, ConnectorTableHandle handle)
    {
        return false;
    }

    /**
     * Hetu can only create index for supported connectors.
     */
    default boolean isHeuristicIndexSupported()
    {
        return false;
    }

    /**
     * Hetu can only support pre-aggregation for supported connectors.
     *
     * @param session Hetu session
     * @return true, if connector supports pre aggregation
     *         false, otherwise
     */
    default boolean isPreAggregationSupported(ConnectorSession session)
    {
        return false;
    }

    /**
     * Whether this table can be used as input for snapshot-enabled query executions.
     *
     * @param session Presto session
     * @param table Connector specific table handle
     */
    default boolean isSnapshotSupportedAsInput(ConnectorSession session, ConnectorTableHandle table)
    {
        // Most connectors do *not* support snapshot. Only Hive, TPCDS, and TPCH support it.
        return false;
    }

    /**
     * Whether this table can be used as output for snapshot-enabled query executions
     *
     * @param session Presto session
     * @param table Connector specific table handle
     */
    default boolean isSnapshotSupportedAsOutput(ConnectorSession session, ConnectorTableHandle table)
    {
        // Most connectors do *not* support snapshot. Only Hive with ORC format supports it.
        return false;
    }

    /**
     * Whether new table with specified format can be used as output for snapshot-enabled
     *
     * @param session Presto session
     * @param tableProperties Table properties
     */
    default boolean isSnapshotSupportedAsNewTable(ConnectorSession session, Map<String, Object> tableProperties)
    {
        // Most connectors do *not* support snapshot. Only Hive with ORC format supports it.
        return false;
    }

    /**
     * Snapshot: Remove any previous changes from previous execution attempt, to prepare for query resume
     */
    default void resetInsertForRerun(ConnectorSession session, ConnectorInsertTableHandle tableHandle, OptionalLong snapshotIndex)
    {
        throw new UnsupportedOperationException("This connector does not support query resuming");
    }

    /**
     * Snapshot: Remove any previous changes from previous execution attempt, to prepare for query resume
     */
    default void resetCreateForRerun(ConnectorSession session, ConnectorOutputTableHandle tableHandle, OptionalLong snapshotIndex)
    {
        throw new UnsupportedOperationException("This connector does not support query resuming");
    }

    default PartialAndFinalAggregationType validateAndGetSortAggregationType(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> keyNames)
    {
        PartialAndFinalAggregationType partialAndFinalAggregationType = new PartialAndFinalAggregationType();
        return partialAndFinalAggregationType;
    }

    default void refreshMetadataCache()
    {
        throw new UnsupportedOperationException("This connector does not support refreshing metadata cache");
    }

    default void dropPartition(ConnectorSession toConnectorSession, ConnectorTableHandle connectorHandle, List<Map<String, String>> partitions, boolean ifExists, List<Map<String, String>> operatorMap)
    {
        throw new UnsupportedOperationException("This connector does not support drop partition");
    }
}
