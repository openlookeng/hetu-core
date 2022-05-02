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

package io.prestosql.metadata;

import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.spi.PartialAndFinalAggregationType;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorCapabilities;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.connector.SampleType;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.function.SqlFunction;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.GrantInfo;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.statistics.TableStatisticsMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.planner.PartitioningHandle;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.LongSupplier;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;

public interface Metadata
{
    Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogName catalogName);

    boolean catalogExists(Session session, String catalogName);

    boolean schemaExists(Session session, CatalogSchemaName schema);

    List<String> listSchemaNames(Session session, String catalogName);

    /**
     * Returns a table handle for the specified table name.
     */
    Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName);

    Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName);

    Optional<TableHandle> getTableHandleForStatisticsCollection(Session session, QualifiedObjectName tableName, Map<String, Object> analyzeProperties);

    @Deprecated
    Optional<TableLayoutResult> getLayout(Session session, TableHandle tableHandle, Constraint constraint, Optional<Set<ColumnHandle>> desiredColumns);

    TableProperties getTableProperties(Session session, TableHandle handle);

    /**
     * Return a table handle whose partitioning is converted to the provided partitioning handle,
     * but otherwise identical to the provided table handle.
     * The provided table handle must be one that the connector can transparently convert to from
     * the original partitioning handle associated with the provided table handle,
     * as promised by {@link #getCommonPartitioning}.
     */
    TableHandle makeCompatiblePartitioning(Session session, TableHandle table, PartitioningHandle partitioningHandle);

    /**
     * Return a partitioning handle which the connector can transparently convert both {@code left} and {@code right} into.
     */
    Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right);

    Optional<Object> getInfo(Session session, TableHandle handle);

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    TableMetadata getTableMetadata(Session session, TableHandle tableHandle);

    /**
     * Return statistics for specified table for given filtering contraint with a check either to include ColumnStatistics or not
     */
    TableStatistics getTableStatistics(Session session, TableHandle tableHandle, Constraint constraint, boolean includeColumnStatistics);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix);

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle);

    /**
     * Gets the metadata for the specified table column.
     *
     * @throws RuntimeException if table or column handles are no longer valid
     */
    ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     */
    Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix);

    /**
     * Creates a schema.
     */
    void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties);

    /**
     * Drops the specified schema.
     */
    void dropSchema(Session session, CatalogSchemaName schema);

    /**
     * Renames the specified schema.
     */
    void renameSchema(Session session, CatalogSchemaName source, String target);

    /**
     * Creates a table using the specified table metadata.
     *
     * @throws PrestoException with {@code ALREADY_EXISTS} if the table already exists and {@param ignoreExisting} is not set
     */
    void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting);

    /**
     * Rename the specified table.
     */
    void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName);

    /**
     * Comments to the specified table.
     */
    void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment);

    /**
     * Rename the specified column.
     */
    void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target);

    /**
     * Add the specified column to the table.
     */
    void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column);

    /**
     * Drop the specified column.
     */
    void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column);

    /**
     * Drops the specified table
     *
     * @throws RuntimeException if the table can not be dropped or table handle is no longer valid
     */
    void dropTable(Session session, TableHandle tableHandle);

    Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata);

    /**
     * Begin the atomic creation of a table with data.
     */
    OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout);

    /**
     * Finish a table creation with data after the data is written.
     */
    Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);

    Optional<NewTableLayout> getInsertLayout(Session session, TableHandle target);

    /**
     * For the Table UPDATE/DELETE Layouts if supported.
     *
     * @return
     */
    default Optional<NewTableLayout> getUpdateLayout(Session session, TableHandle target)
    {
        return Optional.empty();
    }

    /**
     * Describes statistics that must be collected during a write.
     */
    TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, String catalogName, ConnectorTableMetadata tableMetadata);

    /**
     * Describe statistics that must be collected during a statistics collection
     */
    TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata);

    /**
     * Begin statistics collection
     */
    AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle);

    /**
     * Finish statistics collection
     */
    void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics);

    /**
     * Cleanup after a query. This is the very last notification after the query finishes, regardless if it succeeds or fails.
     * An exception thrown in this method will not affect the result of the query.
     */
    void cleanupQuery(Session session);

    /**
     * Begin insert query
     */
    InsertTableHandle beginInsert(Session session, TableHandle tableHandle, boolean isOverwrite);

    /**
     * Finish insert query
     */
    Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);

    /**
     * Get the row ID column handle used with UpdatablePageSource#deleteRows.
     */
    ColumnHandle getDeleteRowIdColumnHandle(Session session, TableHandle tableHandle);

    /**
     * Get the row ID column handle used with UpdatablePageSource#updateRows.
     */
    ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns);

    /**
     * @return whether delete without table scan is supported
     */
    boolean supportsMetadataDelete(Session session, TableHandle tableHandle);

    /**
     * Push delete into connector
     */
    Optional<TableHandle> applyDelete(Session session, TableHandle tableHandle, Constraint constraint);

    /**
     * Push delete into connector
     */
    Optional<TableHandle> applyDelete(Session session, TableHandle tableHandle);

    /**
     * Execute delete in connector
     */
    OptionalLong executeDelete(Session session, TableHandle tableHandle);

    OptionalLong executeUpdate(Session session, TableHandle tableHandle);

    /**
     * Begin delete query
     */
    TableHandle beginDelete(Session session, TableHandle tableHandle);

    /**
     * Begin update query
     */
    default UpdateTableHandle beginUpdateAsInsert(Session session, TableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "update not supported yet for this connector!!");
    }

    /**
     * Begin update query
     */
    default TableHandle beginUpdate(Session session, TableHandle tableHandle, List<Type> updatedColumnTypes)
    {
        throw new PrestoException(NOT_SUPPORTED, "update not supported yet for this connector!!");
    }

    /**
     * Finish update query
     */
    default void finishUpdate(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "update not supported yet for this connector!!");
    }

    /**
     * Begin DeletAsInsert query
     */
    default DeletesAsInsertTableHandle beginDeletAsInsert(Session session, TableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "DeletAsInsert not supported yet for this connector!!");
    }

    default Optional<ConnectorOutputMetadata> finishUpdateAsInsert(Session session, UpdateTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(NOT_SUPPORTED, "update not supported yet for this connector!!");
    }

    default Optional<ConnectorOutputMetadata> finishDeleteAsInsert(Session session, DeletesAsInsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(NOT_SUPPORTED, "Delete as Insert  not supported yet for this connector!!");
    }

    /**
     * Begin update query
     *
     * @return
     */
    default VacuumTableHandle beginVacuum(Session session, TableHandle tableHandle, boolean full, boolean merge, Optional<String> partition)
    {
        throw new PrestoException(NOT_SUPPORTED, "vacuum not supported yet for this connector!!");
    }

    default Optional<ConnectorOutputMetadata> finishVacuum(Session session, VacuumTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new PrestoException(NOT_SUPPORTED, "vacuum not supported yet for this connector!!");
    }

    /**
     * Finish delete query
     */
    void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments);

    /**
     * Returns a connector id for the specified catalog name.
     */
    Optional<CatalogName> getCatalogHandle(Session session, String catalogName);

    /**
     * Gets all the loaded catalogs
     *
     * @return Map of catalog name to connector id
     */
    Map<String, CatalogName> getCatalogNames(Session session);

    /**
     * Get the names that match the specified table prefix (never null).
     */
    List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Get the view definitions that match the specified table prefix (never null).
     */
    Map<QualifiedObjectName, ConnectorViewDefinition> getViews(Session session, QualifiedTablePrefix prefix);

    /**
     * Returns the view definition for the specified view name.
     */
    Optional<ConnectorViewDefinition> getView(Session session, QualifiedObjectName viewName);

    /**
     * Creates the specified view with the specified view definition.
     */
    void createView(Session session, QualifiedObjectName viewName, ConnectorViewDefinition definition, boolean replace);

    /**
     * Drops the specified view.
     */
    void dropView(Session session, QualifiedObjectName viewName);

    /**
     * Try to locate a table index that can lookup results by indexableColumns and provide the requested outputColumns.
     */
    Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain);

    @Deprecated
    boolean usesLegacyTableLayouts(Session session, TableHandle table);

    Optional<LimitApplicationResult<TableHandle>> applyLimit(Session session, TableHandle table, long limit);

    Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint);

    default Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table,
            Constraint constraint,
            List<Constraint> disjunctConstraints,
            Set<ColumnHandle> allColumnHandles,
            boolean pushPartitionsOnly)
    {
        return applyFilter(session, table, constraint);
    }

    Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments);

    Optional<TableHandle> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio);

    //
    // Roles and Grants
    //

    /**
     * Creates the specified role in the specified catalog.
     *
     * @param grantor represents the principal specified by WITH ADMIN statement
     */
    void createRole(Session session, String role, Optional<PrestoPrincipal> grantor, String catalog);

    /**
     * Drops the specified role in the specified catalog.
     */
    void dropRole(Session session, String role, String catalog);

    /**
     * List available roles in specified catalog.
     */
    Set<String> listRoles(Session session, String catalog);

    /**
     * List roles grants in the specified catalog for a given principal, not recursively.
     */
    Set<RoleGrant> listRoleGrants(Session session, String catalog, PrestoPrincipal principal);

    /**
     * Grants the specified roles to the specified grantees in the specified catalog
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    void grantRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalog);

    /**
     * Revokes the specified roles from the specified grantees in the specified catalog
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    void revokeRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalog);

    /**
     * List applicable roles, including the transitive grants, for the specified principal
     */
    Set<RoleGrant> listApplicableRoles(Session session, PrestoPrincipal principal, String catalog);

    /**
     * List applicable roles, including the transitive grants, in given session
     */
    Set<String> listEnabledRoles(Session session, String catalog);

    /**
     * Grants the specified privilege to the specified user on the specified table
     */
    void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption);

    /**
     * Revokes the specified privilege on the specified table from the specified user
     */
    void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption);

    /**
     * Gets the privileges for the specified table available to the given grantee considering the selected session role
     */
    List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix);

    //
    // Types
    //

    Type getType(TypeSignature signature);

    void verifyComparableOrderableContract();

    List<SqlFunction> listFunctions(Optional<Session> session);

    List<SqlFunction> listFunctionsWithoutFilterOut(Optional<Session> session);

    default LongSupplier getTableLastModifiedTimeSupplier(Session session, TableHandle tableHandle)
    {
        return null;
    }

    FunctionAndTypeManager getFunctionAndTypeManager();

    ProcedureRegistry getProcedureRegistry();

    //
    // Properties
    //

    SessionPropertyManager getSessionPropertyManager();

    SchemaPropertyManager getSchemaPropertyManager();

    TablePropertyManager getTablePropertyManager();

    ColumnPropertyManager getColumnPropertyManager();

    AnalyzePropertyManager getAnalyzePropertyManager();

    /**
     * Hetu can only cache execution plans for supported connectors.
     * This method checks if the property for supporting execution plan caching is enabled for a given connector.
     *
     * @param session Presto session
     * @param handle Connector specific table handle
     */
    boolean isExecutionPlanCacheSupported(Session session, TableHandle handle);

    /**
     * Hetu can only create index for supported connectors.
     *
     * @param session Presto session
     * @param tableName Connector specific tableName
     */
    boolean isHeuristicIndexSupported(Session session, QualifiedObjectName tableName);

    /**
     * Whether this table can be used as input for snapshot-enabled query executions
     *
     * @param session Presto session
     * @param table Connector specific table handle
     */
    boolean isSnapshotSupportedAsInput(Session session, TableHandle table);

    /**
     * Whether this table can be used as output for snapshot-enabled query executions
     *
     * @param session Presto session
     * @param table Connector specific table handle
     */
    boolean isSnapshotSupportedAsOutput(Session session, TableHandle table);

    /**
     * Whether new table with specified format can be used as output for snapshot-enabled
     *
     * @param session Presto session
     * @param catalogName Catalog name
     * @param tableProperties Table properties
     */
    boolean isSnapshotSupportedAsNewTable(Session session, CatalogName catalogName, Map<String, Object> tableProperties);

    /**
     * Snapshot: Remove any previous changes from previous execution attempt, to prepare for query resume
     */
    void resetInsertForRerun(Session session, InsertTableHandle tableHandle, OptionalLong snapshotIndex);

    /**
     * Snapshot: Remove any previous changes from previous execution attempt, to prepare for query resume
     */
    void resetCreateForRerun(Session session, OutputTableHandle tableHandle, OptionalLong snapshotIndex);

    /**
     * Cube pre-aggregation is applicable only for supported connectors.
     *
     * @param session Hetu session
     * @param catalogName catalog name
     * @return true, if underlying connector supports pre-aggregation
     * false, otherwise
     */
    boolean isPreAggregationSupported(Session session, CatalogName catalogName);

    PartialAndFinalAggregationType validateAndGetSortAggregationType(Session session, TableHandle tableHandle, List<String> keyNames);

    void refreshMetadataCache(Session session, Optional<String> catalogName);
}
