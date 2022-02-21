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

import com.esotericsoftware.kryo.Kryo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.connector.DataCenterConnectorManager;
import io.prestosql.spi.PartialAndFinalAggregationType;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorCapabilities;
import io.prestosql.spi.connector.ConnectorDeleteAsInsertTableHandle;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorResolvedIndex;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutResult;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.ConnectorUpdateTableHandle;
import io.prestosql.spi.connector.ConnectorVacuumTableHandle;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.connector.SampleType;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.function.OperatorType;
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
import io.prestosql.spi.type.TypeNotFoundException;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.planner.PartitioningHandle;
import io.prestosql.transaction.TransactionManager;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.metadata.MetadataUtil.convertFromSchemaTableName;
import static io.prestosql.metadata.MetadataUtil.toSchemaTableName;
import static io.prestosql.spi.StandardErrorCode.INVALID_VIEW;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.prestosql.spi.connector.ConnectorViewDefinition.ViewColumn;
import static io.prestosql.spi.function.OperatorType.BETWEEN;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class MetadataManager
        implements Metadata
{
    private final FunctionAndTypeManager functionAndTypeManager;
    private final ProcedureRegistry procedures;
    private final SessionPropertyManager sessionPropertyManager;
    private final SchemaPropertyManager schemaPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final ColumnPropertyManager columnPropertyManager;
    private final AnalyzePropertyManager analyzePropertyManager;
    private final TransactionManager transactionManager;

    private final ConcurrentMap<QueryId, QueryCatalogs> catalogsByQueryId = new ConcurrentHashMap<>();

    // This section of code declares the dataCenterConnectorManager object
    private final Provider<DataCenterConnectorManager> dataCenterConnectorManager;
    private Map<String, ConcurrentHashMap<ConnectorTableHandle, TableProperties>> tablePropertiesQueryCache = new ConcurrentHashMap<>();

    @Inject
    public MetadataManager(
            FunctionAndTypeManager functionAndTypeManager,
            FeaturesConfig featuresConfig,
            SessionPropertyManager sessionPropertyManager,
            SchemaPropertyManager schemaPropertyManager,
            TablePropertyManager tablePropertyManager,
            ColumnPropertyManager columnPropertyManager,
            AnalyzePropertyManager analyzePropertyManager,
            TransactionManager transactionManager,
            Provider<DataCenterConnectorManager> dataCenterConnectorManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.procedures = new ProcedureRegistry(this);
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.schemaPropertyManager = requireNonNull(schemaPropertyManager, "schemaPropertyManager is null");
        this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
        this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
        this.analyzePropertyManager = requireNonNull(analyzePropertyManager, "analyzePropertyManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.dataCenterConnectorManager = dataCenterConnectorManager;

        verifyComparableOrderableContract();
    }

    // this method returns the dataCenterConnectorManager object
    public DataCenterConnectorManager getDataCenterConnectorManager()
    {
        if (dataCenterConnectorManager != null) {
            return dataCenterConnectorManager.get();
        }
        else {
            return null;
        }
    }

    public static MetadataManager createTestMetadataManager()
    {
        return createTestMetadataManager(new FeaturesConfig());
    }

    public static MetadataManager createTestMetadataManager(FeaturesConfig featuresConfig)
    {
        return createTestMetadataManager(new CatalogManager(), featuresConfig);
    }

    public static MetadataManager createTestMetadataManager(CatalogManager catalogManager)
    {
        return createTestMetadataManager(catalogManager, new FeaturesConfig());
    }

    public static MetadataManager createTestMetadataManager(CatalogManager catalogManager, FeaturesConfig featuresConfig)
    {
        return createTestMetadataManager(createTestTransactionManager(catalogManager), featuresConfig);
    }

    public static MetadataManager createTestMetadataManager(TransactionManager transactionManager, FeaturesConfig featuresConfig)
    {
        return new MetadataManager(
                new FunctionAndTypeManager(transactionManager, featuresConfig, new HandleResolver(), ImmutableSet.of(), new Kryo()),
                featuresConfig,
                new SessionPropertyManager(),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                new ColumnPropertyManager(),
                new AnalyzePropertyManager(),
                transactionManager,
                null);
    }

    @Override
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogName catalogName)
    {
        return getCatalogMetadata(session, catalogName).getConnectorCapabilities();
    }

    @Override
    public boolean catalogExists(Session session, String catalogName)
    {
        if (getDataCenterConnectorManager() != null) {
            getDataCenterConnectorManager().loadAllDCCatalogs();
        }
        return getOptionalCatalogMetadata(session, catalogName).isPresent();
    }

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, schema.getCatalogName());
        if (!catalog.isPresent()) {
            return false;
        }
        CatalogMetadata catalogMetadata = catalog.get();
        ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogName());
        return catalogMetadata.listConnectorIds().stream()
                .map(catalogMetadata::getMetadataFor)
                .anyMatch(metadata -> metadata.schemaExists(connectorSession, schema.getSchemaName()));
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, catalogName);

        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogName());
            for (CatalogName connectorId : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
                metadata.listSchemaNames(connectorSession).stream()
                        .map(schema -> schema.toLowerCase(Locale.ENGLISH))
                        .forEach(schemaNames::add);
            }
        }
        return ImmutableList.copyOf(schemaNames.build());
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName table)
    {
        requireNonNull(table, "table is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, table.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogName catalogName = catalogMetadata.getConnectorId(session, table);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

            ConnectorSession connectorSession = session.toConnectorSession(catalogName);
            ConnectorTableHandle tableHandle = metadata.getTableHandle(connectorSession, toSchemaTableName(table));
            if (tableHandle != null) {
                return Optional.of(new TableHandle(
                        catalogName,
                        tableHandle,
                        catalogMetadata.getTransactionHandleFor(catalogName),
                        Optional.empty()));
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<TableHandle> getTableHandleForStatisticsCollection(Session session, QualifiedObjectName table, Map<String, Object> analyzeProperties)
    {
        requireNonNull(table, "table is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, table.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogName catalogName = catalogMetadata.getConnectorId(session, table);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

            ConnectorTableHandle tableHandle = metadata.getTableHandleForStatisticsCollection(session.toConnectorSession(catalogName), toSchemaTableName(table), analyzeProperties);
            if (tableHandle != null) {
                return Optional.of(new TableHandle(
                        catalogName,
                        tableHandle,
                        catalogMetadata.getTransactionHandleFor(catalogName),
                        Optional.empty()));
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "table is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, tableName.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            // we query only main connector for runtime system tables
            CatalogName catalogName = catalogMetadata.getCatalogName();
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

            return metadata.getSystemTable(session.toConnectorSession(catalogName), toSchemaTableName(tableName));
        }
        return Optional.empty();
    }

    @Override
    public Optional<TableLayoutResult> getLayout(Session session, TableHandle table, Constraint constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        if (constraint.getSummary().isNone()) {
            return Optional.empty();
        }

        CatalogName catalogName = table.getCatalogName();
        ConnectorTableHandle connectorTable = table.getConnectorHandle();

        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

        checkState(metadata.usesLegacyTableLayouts(), "getLayout() was called even though connector doesn't support legacy Table Layout");

        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        List<ConnectorTableLayoutResult> layouts = metadata.getTableLayouts(connectorSession, connectorTable, constraint, desiredColumns);
        if (layouts.isEmpty()) {
            return Optional.empty();
        }

        if (layouts.size() > 1) {
            throw new PrestoException(NOT_SUPPORTED, format("Connector returned multiple layouts for table %s", table));
        }

        ConnectorTableLayout tableLayout = layouts.get(0).getTableLayout();
        return Optional.of(new TableLayoutResult(
                new TableHandle(catalogName, connectorTable, transaction, Optional.of(tableLayout.getHandle())),
                new TableProperties(catalogName, transaction, new ConnectorTableProperties(tableLayout)),
                layouts.get(0).getUnenforcedConstraint()));
    }

    @Override
    public TableProperties getTableProperties(Session session, TableHandle handle)
    {
        CatalogName catalogName = handle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        TableProperties tableProperties;
        ConcurrentHashMap<ConnectorTableHandle, TableProperties> tablePropertiesMap;
        String queryId = session.getQueryId().getId();

        if (metadata.usesLegacyTableLayouts()) {
            return handle.getLayout()
                    .map(layout -> new TableProperties(catalogName, handle.getTransaction(), new ConnectorTableProperties(metadata.getTableLayout(connectorSession, layout))))
                    .orElseGet(() -> getLayout(session, handle, Constraint.alwaysTrue(), Optional.empty())
                            .get()
                            .getTableProperties());
        }

        if (!handle.getConnectorHandle().isTablePropertiesCacheSupported()) {
            return new TableProperties(catalogName, handle.getTransaction(), metadata.getTableProperties(connectorSession, handle.getConnectorHandle()));
        }

        if (tablePropertiesQueryCache.get(queryId) != null && tablePropertiesQueryCache.get(queryId).get(handle.getConnectorHandle()) != null) {
            return tablePropertiesQueryCache.get(queryId).get(handle.getConnectorHandle());
        }
        else {
            tableProperties = new TableProperties(catalogName, handle.getTransaction(), metadata.getTableProperties(connectorSession, handle.getConnectorHandle()));
            if (tablePropertiesQueryCache.containsKey(queryId)) {
                tablePropertiesMap = tablePropertiesQueryCache.get(queryId);
            }
            else {
                tablePropertiesMap = new ConcurrentHashMap<>();
            }

            tablePropertiesMap.put(handle.getConnectorHandle(), tableProperties);
            tablePropertiesQueryCache.put(queryId, tablePropertiesMap);
        }

        return tableProperties;
    }

    @Override
    public TableHandle makeCompatiblePartitioning(Session session, TableHandle tableHandle, PartitioningHandle partitioningHandle)
    {
        checkArgument(partitioningHandle.getConnectorId().isPresent(), "Expect partitioning handle from connector, got system partitioning handle");
        CatalogName catalogName = partitioningHandle.getConnectorId().get();
        checkArgument(catalogName.equals(tableHandle.getCatalogName()), "ConnectorId of tableHandle and partitioningHandle does not match");
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(catalogName);
        if (metadata.usesLegacyTableLayouts()) {
            ConnectorTableLayoutHandle newTableLayoutHandle = metadata.makeCompatiblePartitioning(session.toConnectorSession(catalogName), tableHandle.getLayout().get(), partitioningHandle.getConnectorHandle());
            return new TableHandle(catalogName, tableHandle.getConnectorHandle(), transaction, Optional.of(newTableLayoutHandle));
        }
        verify(!tableHandle.getLayout().isPresent(), "layout should not be present");
        ConnectorTableHandle newTableHandle = metadata.makeCompatiblePartitioning(
                session.toConnectorSession(catalogName),
                tableHandle.getConnectorHandle(),
                partitioningHandle.getConnectorHandle());
        return new TableHandle(catalogName, newTableHandle, transaction, Optional.empty());
    }

    @Override
    public Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        Optional<CatalogName> leftConnectorId = left.getConnectorId();
        Optional<CatalogName> rightConnectorId = right.getConnectorId();
        if (!leftConnectorId.isPresent() || !rightConnectorId.isPresent() || !leftConnectorId.equals(rightConnectorId)) {
            return Optional.empty();
        }
        if (!left.getTransactionHandle().equals(right.getTransactionHandle())) {
            return Optional.empty();
        }
        CatalogName catalogName = leftConnectorId.get();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
        Optional<ConnectorPartitioningHandle> commonHandle = metadata.getCommonPartitioningHandle(session.toConnectorSession(catalogName), left.getConnectorHandle(), right.getConnectorHandle());
        return commonHandle.map(handle -> new PartitioningHandle(Optional.of(catalogName), left.getTransactionHandle(), handle));
    }

    @Override
    public Optional<Object> getInfo(Session session, TableHandle handle)
    {
        CatalogName catalogName = handle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (usesLegacyTableLayouts(session, handle)) {
            ConnectorTableLayoutHandle layoutHandle = handle.getLayout()
                    .orElseGet(() -> getLayout(session, handle, Constraint.alwaysTrue(), Optional.empty())
                            .get()
                            .getNewTableHandle()
                            .getLayout()
                            .get());

            return metadata.getInfo(layoutHandle);
        }

        return metadata.getInfo(handle.getConnectorHandle());
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
        if (tableMetadata.getColumns().isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Table has no columns: " + tableHandle);
        }

        return new TableMetadata(catalogName, tableMetadata);
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, Constraint constraint, boolean includeColumnStatistics)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.getTableStatistics(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), constraint, includeColumnStatistics);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        Map<String, ColumnHandle> handles = metadata.getColumnHandles(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());

        ImmutableMap.Builder<String, ColumnHandle> map = ImmutableMap.builder();
        for (Entry<String, ColumnHandle> mapEntry : handles.entrySet()) {
            map.put(mapEntry.getKey().toLowerCase(ENGLISH), mapEntry.getValue());
        }
        return map.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columnHandle, "columnHandle is null");

        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.getColumnMetadata(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), columnHandle);
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());
        Set<QualifiedObjectName> tables = new LinkedHashSet<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);
                metadata.listTables(connectorSession, prefix.getSchemaName()).stream()
                        .map(convertFromSchemaTableName(prefix.getCatalogName()))
                        .filter(prefix::matches)
                        .forEach(tables::add);
            }
        }
        return ImmutableList.copyOf(tables);
    }

    @Override
    public Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());
        Map<QualifiedObjectName, List<ColumnMetadata>> tableColumns = new HashMap<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();
            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

                ConnectorSession connectorSession = session.toConnectorSession(catalogName);
                for (Entry<SchemaTableName, List<ColumnMetadata>> entry : metadata.listTableColumns(connectorSession, tablePrefix).entrySet()) {
                    QualifiedObjectName tableName = new QualifiedObjectName(
                            prefix.getCatalogName(),
                            entry.getKey().getSchemaName(),
                            entry.getKey().getTableName());
                    tableColumns.put(tableName, entry.getValue());
                }

                // if table and view names overlap, the view wins
                for (Entry<QualifiedObjectName, ConnectorViewDefinition> entry : getViews(session, prefix).entrySet()) {
                    ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
                    for (ViewColumn column : entry.getValue().getColumns()) {
                        try {
                            columns.add(new ColumnMetadata(column.getName(), getType(column.getType())));
                        }
                        catch (TypeNotFoundException e) {
                            throw new PrestoException(INVALID_VIEW, format("Unknown type '%s' for column '%s' in view: %s", column.getType(), column.getName(), entry.getKey()));
                        }
                    }
                    tableColumns.put(entry.getKey(), columns.build());
                }
            }
        }
        return ImmutableMap.copyOf(tableColumns);
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schema.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.createSchema(session.toConnectorSession(catalogName), schema.getSchemaName(), properties);
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schema.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.dropSchema(session.toConnectorSession(catalogName), schema.getSchemaName());
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, source.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.renameSchema(session.toConnectorSession(catalogName), source.getSchemaName(), target);
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogName catalog = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.createTable(session.toConnectorSession(catalog), tableMetadata, ignoreExisting);
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
    {
        String catalogName = newTableName.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogName catalog = catalogMetadata.getCatalogName();
        if (!tableHandle.getCatalogName().equals(catalog)) {
            throw new PrestoException(SYNTAX_ERROR, "Cannot rename tables across catalogs");
        }

        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.renameTable(session.toConnectorSession(catalog), tableHandle.getConnectorHandle(), toSchemaTableName(newTableName));
    }

    @Override
    public void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.setTableComment(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), comment);
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.renameColumn(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), source, target.toLowerCase(ENGLISH));
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.addColumn(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), column);
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.dropColumn(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), column);
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.dropTable(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
    }

    @Override
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        return metadata.getInsertLayout(session.toConnectorSession(catalogName), table.getConnectorHandle())
                .map(layout -> new NewTableLayout(catalogName, catalogMetadata.getTransactionHandleFor(catalogName), layout));
    }

    @Override
    public Optional<NewTableLayout> getUpdateLayout(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        return metadata.getUpdateLayout(session.toConnectorSession(catalogName), table.getConnectorHandle())
                .map(layout -> new NewTableLayout(catalogName, catalogMetadata.getTransactionHandleFor(catalogName), layout));
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        CatalogName catalog = catalogMetadata.getCatalogName();
        return metadata.getStatisticsCollectionMetadataForWrite(session.toConnectorSession(catalog), tableMetadata);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        CatalogName catalog = catalogMetadata.getCatalogName();
        return metadata.getStatisticsCollectionMetadata(session.toConnectorSession(catalog), tableMetadata);
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorTableHandle connectorTableHandle = metadata.beginStatisticsCollection(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
        return new AnalyzeTableHandle(catalogName, transactionHandle, connectorTableHandle);
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        catalogMetadata.getMetadata().finishStatisticsCollection(session.toConnectorSession(), tableHandle.getConnectorHandle(), computedStatistics);
    }

    @Override
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogName catalog = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalog);
        ConnectorSession connectorSession = session.toConnectorSession(catalog);
        return metadata.getNewTableLayout(connectorSession, tableMetadata)
                .map(layout -> new NewTableLayout(catalog, transactionHandle, layout));
    }

    @Override
    public void cleanupQuery(Session session)
    {
        tablePropertiesQueryCache.remove(session.getQueryId().getId());
        QueryCatalogs queryCatalogs = catalogsByQueryId.remove(session.getQueryId());
        if (queryCatalogs != null) {
            queryCatalogs.finish();
        }
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogName catalog = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalog);
        ConnectorSession connectorSession = session.toConnectorSession(catalog);
        ConnectorOutputTableHandle handle = metadata.beginCreateTable(connectorSession, tableMetadata, layout.map(NewTableLayout::getLayout));
        return new OutputTableHandle(catalog, transactionHandle, handle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.finishCreateTable(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), fragments, computedStatistics);
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle, boolean isOverwrite)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorInsertTableHandle handle;
        if (isOverwrite) {
            handle = metadata.beginInsert(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), true);
        }
        else {
            handle = metadata.beginInsert(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
        }
        return new InsertTableHandle(tableHandle.getCatalogName(), transactionHandle, handle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.finishInsert(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), fragments, computedStatistics);
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.getDeleteRowIdColumnHandle(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.getUpdateRowIdColumnHandle(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), updatedColumns);
    }

    @Override
    public UpdateTableHandle beginUpdateAsInsert(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorUpdateTableHandle handle = metadata.beginUpdateAsInsert(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
        return new UpdateTableHandle(tableHandle.getCatalogName(), transactionHandle, handle);
    }

    @Override
    public DeletesAsInsertTableHandle beginDeletAsInsert(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorDeleteAsInsertTableHandle handle = metadata.beginDeletesAsInsert(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
        return new DeletesAsInsertTableHandle(tableHandle.getCatalogName(), transactionHandle, handle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishUpdateAsInsert(Session session, UpdateTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.finishUpdateAsInsert(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), fragments, computedStatistics);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishDeleteAsInsert(Session session, DeletesAsInsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.finishDeleteAsInsert(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), fragments, computedStatistics);
    }

    @Override
    public VacuumTableHandle beginVacuum(Session session, TableHandle tableHandle, boolean full, boolean merge, Optional<String> partition)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorVacuumTableHandle vacuumTableHandle = metadata.beginVacuum(session.toConnectorSession(catalogName),
                tableHandle.getConnectorHandle(), full, merge, partition);
        return new VacuumTableHandle(tableHandle.getCatalogName(), transactionHandle, vacuumTableHandle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishVacuum(Session session, VacuumTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.finishVacuum(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), fragments, computedStatistics);
    }

    @Override
    public boolean supportsMetadataDelete(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.supportsMetadataDelete(
                session.toConnectorSession(catalogName),
                tableHandle.getConnectorHandle(),
                tableHandle.getLayout().orElse(null));
    }

    @Override
    public Optional<TableHandle> applyDelete(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyDelete(connectorSession, table.getConnectorHandle())
                .map(newHandle -> new TableHandle(catalogName, newHandle, table.getTransaction(), Optional.empty()));
    }

    @Override
    public Optional<TableHandle> applyDelete(Session session, TableHandle table, Constraint constraint)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyDelete(connectorSession, table.getConnectorHandle(), constraint)
                .map(newHandle -> new TableHandle(catalogName, newHandle, table.getTransaction(), Optional.empty()));
    }

    @Override
    public OptionalLong executeDelete(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            checkArgument(table.getLayout().isPresent(), "table layout is missing");
            return metadata.metadataDelete(session.toConnectorSession(catalogName), table.getConnectorHandle(), table.getLayout().get());
        }
        checkArgument(!table.getLayout().isPresent(), "table layout should not be present");

        return metadata.executeDelete(connectorSession, table.getConnectorHandle());
    }

    @Override
    public OptionalLong executeUpdate(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.executeUpdate(connectorSession, table.getConnectorHandle());
    }

    @Override
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        ConnectorTableHandle newHandle = metadata.beginDelete(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
        return new TableHandle(tableHandle.getCatalogName(), newHandle, tableHandle.getTransaction(), tableHandle.getLayout());
    }

    @Override
    public void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        metadata.finishDelete(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public TableHandle beginUpdate(Session session, TableHandle tableHandle, List<Type> updatedColumnTypes)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        ConnectorTableHandle newHandle = metadata.beginUpdate(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), updatedColumnTypes);
        return new TableHandle(tableHandle.getCatalogName(), newHandle, tableHandle.getTransaction(), tableHandle.getLayout());
    }

    @Override
    public void finishUpdate(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        metadata.finishUpdate(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public Optional<CatalogName> getCatalogHandle(Session session, String catalogName)
    {
        return transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), catalogName).map(CatalogMetadata::getCatalogName);
    }

    @Override
    public Map<String, CatalogName> getCatalogNames(Session session)
    {
        return transactionManager.getCatalogNames(session.getRequiredTransactionId());
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        Set<QualifiedObjectName> views = new LinkedHashSet<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);
                metadata.listViews(connectorSession, prefix.getSchemaName()).stream()
                        .map(convertFromSchemaTableName(prefix.getCatalogName()))
                        .filter(prefix::matches)
                        .forEach(views::add);
            }
        }
        return ImmutableList.copyOf(views);
    }

    @Override
    public Map<QualifiedObjectName, ConnectorViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        Map<QualifiedObjectName, ConnectorViewDefinition> views = new LinkedHashMap<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();
            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);

                Map<SchemaTableName, ConnectorViewDefinition> viewMap;
                if (tablePrefix.getTable().isPresent()) {
                    viewMap = metadata.getView(connectorSession, tablePrefix.toSchemaTableName())
                            .map(view -> ImmutableMap.of(tablePrefix.toSchemaTableName(), view))
                            .orElse(ImmutableMap.of());
                }
                else {
                    viewMap = metadata.getViews(connectorSession, tablePrefix.getSchema());
                }

                for (Entry<SchemaTableName, ConnectorViewDefinition> entry : viewMap.entrySet()) {
                    QualifiedObjectName viewName = new QualifiedObjectName(
                            prefix.getCatalogName(),
                            entry.getKey().getSchemaName(),
                            entry.getKey().getTableName());
                    views.put(viewName, entry.getValue());
                }
            }
        }
        return ImmutableMap.copyOf(views);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, viewName.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogName catalogName = catalogMetadata.getConnectorId(session, viewName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

            ConnectorSession connectorSession = session.toConnectorSession(catalogName);
            return metadata.getView(connectorSession, toSchemaTableName(viewName));
        }
        return Optional.empty();
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.createView(session.toConnectorSession(catalogName), toSchemaTableName(viewName), definition, replace);
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.dropView(session.toConnectorSession(catalogName), toSchemaTableName(viewName));
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        Optional<ConnectorResolvedIndex> resolvedIndex = metadata.resolveIndex(connectorSession, tableHandle.getConnectorHandle(), indexableColumns, outputColumns, tupleDomain);
        return resolvedIndex.map(resolved -> new ResolvedIndex(tableHandle.getCatalogName(), transaction, resolved));
    }

    @Override
    public boolean usesLegacyTableLayouts(Session session, TableHandle table)
    {
        return getMetadata(session, table.getCatalogName()).usesLegacyTableLayouts();
    }

    /**
     * Hetu can only cache execution plans for supported connectors.
     * This method checks if the property for supporting execution plan caching is enabled for a given connector.
     *
     * @param session Presto session
     * @param table table handle containing a ConnectorTableHandle
     */
    @Override
    public boolean isExecutionPlanCacheSupported(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        return metadata.isExecutionPlanCacheSupported(session.toConnectorSession(), table.getConnectorHandle());
    }

    /**
     * Hetu can only create index for supported connectors.
     *
     * @param session Presto session
     * @param tableName Connector specific tableName
     */
    @Override
    public boolean isHeuristicIndexSupported(Session session, QualifiedObjectName tableName)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, tableName.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogName catalogName = catalogMetadata.getConnectorId(session, tableName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

            return metadata.isHeuristicIndexSupported();
        }

        return false;
    }

    @Override
    public boolean isPreAggregationSupported(Session session, CatalogName catalogName)
    {
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.isPreAggregationSupported(session.toConnectorSession());
    }

    /**
     * Whether this table can be used as input for snapshot-enabled query executions
     *
     * @param session Presto session
     * @param table Connector specific table handle
     */
    @Override
    public boolean isSnapshotSupportedAsInput(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        return metadata.isSnapshotSupportedAsInput(session.toConnectorSession(catalogName), table.getConnectorHandle());
    }

    /**
     * Whether this table can be used as output for snapshot-enabled query executions
     *
     * @param session Presto session
     * @param table Connector specific table handle
     */
    @Override
    public boolean isSnapshotSupportedAsOutput(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        return metadata.isSnapshotSupportedAsOutput(session.toConnectorSession(catalogName), table.getConnectorHandle());
    }

    /**
     * Whether new table with specified format can be used as output for snapshot-enabled
     *
     * @param session Presto session
     * @param catalogName Catalog name
     * @param tableProperties Table properties
     */
    @Override
    public boolean isSnapshotSupportedAsNewTable(Session session, CatalogName catalogName, Map<String, Object> tableProperties)
    {
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.isSnapshotSupportedAsNewTable(session.toConnectorSession(catalogName), tableProperties);
    }

    /**
     * Snapshot: Remove any previous changes from previous execution attempt, to prepare for query resume
     */
    @Override
    public void resetInsertForRerun(Session session, InsertTableHandle tableHandle, OptionalLong snapshotIndex)
    {
        ConnectorMetadata metadata = getMetadata(session, tableHandle.getCatalogName());
        metadata.resetInsertForRerun(session.toConnectorSession(tableHandle.getCatalogName()), tableHandle.getConnectorHandle(), snapshotIndex);
    }

    /**
     * Snapshot: Remove any previous changes from previous execution attempt, to prepare for query resume
     */
    @Override
    public void resetCreateForRerun(Session session, OutputTableHandle tableHandle, OptionalLong snapshotIndex)
    {
        ConnectorMetadata metadata = getMetadata(session, tableHandle.getCatalogName());
        metadata.resetCreateForRerun(session.toConnectorSession(tableHandle.getCatalogName()), tableHandle.getConnectorHandle(), snapshotIndex);
    }

    @Override
    public Optional<LimitApplicationResult<TableHandle>> applyLimit(Session session, TableHandle table, long limit)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyLimit(connectorSession, table.getConnectorHandle(), limit)
                .map(result -> new LimitApplicationResult<>(
                        new TableHandle(catalogName, result.getHandle(), table.getTransaction(), Optional.empty()),
                        result.isLimitGuaranteed()));
    }

    @Override
    public Optional<TableHandle> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applySample(connectorSession, table.getConnectorHandle(), sampleType, sampleRatio)
                .map(result -> new TableHandle(
                        catalogName,
                        result,
                        table.getTransaction(),
                        Optional.empty()));
    }

    @Override
    public Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyFilter(connectorSession, table.getConnectorHandle(), constraint)
                .map(result -> new ConstraintApplicationResult<>(
                        new TableHandle(catalogName, result.getHandle(), table.getTransaction(), Optional.empty()),
                        result.getRemainingFilter()));
    }

    @Override
    public Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint,
            List<Constraint> disjunctConstraints,
            Set<ColumnHandle> allColumnHandles,
            boolean pushPartitionsOnly)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyFilter(connectorSession, table.getConnectorHandle(), constraint, disjunctConstraints, allColumnHandles, pushPartitionsOnly)
                .map(result -> new ConstraintApplicationResult<>(
                        new TableHandle(catalogName, result.getHandle(), table.getTransaction(), Optional.empty()),
                        result.getRemainingFilter()));
    }

    public Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyProjection(connectorSession, table.getConnectorHandle(), projections, assignments)
                .map(result -> new ProjectionApplicationResult<>(
                        new TableHandle(catalogName, result.getHandle(), table.getTransaction(), Optional.empty()),
                        result.getProjections(),
                        result.getAssignments()));
    }

    //
    // Roles and Grants
    //

    @Override
    public void createRole(Session session, String role, Optional<PrestoPrincipal> grantor, String catalog)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog);
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.createRole(session.toConnectorSession(catalogName), role, grantor);
    }

    @Override
    public void dropRole(Session session, String role, String catalog)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog);
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.dropRole(session.toConnectorSession(catalogName), role);
    }

    @Override
    public Set<String> listRoles(Session session, String catalog)
    {
        Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog);
        if (!catalogMetadata.isPresent()) {
            return ImmutableSet.of();
        }
        CatalogName catalogName = catalogMetadata.get().getCatalogName();
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(catalogName);
        return metadata.listRoles(connectorSession).stream()
                .collect(toImmutableSet());
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, String catalog, PrestoPrincipal principal)
    {
        Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog);
        if (!catalogMetadata.isPresent()) {
            return ImmutableSet.of();
        }
        CatalogName catalogName = catalogMetadata.get().getCatalogName();
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(catalogName);
        return metadata.listRoleGrants(connectorSession, principal);
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalog)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog);
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.grantRoles(session.toConnectorSession(catalogName), roles, grantees, withAdminOption, grantor);
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalog)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog);
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.revokeRoles(session.toConnectorSession(catalogName), roles, grantees, adminOptionFor, grantor);
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, PrestoPrincipal principal, String catalog)
    {
        Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog);
        if (!catalogMetadata.isPresent()) {
            return ImmutableSet.of();
        }
        CatalogName catalogName = catalogMetadata.get().getCatalogName();
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(catalogName);
        return ImmutableSet.copyOf(metadata.listApplicableRoles(connectorSession, principal));
    }

    @Override
    public Set<String> listEnabledRoles(Session session, String catalog)
    {
        Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog);
        if (!catalogMetadata.isPresent()) {
            return ImmutableSet.of();
        }
        CatalogName catalogName = catalogMetadata.get().getCatalogName();
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(catalogName);
        return ImmutableSet.copyOf(metadata.listEnabledRoles(connectorSession));
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.grantTablePrivileges(session.toConnectorSession(catalogName), toSchemaTableName(tableName), privileges, grantee, grantOption);
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.revokeTablePrivileges(session.toConnectorSession(catalogName), toSchemaTableName(tableName), privileges, grantee, grantOption);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        ImmutableSet.Builder<GrantInfo> grantInfos = ImmutableSet.builder();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogName());

            List<CatalogName> connectorIds = prefix.asQualifiedObjectName()
                    .map(qualifiedTableName -> singletonList(catalogMetadata.getConnectorId(session, qualifiedTableName)))
                    .orElseGet(catalogMetadata::listConnectorIds);
            for (CatalogName catalogName : connectorIds) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
                grantInfos.addAll(metadata.listTablePrivileges(connectorSession, prefix.asSchemaTablePrefix()));
            }
        }
        return ImmutableList.copyOf(grantInfos.build());
    }

    //
    // Types
    //

    @Override
    public Type getType(TypeSignature signature)
    {
        return functionAndTypeManager.getType(signature);
    }

    @Override
    public void verifyComparableOrderableContract()
    {
        Multimap<Type, OperatorType> missingOperators = HashMultimap.create();
        for (Type type : functionAndTypeManager.getTypes()) {
            if (type.isComparable()) {
                if (!canResolveOperator(HASH_CODE, fromTypes(type))) {
                    missingOperators.put(type, HASH_CODE);
                }
                if (!canResolveOperator(EQUAL, fromTypes(type, type))) {
                    missingOperators.put(type, EQUAL);
                }
                if (!canResolveOperator(NOT_EQUAL, fromTypes(type, type))) {
                    missingOperators.put(type, NOT_EQUAL);
                }
            }
            if (type.isOrderable()) {
                for (OperatorType operator : ImmutableList.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL)) {
                    if (!canResolveOperator(operator, fromTypes(type, type))) {
                        missingOperators.put(type, operator);
                    }
                }
                if (!canResolveOperator(BETWEEN, fromTypes(type, type, type))) {
                    missingOperators.put(type, BETWEEN);
                }
            }
        }
        // TODO: verify the parametric types too
        if (!missingOperators.isEmpty()) {
            List<String> messages = new ArrayList<>();
            for (Type type : missingOperators.keySet()) {
                messages.add(format("%s missing for %s", missingOperators.get(type), type));
            }
            throw new IllegalStateException(Joiner.on(", ").join(messages));
        }
    }

    private boolean canResolveOperator(OperatorType operatorType, List<TypeSignatureProvider> argumentTypes)
    {
        try {
            getFunctionAndTypeManager().resolveOperatorFunctionHandle(operatorType, argumentTypes);
            return true;
        }
        catch (OperatorNotFoundException e) {
            return false;
        }
    }

    //
    // Functions
    //
    @Override
    public LongSupplier getTableLastModifiedTimeSupplier(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = getTableMetadata(session, tableHandle).getCatalogName();
        ConnectorMetadata connectorMetadata = getMetadata(session, catalogName);
        long modificationTime = connectorMetadata.getTableModificationTime(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
        return () -> modificationTime;
    }

    public List<SqlFunction> listFunctions(Optional<Session> session)
    {
        // TODO: transactional when FunctionManager is made transactional
        return functionAndTypeManager.listFunctions(session);
    }

    @Override
    public List<SqlFunction> listFunctionsWithoutFilterOut(Optional<Session> session)
    {
        // TODO: transactional when FunctionManager is made transactional
        return functionAndTypeManager.listFunctionsWithoutFilterOut(session);
    }

    @Override
    public FunctionAndTypeManager getFunctionAndTypeManager()
    {
        // TODO: transactional when FunctionManager is made transactional
        return functionAndTypeManager;
    }

    @Override
    public ProcedureRegistry getProcedureRegistry()
    {
        return procedures;
    }

    //
    // Properties
    //

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        return sessionPropertyManager;
    }

    @Override
    public SchemaPropertyManager getSchemaPropertyManager()
    {
        return schemaPropertyManager;
    }

    @Override
    public TablePropertyManager getTablePropertyManager()
    {
        return tablePropertyManager;
    }

    @Override
    public ColumnPropertyManager getColumnPropertyManager()
    {
        return columnPropertyManager;
    }

    @Override
    public AnalyzePropertyManager getAnalyzePropertyManager()
    {
        return analyzePropertyManager;
    }

    @Override
    public PartialAndFinalAggregationType validateAndGetSortAggregationType(Session session, TableHandle tableHandle, List<String> keyNames)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.validateAndGetSortAggregationType(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), keyNames);
    }

    @Override
    public void refreshMetadataCache(Session session, Optional<String> catalogName)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, catalogName.get());
        if (catalog.isPresent()) {
            ConnectorMetadata metadata = getMetadata(session, catalog.get().getCatalogName());
            metadata.refreshMetadataCache();
        }
        else {
            throw new PrestoException(NOT_FOUND, "Catalog does not exist:" + catalogName.get());
        }
    }

    //
    // Helpers
    //

    private Optional<CatalogMetadata> getOptionalCatalogMetadata(Session session, String catalogName)
    {
        Optional<CatalogMetadata> optionalCatalogMetadata = transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), catalogName);
        optionalCatalogMetadata.ifPresent(catalogMetadata -> registerCatalogForQuery(session, catalogMetadata));
        return optionalCatalogMetadata;
    }

    private CatalogMetadata getCatalogMetadata(Session session, CatalogName catalogName)
    {
        CatalogMetadata catalogMetadata = transactionManager.getCatalogMetadata(session.getRequiredTransactionId(), catalogName);
        registerCatalogForQuery(session, catalogMetadata);
        return catalogMetadata;
    }

    private CatalogMetadata getCatalogMetadataForWrite(Session session, String catalogName)
    {
        CatalogMetadata catalogMetadata = transactionManager.getCatalogMetadataForWrite(session.getRequiredTransactionId(), catalogName);
        registerCatalogForQuery(session, catalogMetadata);
        return catalogMetadata;
    }

    private CatalogMetadata getCatalogMetadataForWrite(Session session, CatalogName catalogName)
    {
        CatalogMetadata catalogMetadata = transactionManager.getCatalogMetadataForWrite(session.getRequiredTransactionId(), catalogName);
        registerCatalogForQuery(session, catalogMetadata);
        return catalogMetadata;
    }

    private ConnectorMetadata getMetadata(Session session, CatalogName catalogName)
    {
        return getCatalogMetadata(session, catalogName).getMetadataFor(catalogName);
    }

    private ConnectorMetadata getMetadataForWrite(Session session, CatalogName catalogName)
    {
        return getCatalogMetadataForWrite(session, catalogName).getMetadata();
    }

    private void registerCatalogForQuery(Session session, CatalogMetadata catalogMetadata)
    {
        catalogsByQueryId.computeIfAbsent(session.getQueryId(), queryId -> new QueryCatalogs(session))
                .registerCatalog(catalogMetadata);
    }

    @VisibleForTesting
    public Set<QueryId> getActiveQueryIds()
    {
        return ImmutableSet.copyOf(catalogsByQueryId.keySet());
    }

    private static class QueryCatalogs
    {
        private final Session session;
        @GuardedBy("this")
        private final Map<CatalogName, CatalogMetadata> catalogs = new HashMap<>();
        @GuardedBy("this")
        private boolean finished;

        public QueryCatalogs(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        private synchronized void registerCatalog(CatalogMetadata catalogMetadata)
        {
            checkState(!finished, "Query is already finished");
            if (catalogs.putIfAbsent(catalogMetadata.getCatalogName(), catalogMetadata) == null) {
                ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogName());
                catalogMetadata.getMetadata().beginQuery(connectorSession);
            }
        }

        private synchronized void finish()
        {
            List<CatalogMetadata> catalogMetadataList;
            synchronized (this) {
                checkState(!finished, "Query is already finished");
                finished = true;
                catalogMetadataList = new ArrayList<>(this.catalogs.values());
            }

            for (CatalogMetadata catalogMetadata : catalogMetadataList) {
                ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogName());
                catalogMetadata.getMetadata().cleanupQuery(connectorSession);
            }
        }
    }
}
