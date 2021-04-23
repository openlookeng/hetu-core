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
package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.connector.ViewNotFoundException;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;
import io.prestosql.spi.metastore.model.TableEntityType;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.TypeManager;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

@ThreadSafe
public class MemoryMetadata
        implements ConnectorMetadata
{
    public static final String MEM_KEY = "memory";
    public static final String DEFAULT_SCHEMA = "default";
    public static final String ID_KEY = "id";
    public static final String NEXT_ID_KEY = "_NEXT_ID_";
    private static final JsonCodec<ConnectorViewDefinition> VIEW_CODEC = jsonCodec(ConnectorViewDefinition.class);

    private final NodeManager nodeManager;
    private final TypeManager typeManager;
    private final AtomicLong nextTableId;
    private final Map<Long, TableInfo> tables = new ConcurrentHashMap<>();
    private final Map<SchemaTableName, ConnectorViewDefinition> views = new ConcurrentHashMap<>();
    private final HetuMetastore metastore;

    @Inject
    public MemoryMetadata(TypeManager typeManager, NodeManager nodeManager, HetuMetastore metastore)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.metastore = metastore;
        Optional<CatalogEntity> oldCatalog = metastore.getCatalog(MEM_KEY);
        if (!oldCatalog.isPresent()) {
            CatalogEntity newCatalog = CatalogEntity.builder()
                    .setCatalogName(MEM_KEY)
                    .setComment(Optional.of("Hetu memory connector"))
                    .build();
            metastore.createCatalogIfNotExist(newCatalog);
            this.nextTableId = new AtomicLong();
        }
        else {
            this.nextTableId = new AtomicLong(Long.parseLong(oldCatalog.get().getParameters().getOrDefault(NEXT_ID_KEY, "0")));
        }
        DatabaseEntity.Builder databaseBuilder = DatabaseEntity.builder()
                .setCatalogName(MEM_KEY)
                .setDatabaseName(DEFAULT_SCHEMA);
        metastore.createDatabaseIfNotExist(databaseBuilder.build());
    }

    @Override
    public synchronized List<String> listSchemaNames(ConnectorSession session)
    {
        ImmutableList.Builder<String> schemaNames = ImmutableList.builder();
        metastore.getAllDatabases(MEM_KEY).forEach(databaseEntity -> schemaNames.add(databaseEntity.getName()));
        return schemaNames.build();
    }

    @Override
    public synchronized void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        checkSchemaNotExists(schemaName);

        DatabaseEntity.Builder databaseBuilder = DatabaseEntity.builder()
                .setCatalogName(MEM_KEY)
                .setDatabaseName(schemaName)
                .setCreateTime(session.getStartTime())
                .setOwner(session.getUser());
        metastore.createDatabaseIfNotExist(databaseBuilder.build());
    }

    @Override
    public synchronized void dropSchema(ConnectorSession session, String schemaName)
    {
        checkSchemaExists(schemaName);

        if (!metastore.getAllTables(MEM_KEY, schemaName).isEmpty()) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }

        metastore.dropDatabase(MEM_KEY, schemaName);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Optional<TableEntity> tableEntity = metastore.getTable(MEM_KEY, schemaTableName.getSchemaName(), schemaTableName.getTableName());
        if (!tableEntity.isPresent()) {
            return null;
        }

        String idStr = tableEntity.get().getParameters().get(ID_KEY);
        if (idStr == null) {
            return null;
        }

        return new MemoryTableHandle(Long.parseLong(idStr));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        return getTableInfo((handle).getId()).getMetadata(typeManager);
    }

    @Override
    public synchronized List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return getTableStream(schemaName, false)
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        return getTableInfo(handle.getId())
                .getColumns().stream()
                .collect(toMap(ColumnInfo::getName, ColumnInfo::getHandle));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        return getTableInfo(handle.getId())
                .getColumn(columnHandle)
                .getMetadata(typeManager);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return getMemoryCatalogEntity().getParameters().values().stream()
                .map(TableInfo::deserialize)
                .filter(table -> prefix.matches(table.getSchemaTableName()))
                .collect(toMap(TableInfo::getSchemaTableName, handle -> handle.getMetadata(typeManager).getColumns()));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        TableInfo info = getTableInfo(handle.getId());
        metastore.dropTable(MEM_KEY, info.getSchemaName(), info.getTableName());
        updateTableInfo(handle.getId(), null);
    }

    // CONTINUE HERE

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        checkSchemaExists(newTableName.getSchemaName());
        checkTableNotExists(newTableName, false);

        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        long tableId = handle.getId();

        TableInfo oldInfo = getTableInfo(tableId);
        updateTableInfo(tableId, new TableInfo(
                tableId,
                newTableName.getSchemaName(),
                newTableName.getTableName(),
                oldInfo.getColumns(),
                oldInfo.getDataFragments()));

        metastore.alterTable(MEM_KEY, oldInfo.getSchemaName(), oldInfo.getTableName(),
                TableEntity.builder()
                        .setCatalogName(MEM_KEY)
                        .setDatabaseName(newTableName.getSchemaName())
                        .setTableName(newTableName.getTableName())
                        .setTableType(TableEntityType.TABLE.toString())
                        .setParameter(ID_KEY, String.valueOf(tableId))
                        .build());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
        finishCreateTable(session, outputTableHandle, ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public MemoryOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        checkSchemaExists(tableMetadata.getTable().getSchemaName());
        checkTableNotExists(tableMetadata.getTable(), false);

        List<SortingColumn> sortedBy = MemoryTableProperties.getSortedBy(tableMetadata.getProperties());
        if (sortedBy == null) {
            sortedBy = Collections.emptyList();
        }

        if (sortedBy.size() > 1) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "sort_by property currently only supports one column");
        }

        Set<String> sortedByColumnNames = new HashSet<>();
        for (SortingColumn s : sortedBy) {
            if (!sortedByColumnNames.add(s.getColumnName())) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "duplicate column(s) in sort_by property");
            }
        }

        List<String> indexColumns = MemoryTableProperties.getIndexedColumns(tableMetadata.getProperties());
        if (indexColumns == null) {
            indexColumns = Collections.emptyList();
        }

        Set<String> indexColumnNames = new HashSet<>();
        for (String c : indexColumns) {
            if (!indexColumnNames.add(c)) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "duplicate column(s) in index_columns property");
            }

            if (sortedByColumnNames.contains(c)) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "duplicate column(s) in sort_by and index_columns, sort_by columns are automatically indexed");
            }
        }

        ImmutableList.Builder<ColumnInfo> columns = ImmutableList.builder();
        Set<String> columnNames = new HashSet<>();
        for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
            ColumnMetadata column = tableMetadata.getColumns().get(i);
            columns.add(new ColumnInfo(new MemoryColumnHandle(i, column.getType().getTypeSignature()), column.getName()));
            columnNames.add(column.getName());
        }

        sortedByColumnNames.removeAll(columnNames);
        if (!sortedByColumnNames.isEmpty()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "invalid column(s) in sort_by");
        }

        indexColumnNames.removeAll(columnNames);
        if (!indexColumnNames.isEmpty()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "invalid column(s) in index_columns");
        }

        long nextId = nextTableId.getAndIncrement();
        metastore.alterCatalogParameter(MEM_KEY, NEXT_ID_KEY, String.valueOf(nextTableId.get()));
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No Memory nodes available");

        long tableId = nextId;

        List<ColumnInfo> columnInfos = columns.build();
        metastore.createTable(TableEntity.builder()
                .setCatalogName(MEM_KEY)
                .setDatabaseName(tableMetadata.getTable().getSchemaName())
                .setTableName(tableMetadata.getTable().getTableName())
                .setTableType(TableEntityType.TABLE.toString())
                .setParameter(ID_KEY, String.valueOf(tableId))
                .build());
        updateTableInfo(tableId, new TableInfo(
                tableId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columnInfos,
                new HashMap<>()));

        return new MemoryOutputTableHandle(tableId, getTableIdSet(), columnInfos, sortedBy, indexColumns);
    }

    private void checkSchemaNotExists(String schemaName)
    {
        if (metastore.getDatabase(MEM_KEY, schemaName).isPresent()) {
            throw new PrestoException(ALREADY_EXISTS, format("Schema already exists", schemaName));
        }
    }

    private DatabaseEntity checkSchemaExists(String schemaName)
    {
        Optional<DatabaseEntity> databaseEntity = metastore.getDatabase(MEM_KEY, schemaName);
        if (!databaseEntity.isPresent()) {
            throw new SchemaNotFoundException(schemaName);
        }
        return databaseEntity.get();
    }

    private void checkTableNotExists(SchemaTableName tableName, boolean isView)
    {
        if (metastore.getTable(MEM_KEY, tableName.getSchemaName(), tableName.getTableName()).isPresent()) {
            throw new PrestoException(ALREADY_EXISTS, format("%s [%s] already exists", isView ? "View" : "Table", tableName));
        }
    }

    private TableEntity checkTableExists(SchemaTableName tableName, boolean isView)
    {
        Optional<TableEntity> tableEntity = metastore.getTable(MEM_KEY, tableName.getSchemaName(), tableName.getTableName());
        if (!tableEntity.isPresent()) {
            throw isView ? new ViewNotFoundException(tableName) : new TableNotFoundException(tableName);
        }
        return tableEntity.get();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        MemoryOutputTableHandle memoryOutputHandle = (MemoryOutputTableHandle) tableHandle;

        updateRowsOnHosts(memoryOutputHandle.getTable(), fragments);
        return Optional.empty();
    }

    @Override
    public MemoryInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) tableHandle;
        return new MemoryInsertTableHandle(memoryTableHandle.getId(), getTableIdSet());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(insertHandle, "insertHandle is null");
        MemoryInsertTableHandle memoryInsertHandle = (MemoryInsertTableHandle) insertHandle;

        updateRowsOnHosts(memoryInsertHandle.getTable(), fragments);
        return Optional.empty();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        checkSchemaExists(viewName.getSchemaName());

        if (!replace) {
            checkTableNotExists(viewName, true);
        }
        updateViewDef(viewName, definition);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        checkTableExists(viewName, true);
        updateViewDef(viewName, null);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return getTableStream(schemaName, true)
                .collect(Collectors.toList());
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new).orElseGet(SchemaTablePrefix::new);
        return getTableStream(schemaName, true)
                .filter(prefix::matches)
                .collect(Collectors.toMap(
                        name -> name,
                        this::getViewDef));
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.ofNullable(getViewDef(viewName));
    }

    private void updateRowsOnHosts(long tableId, Collection<Slice> fragments)
    {
        TableInfo info = getTableInfo(tableId);
        checkState(
                info != null,
                "Uninitialized tableId [%s.%s]",
                info.getSchemaName(),
                info.getTableName());

        Map<HostAddress, MemoryDataFragment> dataFragments = new HashMap<>(info.getDataFragments());
        for (Slice fragment : fragments) {
            MemoryDataFragment memoryDataFragment = MemoryDataFragment.fromSlice(fragment);
            dataFragments.merge(memoryDataFragment.getHostAddress(), memoryDataFragment, MemoryDataFragment::merge);
        }

        updateTableInfo(tableId, new TableInfo(tableId, info.getSchemaName(), info.getTableName(), info.getColumns(), dataFragments));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    public List<MemoryDataFragment> getDataFragments(long tableId)
    {
        return ImmutableList.copyOf(getTableInfo(tableId).getDataFragments().values());
    }

    // TODO: disabled for now
//    @Override
//    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
//    {
//        MemoryTableHandle table = (MemoryTableHandle) handle;
//
//        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
//            return Optional.empty();
//        }
//
//        return Optional.of(new LimitApplicationResult<>(
//                new MemoryTableHandle(table.getId(), OptionalLong.of(limit), OptionalDouble.empty(), table.getPredicate()),
//                true));
//    }

    // TODO: disabled for now
//    @Override
//    public Optional<ConnectorTableHandle> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio)
//    {
//        MemoryTableHandle table = (MemoryTableHandle) handle;
//
//        if ((table.getSampleRatio().isPresent() && table.getSampleRatio().getAsDouble() == sampleRatio) || sampleType != SYSTEM || table.getLimit().isPresent()) {
//            return Optional.empty();
//        }
//
//        return Optional.of(new MemoryTableHandle(table.getId(), table.getLimit(), OptionalDouble.of(table.getSampleRatio().orElse(1) * sampleRatio), table.getPredicate()));
//    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        if (constraint.getSummary().isAll()) {
            return Optional.empty();
        }

        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) handle;

        MemoryTableHandle newMemoryTableHandle = new MemoryTableHandle(
                memoryTableHandle.getId(),
                memoryTableHandle.getLimit(),
                memoryTableHandle.getSampleRatio(),
                constraint.getSummary());

        return Optional.of(new ConstraintApplicationResult<>(newMemoryTableHandle, constraint.getSummary()));
    }

    /**
     * Get all tables in the given schema (if present) or all tables in memory catalog.
     */
    private Stream<SchemaTableName> getTableStream(Optional<String> schemaName, boolean isView)
    {
        Stream<TableEntity> tables = schemaName.isPresent() ?
                metastore.getAllTables(MEM_KEY, schemaName.get()).stream() :
                metastore.getAllDatabases(MEM_KEY).stream()
                        .flatMap(databaseEntity -> metastore.getAllTables(MEM_KEY, databaseEntity.getName()).stream());
        return tables
                .filter(tableEntity -> isView(tableEntity) == isView)
                .map(tableEntity -> new SchemaTableName(tableEntity.getDatabaseName(), tableEntity.getName()));
    }

    private TableInfo getTableInfo(Long tableId)
    {
        // cache tableInfo in the map to avoid deserializing it on every visit
        TableInfo tableInfo = tables.get(tableId);
        if (tableInfo == null) {
            tableInfo = TableInfo.deserialize(getMemoryCatalogEntity().getParameters().get(tableId.toString()));
            tables.put(tableId, tableInfo);
        }
        return tableInfo;
    }

    /**
     * Update catalog parameter map. remove entry if {@code null} tableInfo passed in.
     */
    private void updateTableInfo(Long tableId, TableInfo newTableInfo)
    {
        if (newTableInfo == null) {
            metastore.alterCatalogParameter(MEM_KEY, String.valueOf(tableId), null);
        }
        else {
            metastore.alterCatalogParameter(MEM_KEY, String.valueOf(tableId), newTableInfo.serialize());
        }
        tables.remove(tableId);
    }

    private ConnectorViewDefinition getViewDef(SchemaTableName viewName)
    {
        // cache view def in the map to avoid deserializing it on every visit
        ConnectorViewDefinition view = views.get(viewName);
        if (view == null) {
            try {
                TableEntity tableEntity = checkTableExists(viewName, true);
                if (isView(tableEntity)) {
                    view = VIEW_CODEC.fromJson(Base64.getDecoder().decode(tableEntity.getViewOriginalText()));
                    views.put(viewName, view);
                }
            }
            catch (Exception e) {
                views.remove(viewName);
            }
        }
        return view;
    }

    private void updateViewDef(SchemaTableName viewName, ConnectorViewDefinition newViewDef)
    {
        views.remove(viewName);
        if (newViewDef == null) {
            metastore.dropTable(MEM_KEY, viewName.getSchemaName(), viewName.getTableName());
        }
        else {
            String encoded = Base64.getEncoder().encodeToString(VIEW_CODEC.toJsonBytes(newViewDef));
            Optional<TableEntity> tableEntity = metastore.getTable(MEM_KEY, viewName.getSchemaName(), viewName.getTableName());
            if (!tableEntity.isPresent()) {
                metastore.createTable(
                        TableEntity.builder()
                                .setCatalogName(MEM_KEY)
                                .setDatabaseName(viewName.getSchemaName())
                                .setTableName(viewName.getTableName())
                                .setViewOriginalText(Optional.ofNullable(encoded))
                                .setTableType(TableEntityType.TABLE.toString())
                                .build());
            }
            else {
                TableEntity newTableEntity = tableEntity.get();
                newTableEntity.setViewOriginalText(encoded);
                metastore.alterTable(MEM_KEY, viewName.getSchemaName(), viewName.getTableName(), newTableEntity);
            }
        }
    }

    private CatalogEntity getMemoryCatalogEntity()
    {
        Optional<CatalogEntity> catalogEntity = metastore.getCatalog(MEM_KEY);
        if (!catalogEntity.isPresent()) {
            throw new IllegalStateException("Metastore catalog " + MEM_KEY + " does not exist");
        }
        return catalogEntity.get();
    }

    private boolean isView(TableEntity tableEntity)
    {
        return tableEntity.getViewOriginalText() != null;
    }

    private Set<Long> getTableIdSet()
    {
        return getMemoryCatalogEntity().getParameters().keySet().stream().filter(e -> !NEXT_ID_KEY.equals(e)).map(Long::valueOf).collect(Collectors.toSet());
    }
}
