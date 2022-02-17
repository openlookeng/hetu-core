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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.plugin.memory.data.MemoryTableManager;
import io.prestosql.plugin.memory.statistics.StatisticsUtils;
import io.prestosql.plugin.memory.statistics.TableStatisticsData;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
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
import io.prestosql.spi.statistics.ColumnStatisticMetadata;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.TableStatisticType;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.statistics.TableStatisticsMetadata;
import io.prestosql.spi.type.TypeManager;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Arrays;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.prestosql.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

@ThreadSafe
public class MemoryMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(MemoryMetadata.class);
    public static final String MEM_KEY = "memory"; // memory metadata all under this catalog
    public static final String DEFAULT_SCHEMA = "default";
    public static final String TABLE_ID_KEY = "id"; // used as param key in TableEntity
    public static final String TABLE_STATS_KEY = "table_statistics";
    public static final String TABLE_OUTPUT_HANDLE = "output_handle"; // used as param key in TableEntity
    public static final String NEXT_ID_KEY = "_NEXT_ID_"; // used as param key in CatalogEntity
    private static final JsonCodec<ConnectorViewDefinition> VIEW_CODEC = jsonCodec(ConnectorViewDefinition.class);
    private static final JsonCodec<MemoryWriteTableHandle> OUTPUT_TABLE_HANDLE_JSON_CODEC = jsonCodec(MemoryWriteTableHandle.class);
    private static final JsonCodec<TableStatisticsData> STATS_JSON_CODEC = jsonCodec(TableStatisticsData.class);
    private static final long metastoreCheckInterval = 10L; // time between scheduled metastore check to remove spilled data

    private final NodeManager nodeManager;
    private final TypeManager typeManager;
    private final AtomicLong nextTableId;
    private final HetuMetastore metastore;
    private final MemoryConfig config;
    private final MemoryTableManager tableManager;

    // tables and views are cached here
    private final Map<Long, TableInfo> tables = new ConcurrentHashMap<>();
    private final Map<SchemaTableName, ConnectorViewDefinition> views = new ConcurrentHashMap<>();
    private Map<Long, TableStatistics> tableStats = new ConcurrentHashMap<>();

    private boolean refreshScheduled; // used to make sure that refresh is only scheduled once

    @Inject
    public MemoryMetadata(TypeManager typeManager, NodeManager nodeManager, HetuMetastore metastore, MemoryTableManager tableManager, MemoryConfig memoryConfig)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.config = requireNonNull(memoryConfig, "memoryConfig is null");
        this.metastore = metastore;
        this.tableManager = tableManager;
        this.refreshScheduled = false;
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

    synchronized void scheduleRefreshJob()
    {
        if (!this.refreshScheduled && MemoryThreadManager.isSharedThreadPoolInitilized()) {
            MemoryThreadManager.getSharedThreadPool().scheduleAtFixedRate(() -> {
                if (tableManager != null) {
                    nextTableId.set(Long.parseLong(getMemoryCatalogEntity().getParameters().getOrDefault(NEXT_ID_KEY, nextTableId.toString())));
                    tableManager.refreshTables(getTableIdSet(nextTableId.get()));
                }
            }, 1L, metastoreCheckInterval, TimeUnit.SECONDS);
            this.refreshScheduled = true;
        }
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
        checkSchemaExists(schemaName, false);

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
        checkSchemaExists(schemaName, true);

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

        String idStr = tableEntity.get().getParameters().get(TABLE_ID_KEY);
        if (idStr == null) {
            return null;
        }

        return new MemoryTableHandle(Long.parseLong(idStr), schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!config.isTableStatisticsEnabled()) {
            return TableStatisticsMetadata.empty();
        }
        return getStatisticsCollectionMetadata(session, tableMetadata);
    }

    @Override
    public ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        MemoryTableHandle handle = (MemoryTableHandle) getTableHandle(session, tableName);
        return handle;
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Set<ColumnStatisticMetadata> columnStatistics = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .map(this::getColumnStatisticMetadata)
                .flatMap(List::stream)
                .collect(toImmutableSet());
        Set<TableStatisticType> tableStatistics = ImmutableSet.of(ROW_COUNT);
        return new TableStatisticsMetadata(columnStatistics, tableStatistics, Collections.emptyList());
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(ColumnMetadata columnMetadata)
    {
        return getColumnStatisticMetadata(columnMetadata.getName(), StatisticsUtils.getSupportedColumnStatistics(columnMetadata.getType()));
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(String columnName, Set<ColumnStatisticType> statisticTypes)
    {
        return statisticTypes.stream()
                .map(type -> new ColumnStatisticMetadata(columnName, type))
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return tableHandle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        long rowCount = 0;
        for (ComputedStatistics stat : computedStatistics) {
            Block value = stat.getTableStatistics().get(ROW_COUNT);
            if (value != null) {
                rowCount = BIGINT.getLong(value, 0);
                break;
            }
        }

        Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tableHandle);
        TableStatisticsData tableStatistics = StatisticsUtils.fromComputedStatistics(session, computedStatistics, rowCount, columnHandles, typeManager);
        String statsInfo = Base64.getEncoder().encodeToString(STATS_JSON_CODEC.toJsonBytes(tableStatistics));

        // store the stats info into metastore
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        metastore.alterTableParameter(MEM_KEY,
                handle.getSchemaName(),
                handle.getTableName(),
                TABLE_STATS_KEY,
                statsInfo);

        // also update the in-memory cache
        tableStats.put(handle.getId(), tableStatistics.toTableStatistics(columnHandles));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;

        TableInfo info = getTableInfo((handle).getId());

        SchemaTableName schema = new SchemaTableName(info.getSchemaName(), info.getTableName());

        List<ColumnMetadata> columns = info.getColumns().stream()
                .map(columnInfo -> columnInfo.getMetadata(typeManager))
                .collect(Collectors.toList());

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        String creationHandleStr = getTableEntity(info.getSchemaTableName(), false).getParameters().get(TABLE_OUTPUT_HANDLE);

        if (creationHandleStr == null) {
            throw new PrestoException(GENERIC_USER_ERROR, "Table is in an invalid state and should be dropped and recreated.");
        }

        MemoryWriteTableHandle tableWriteHandle = OUTPUT_TABLE_HANDLE_JSON_CODEC.fromJson(Base64.getDecoder().decode(creationHandleStr));
        if (tableWriteHandle.getSortedBy() != null && !tableWriteHandle.getSortedBy().isEmpty()) {
            properties.put(MemoryTableProperties.SORTED_BY_PROPERTY, tableWriteHandle.getSortedBy());
        }
        if (tableWriteHandle.getIndexColumns() != null && !tableWriteHandle.getIndexColumns().isEmpty()) {
            properties.put(MemoryTableProperties.INDEX_COLUMNS_PROPERTY, tableWriteHandle.getIndexColumns());
        }
        properties.put(MemoryTableProperties.SPILL_COMPRESSION_PROPERTY, tableWriteHandle.isCompressionEnabled());

        return new ConnectorTableMetadata(schema, columns, properties.build());
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
                .collect(toMap(MemoryColumnHandle::getColumnName, memoryColumnHandle -> memoryColumnHandle));
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
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint, boolean includeColumnStatistics)
    {
        if (!config.isTableStatisticsEnabled()) {
            return TableStatistics.empty();
        }

        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) tableHandle;
        TableStatistics stat = tableStats.get(memoryTableHandle.getId());
        if (stat == null) {
            TableInfo tableInfo = getTableInfo(memoryTableHandle.getId());
            String creationHandleStr = getTableEntity(tableInfo.getSchemaTableName(), false).getParameters().get(TABLE_STATS_KEY);
            if (creationHandleStr == null) {
                return TableStatistics.empty();
            }

            TableStatisticsData result = STATS_JSON_CODEC.fromJson(Base64.getDecoder().decode(creationHandleStr));
            Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tableHandle);
            stat = result.toTableStatistics(columnHandles);
            tableStats.put(memoryTableHandle.getId(), stat);
        }
        return stat;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return getMemoryCatalogEntity().getParameters().entrySet().stream()
                .filter(entry -> !entry.getKey().equals(NEXT_ID_KEY))
                .map(entry -> TableInfo.deserialize(entry.getValue()))
                .filter(table -> prefix.matches(table.getSchemaTableName()))
                .collect(toMap(TableInfo::getSchemaTableName, handle -> handle.getColumns(typeManager)));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        TableInfo info = getTableInfo(handle.getId());
        metastore.dropTable(MEM_KEY, info.getSchemaName(), info.getTableName());
        updateTableInfo(handle.getId(), null);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        checkSchemaExists(newTableName.getSchemaName(), true);
        checkTableNotExists(newTableName, false);

        MemoryTableHandle handle = (MemoryTableHandle) tableHandle;
        long tableId = handle.getId();

        TableInfo oldInfo = getTableInfo(tableId);
        updateTableInfo(tableId, new TableInfo(
                tableId,
                newTableName.getSchemaName(),
                newTableName.getTableName(),
                oldInfo.getColumns(),
                oldInfo.getDataFragments(),
                System.currentTimeMillis()));

        Optional<TableEntity> oldTableEntity = metastore.getTable(MEM_KEY, oldInfo.getSchemaName(), oldInfo.getTableName());
        String oldOutputTableHandleStr = oldTableEntity.get().getParameters().get(TABLE_OUTPUT_HANDLE);
        if (oldOutputTableHandleStr == null) {
            throw new PrestoException(GENERIC_USER_ERROR, "Table is in an invalid state and should be dropped and recreated.");
        }

        MemoryWriteTableHandle oldTableHandle = OUTPUT_TABLE_HANDLE_JSON_CODEC.fromJson(Base64.getDecoder().decode(oldOutputTableHandleStr));
        MemoryWriteTableHandle newTableHandle = new MemoryWriteTableHandle(
                oldTableHandle.getTable(),
                newTableName.getSchemaName(),
                newTableName.getTableName(),
                oldTableHandle.isCompressionEnabled(),
                oldTableHandle.isAsyncProcessingEnabled(),
                oldTableHandle.getActiveTableIds(),
                oldTableHandle.getColumns(),
                oldTableHandle.getSortedBy(),
                oldTableHandle.getPartitionedBy(),
                oldTableHandle.getIndexColumns());

        metastore.alterTable(MEM_KEY, oldInfo.getSchemaName(), oldInfo.getTableName(),
                TableEntity.builder()
                        .setCatalogName(MEM_KEY)
                        .setDatabaseName(newTableName.getSchemaName())
                        .setTableName(newTableName.getTableName())
                        .setTableType(TableEntityType.TABLE.toString())
                        .setParameter(TABLE_ID_KEY, String.valueOf(tableId))
                        .setParameter(TABLE_OUTPUT_HANDLE, Base64.getEncoder().encodeToString(OUTPUT_TABLE_HANDLE_JSON_CODEC.toJsonBytes(newTableHandle)))
                        .build());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
        finishCreateTable(session, outputTableHandle, ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public synchronized MemoryWriteTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        checkSchemaExists(tableMetadata.getTable().getSchemaName(), true);
        checkTableNotExists(tableMetadata.getTable(), false);

        List<SortingColumn> sortedBy = MemoryTableProperties.getSortedBy(tableMetadata.getProperties());
        if (sortedBy == null) {
            sortedBy = Collections.emptyList();
        }

        if (sortedBy.size() > 1) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "sorted_by property currently only supports one column");
        }

        Set<String> sortedByColumnNames = new HashSet<>();
        for (SortingColumn s : sortedBy) {
            if (!sortedByColumnNames.add(s.getColumnName())) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "duplicate column(s) in sorted_by property");
            }
        }

        List<String> partitionBy = MemoryTableProperties.getPartitionedBy(tableMetadata.getProperties());
        if (partitionBy == null) {
            partitionBy = Collections.emptyList();
        }

        if (partitionBy.size() > 1) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "partition_by property currently only supports one column");
        }

        Set<String> partitionByColumnNames = new HashSet<>();
        for (String p : partitionBy) {
            if (!partitionByColumnNames.add(p)) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "duplicate column(s) in partition_by property");
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
                throw new PrestoException(INVALID_TABLE_PROPERTY, "duplicate column(s) in sorted_by and index_columns, sorted_by columns are automatically indexed");
            }
        }

        ImmutableList.Builder<MemoryColumnHandle> columns = ImmutableList.builder();
        Map<String, ColumnMetadata> columnNames = new HashMap<>();
        for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
            ColumnMetadata column = tableMetadata.getColumns().get(i);
            boolean isPartitionKey = partitionBy.contains(column.getName());
            columns.add(new MemoryColumnHandle(column.getName(), i, column.getType().getTypeSignature(), isPartitionKey));
            columnNames.put(column.getName(), column);
        }

        for (String sortedByColumnName : sortedByColumnNames) {
            if (!columnNames.containsKey(sortedByColumnName)) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "column " + sortedByColumnName + " in sorted_by does not exist");
            }
            if (!columnNames.get(sortedByColumnName).getType().isComparable()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "column " + sortedByColumnName + " in sorted_by is not comparable");
            }
        }

        for (String partitionByColumnName : partitionByColumnNames) {
            if (!columnNames.containsKey(partitionByColumnName)) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "column " + partitionByColumnName + " in partition_column does not exist");
            }
        }

        for (String indexColumnName : indexColumnNames) {
            if (!columnNames.containsKey(indexColumnName)) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "column " + indexColumnName + " in index_columns does not exist");
            }
        }

        long nextId = nextTableId.getAndIncrement();
        metastore.alterCatalogParameter(MEM_KEY, NEXT_ID_KEY, String.valueOf(nextTableId.get()));
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No Memory nodes available");

        long tableId = nextId;

        List<MemoryColumnHandle> columnHandles = columns.build();
        metastore.createTable(TableEntity.builder()
                .setCatalogName(MEM_KEY)
                .setDatabaseName(tableMetadata.getTable().getSchemaName())
                .setTableName(tableMetadata.getTable().getTableName())
                .setTableType(TableEntityType.TABLE.toString())
                .setParameter(TABLE_ID_KEY, String.valueOf(tableId))
                .build());
        updateTableInfo(tableId, new TableInfo(
                tableId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columnHandles,
                new HashMap<>(),
                System.currentTimeMillis()));

        boolean spillCompressionEnabled = MemoryTableProperties.getSpillCompressionEnabled(tableMetadata.getProperties());
        boolean asyncProcessingEnabled = MemoryTableProperties.getAsyncProcessingEnabled(tableMetadata.getProperties());

        return new MemoryWriteTableHandle(
                nextId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                spillCompressionEnabled,
                asyncProcessingEnabled,
                getTableIdSet(nextId),
                columnHandles,
                sortedBy,
                partitionBy,
                indexColumns);
    }

    private void checkSchemaExists(String schemaName, boolean expectExist)
    {
        Optional<DatabaseEntity> databaseEntity = metastore.getDatabase(MEM_KEY, schemaName);
        if (expectExist && !databaseEntity.isPresent()) {
            throw new SchemaNotFoundException(schemaName);
        }
        if (!expectExist && databaseEntity.isPresent()) {
            throw new PrestoException(ALREADY_EXISTS, format("Schema already exists %s", schemaName));
        }
    }

    private void checkTableNotExists(SchemaTableName tableName, boolean isView)
    {
        if (metastore.getTable(MEM_KEY, tableName.getSchemaName(), tableName.getTableName()).isPresent()) {
            throw new PrestoException(ALREADY_EXISTS, format("%s [%s] already exists", isView ? "View" : "Table", tableName));
        }
    }

    private TableEntity getTableEntity(SchemaTableName tableName, boolean isView)
    {
        Optional<TableEntity> tableEntity = metastore.getTable(MEM_KEY, tableName.getSchemaName(), tableName.getTableName());
        if (!tableEntity.isPresent()) {
            throw isView ? new ViewNotFoundException(tableName) : new TableNotFoundException(tableName);
        }
        return tableEntity.get();
    }

    @Override
    public boolean isPreAggregationSupported(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        MemoryWriteTableHandle memoryOutputHandle = (MemoryWriteTableHandle) tableHandle;

        metastore.alterTableParameter(MEM_KEY,
                memoryOutputHandle.getSchemaName(),
                memoryOutputHandle.getTableName(),
                TABLE_OUTPUT_HANDLE,
                Base64.getEncoder().encodeToString(OUTPUT_TABLE_HANDLE_JSON_CODEC.toJsonBytes(memoryOutputHandle)));

        updateRowsOnHosts(memoryOutputHandle.getTable(), fragments);
        return Optional.empty();
    }

    @Override
    public MemoryWriteTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) tableHandle;
        TableInfo tableInfo = getTableInfo(memoryTableHandle.getId());
        String creationHandleStr = getTableEntity(tableInfo.getSchemaTableName(), false).getParameters().get(TABLE_OUTPUT_HANDLE);

        if (creationHandleStr == null) {
            throw new PrestoException(GENERIC_USER_ERROR, "Table is in an invalid state and should be dropped and recreated.");
        }

        MemoryWriteTableHandle res = OUTPUT_TABLE_HANDLE_JSON_CODEC.fromJson(Base64.getDecoder().decode(creationHandleStr));
        res.getActiveTableIds().clear();
        res.getActiveTableIds().addAll(getTableIdSet(res.getTable()));
        return res;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(insertHandle, "insertHandle is null");
        MemoryWriteTableHandle memoryInsertHandle = (MemoryWriteTableHandle) insertHandle;

        updateRowsOnHosts(memoryInsertHandle.getTable(), fragments);

        // invalidate table stats in cache and metastore when new data is inserted
        tableStats.remove(memoryInsertHandle.getTable());
        metastore.alterTableParameter(MEM_KEY,
                memoryInsertHandle.getSchemaName(),
                memoryInsertHandle.getTableName(),
                TABLE_STATS_KEY,
                null);

        return Optional.empty();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        checkSchemaExists(viewName.getSchemaName(), true);

        if (!replace) {
            checkTableNotExists(viewName, true);
        }
        updateViewDef(viewName, definition);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        getTableEntity(viewName, true);
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

        updateTableInfo(tableId, new TableInfo(tableId, info.getSchemaName(), info.getTableName(), info.getColumns(), dataFragments, System.currentTimeMillis()));
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

    // TODO: disabled for now

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        if (constraint.getSummary().isAll()) {
            return Optional.empty();
        }
        MemoryTableHandle memoryTableHandle = (MemoryTableHandle) handle;

        TableInfo tableInfo = getTableInfo(memoryTableHandle.getId());

        MemoryTableHandle newMemoryTableHandle = new MemoryTableHandle(
                memoryTableHandle.getId(),
                tableInfo.getSchemaName(),
                tableInfo.getTableName(),
                memoryTableHandle.getLimit(),
                memoryTableHandle.getSampleRatio(),
                constraint.getSummary());

        return Optional.of(new ConstraintApplicationResult<>(newMemoryTableHandle, constraint.getSummary()));
    }

    @Override
    public long getTableModificationTime(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            return getTableInfo(((MemoryTableHandle) tableHandle).getId()).getModificationTime();
        }
        // We want to make sure the query doesn't fail because of star-tree not being able to get last modified time
        catch (Exception e) {
            log.error("Exception thrown while trying to get modified time", e);
            return -1L;
        }
    }

    /**
     * Get all tables in the given schema (if present) or all tables in memory catalog.
     */
    private Stream<SchemaTableName> getTableStream(Optional<String> schemaName, boolean isView)
    {
        Stream<TableEntity> tableStream = schemaName.isPresent() ?
                metastore.getAllTables(MEM_KEY, schemaName.get()).stream() :
                metastore.getAllDatabases(MEM_KEY).stream()
                        .flatMap(databaseEntity -> metastore.getAllTables(MEM_KEY, databaseEntity.getName()).stream());
        return tableStream
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
                TableEntity tableEntity = getTableEntity(viewName, true);
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

    private Set<Long> getTableIdSet(Long... ids)
    {
        Set<Long> res = new HashSet<>();
        getMemoryCatalogEntity().getParameters().keySet().stream().filter(e -> !NEXT_ID_KEY.equals(e)).map(Long::valueOf).forEach(res::add);
        res.addAll(Arrays.asList(ids));
        return res;
    }
}
