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
package io.prestosql.plugin.tpch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.tpch.Distributions;
import io.airlift.tpch.LineItemColumn;
import io.airlift.tpch.OrderColumn;
import io.airlift.tpch.OrderGenerator;
import io.airlift.tpch.PartColumn;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;
import io.prestosql.plugin.tpch.statistics.ColumnStatisticsData;
import io.prestosql.plugin.tpch.statistics.StatisticsEstimator;
import io.prestosql.plugin.tpch.statistics.TableStatisticsData;
import io.prestosql.plugin.tpch.statistics.TableStatisticsDataRepository;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTablePartitioning;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LocalProperty;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.SortingProperty;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.DoubleRange;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.statistics.TableStatisticsMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.asMap;
import static io.airlift.tpch.OrderColumn.ORDER_STATUS;
import static io.prestosql.plugin.tpch.util.PredicateUtils.convertToPredicate;
import static io.prestosql.plugin.tpch.util.PredicateUtils.filterOutColumnFromPredicate;
import static io.prestosql.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class TpchMetadata
        implements ConnectorMetadata
{
    public static final String TINY_SCHEMA_NAME = "tiny";
    public static final double TINY_SCALE_FACTOR = 0.01;

    public static final List<String> SCHEMA_NAMES = ImmutableList.of(
            TINY_SCHEMA_NAME, "sf1", "sf100", "sf300", "sf1000", "sf3000", "sf10000", "sf30000", "sf100000");

    public static final String ROW_NUMBER_COLUMN_NAME = "row_number";

    private static final Set<Slice> ORDER_STATUS_VALUES = ImmutableSet.of("F", "O", "P").stream()
            .map(Slices::utf8Slice)
            .collect(toImmutableSet());
    private static final Set<NullableValue> ORDER_STATUS_NULLABLE_VALUES = ORDER_STATUS_VALUES.stream()
            .map(value -> new NullableValue(getPrestoType(OrderColumn.ORDER_STATUS), value))
            .collect(toSet());

    private static final Set<Slice> PART_TYPE_VALUES = Distributions.getDefaultDistributions().getPartTypes().getValues().stream()
            .map(Slices::utf8Slice)
            .collect(toImmutableSet());
    private static final Set<NullableValue> PART_TYPE_NULLABLE_VALUES = PART_TYPE_VALUES.stream()
            .map(value -> new NullableValue(getPrestoType(PartColumn.TYPE), value))
            .collect(toSet());

    private static final Set<Slice> PART_CONTAINER_VALUES = Distributions.getDefaultDistributions().getPartContainers().getValues().stream()
            .map(Slices::utf8Slice)
            .collect(toImmutableSet());
    private static final Set<NullableValue> PART_CONTAINER_NULLABLE_VALUES = PART_CONTAINER_VALUES.stream()
            .map(value -> new NullableValue(getPrestoType(PartColumn.CONTAINER), value))
            .collect(toSet());

    private final Set<String> tableNames;
    private final ColumnNaming columnNaming;
    private final StatisticsEstimator statisticsEstimator;
    private final boolean predicatePushdownEnabled;
    private final boolean partitioningEnabled;

    public TpchMetadata()
    {
        this(ColumnNaming.SIMPLIFIED, true, true);
    }

    public TpchMetadata(ColumnNaming columnNaming, boolean predicatePushdownEnabled, boolean partitioningEnabled)
    {
        ImmutableSet.Builder<String> tableNamesBuilder = ImmutableSet.builder();
        for (TpchTable<?> tpchTable : TpchTable.getTables()) {
            tableNamesBuilder.add(tpchTable.getTableName());
        }
        this.tableNames = tableNamesBuilder.build();
        this.columnNaming = columnNaming;
        this.predicatePushdownEnabled = predicatePushdownEnabled;
        this.partitioningEnabled = partitioningEnabled;
        this.statisticsEstimator = createStatisticsEstimator();
    }

    private static StatisticsEstimator createStatisticsEstimator()
    {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new Jdk8Module());
        TableStatisticsDataRepository tableStatisticsDataRepository = new TableStatisticsDataRepository(objectMapper);
        return new StatisticsEstimator(tableStatisticsDataRepository);
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return schemaNameToScaleFactor(schemaName) > 0;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return SCHEMA_NAMES;
    }

    @Override
    public TpchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!tableNames.contains(tableName.getTableName())) {
            return null;
        }

        // parse the scale factor
        double scaleFactor = schemaNameToScaleFactor(tableName.getSchemaName());
        if (scaleFactor <= 0) {
            return null;
        }

        return new TpchTableHandle(tableName.getTableName(), scaleFactor);
    }

    @Override
    public ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        return getTableHandle(session, tableName);
    }

    private Set<NullableValue> filterValues(Set<NullableValue> nullableValues, TpchColumn<?> column, Constraint constraint)
    {
        return nullableValues.stream()
                .filter(convertToPredicate(constraint.getSummary(), toColumnHandle(column)))
                .filter(value -> !constraint.predicate().isPresent() || constraint.predicate().get().test(ImmutableMap.of(toColumnHandle(column), value)))
                .collect(toSet());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        TpchTableLayoutHandle layout = (TpchTableLayoutHandle) handle;

        // tables in this connector have a single layout
        return getTableLayouts(session, layout.getTable(), new Constraint(layout.getPredicate()), Optional.empty())
                .get(0)
                .getTableLayout();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TpchTableHandle tpchTableHandle = (TpchTableHandle) tableHandle;

        TpchTable<?> tpchTable = TpchTable.getTable(tpchTableHandle.getTableName());
        String schemaName = scaleFactorSchemaName(tpchTableHandle.getScaleFactor());

        return getTableMetadata(schemaName, tpchTable, columnNaming);
    }

    private static ConnectorTableMetadata getTableMetadata(String schemaName, TpchTable<?> tpchTable, ColumnNaming columnNaming)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (TpchColumn<? extends TpchEntity> column : tpchTable.getColumns()) {
            columns.add(new ColumnMetadata(columnNaming.getName(column), getPrestoType(column), false, null, null, false, emptyMap()));
        }
        columns.add(new ColumnMetadata(ROW_NUMBER_COLUMN_NAME, BIGINT, null, true));

        SchemaTableName tableName = new SchemaTableName(schemaName, tpchTable.getTableName());
        return new ConnectorTableMetadata(tableName, columns.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getTableMetadata(session, tableHandle).getColumns()) {
            builder.put(columnMetadata.getName(), new TpchColumnHandle(columnMetadata.getName(), columnMetadata.getType()));
        }
        return builder.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (String schemaName : getSchemaNames(session, prefix.getSchema())) {
            for (TpchTable<?> tpchTable : TpchTable.getTables()) {
                if (prefix.getTable().map(tpchTable.getTableName()::equals).orElse(true)) {
                    ConnectorTableMetadata tableMetadata = getTableMetadata(schemaName, tpchTable, columnNaming);
                    tableColumns.put(new SchemaTableName(schemaName, tpchTable.getTableName()), tableMetadata.getColumns());
                }
            }
        }
        return tableColumns.build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint, boolean includeColumnStatistics)
    {
        TpchTableHandle tpchTableHandle = (TpchTableHandle) tableHandle;
        String tableName = tpchTableHandle.getTableName();
        TpchTable<?> tpchTable = TpchTable.getTable(tableName);
        Map<TpchColumn<?>, List<Object>> columnValuesRestrictions = ImmutableMap.of();
        if (predicatePushdownEnabled) {
            columnValuesRestrictions = getColumnValuesRestrictions(tpchTable, constraint);
        }
        Optional<TableStatisticsData> optionalTableStatisticsData = statisticsEstimator.estimateStats(tpchTable, columnValuesRestrictions, tpchTableHandle.getScaleFactor());

        Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tpchTableHandle);
        return optionalTableStatisticsData
                .map(tableStatisticsData -> toTableStatistics(optionalTableStatisticsData.get(), tpchTableHandle, columnHandles))
                .orElse(TableStatistics.empty());
    }

    @Override
    public long getTableModificationTime(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return -1L;
    }

    private Map<TpchColumn<?>, List<Object>> getColumnValuesRestrictions(TpchTable<?> tpchTable, Constraint constraint)
    {
        TupleDomain<ColumnHandle> constraintSummary = constraint.getSummary();
        if (constraintSummary.isAll()) {
            return emptyMap();
        }
        else if (constraintSummary.isNone()) {
            Set<TpchColumn<?>> columns = ImmutableSet.copyOf(tpchTable.getColumns());
            return asMap(columns, key -> emptyList());
        }
        else {
            Map<ColumnHandle, Domain> domains = constraintSummary.getDomains().get();
            Optional<Domain> orderStatusDomain = Optional.ofNullable(domains.get(toColumnHandle(ORDER_STATUS)));
            Optional<Map<TpchColumn<?>, List<Object>>> allowedColumnValues = orderStatusDomain.map(domain -> {
                List<Object> allowedValues = ORDER_STATUS_VALUES.stream()
                        .filter(domain::includesNullableValue)
                        .collect(toList());
                return avoidTrivialOrderStatusRestriction(allowedValues);
            });
            return allowedColumnValues.orElse(emptyMap());
        }
    }

    private Map<TpchColumn<?>, List<Object>> avoidTrivialOrderStatusRestriction(List<Object> allowedValues)
    {
        if (allowedValues.containsAll(ORDER_STATUS_VALUES)) {
            return emptyMap();
        }
        else {
            return ImmutableMap.of(ORDER_STATUS, allowedValues);
        }
    }

    private TableStatistics toTableStatistics(TableStatisticsData tableStatisticsData, TpchTableHandle tpchTableHandle, Map<String, ColumnHandle> columnHandles)
    {
        TableStatistics.Builder builder = TableStatistics.builder()
                .setRowCount(Estimate.of(tableStatisticsData.getRowCount()));
        tableStatisticsData.getColumns().forEach((columnName, stats) -> {
            TpchColumnHandle columnHandle = (TpchColumnHandle) getColumnHandle(tpchTableHandle, columnHandles, columnName);
            builder.setColumnStatistics(columnHandle, toColumnStatistics(stats, columnHandle.getType()));
        });
        return builder.build();
    }

    private ColumnHandle getColumnHandle(TpchTableHandle tpchTableHandle, Map<String, ColumnHandle> columnHandles, String columnName)
    {
        TpchTable<?> table = TpchTable.getTable(tpchTableHandle.getTableName());
        return columnHandles.get(columnNaming.getName(table.getColumn(columnName)));
    }

    private ColumnStatistics toColumnStatistics(ColumnStatisticsData stats, Type columnType)
    {
        return ColumnStatistics.builder()
                .setNullsFraction(Estimate.zero())
                .setDistinctValuesCount(stats.getDistinctValuesCount().map(Estimate::of).orElse(Estimate.unknown()))
                .setDataSize(stats.getDataSize().map(Estimate::of).orElse(Estimate.unknown()))
                .setRange(toRange(stats.getMin(), stats.getMax(), columnType))
                .build();
    }

    private static Optional<DoubleRange> toRange(Optional<Object> min, Optional<Object> max, Type columnType)
    {
        if (columnType instanceof VarcharType) {
            return Optional.empty();
        }
        if (!min.isPresent() || !max.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(new DoubleRange(toDouble(min.get(), columnType), toDouble(max.get(), columnType)));
    }

    private static double toDouble(Object value, Type columnType)
    {
        if (value instanceof String && columnType.equals(DATE)) {
            return LocalDate.parse((CharSequence) value).toEpochDay();
        }
        if (columnType.equals(BIGINT) || columnType.equals(INTEGER) || columnType.equals(DATE)) {
            return ((Number) value).longValue();
        }
        if (columnType.equals(DOUBLE)) {
            return ((Number) value).doubleValue();
        }
        throw new IllegalArgumentException("unsupported column type " + columnType);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return new TableStatisticsMetadata(ImmutableSet.of(), ImmutableSet.of(ROW_COUNT), ImmutableList.of());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof TpchTableHandle);
        return tableHandle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        // do nothing
    }

    @VisibleForTesting
    TpchColumnHandle toColumnHandle(TpchColumn<?> column)
    {
        return new TpchColumnHandle(columnNaming.getName(column), getPrestoType(column));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        String columnName = columnHandle.getColumnName();

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        throw new IllegalArgumentException(format("Table %s does not have column %s", tableMetadata.getTable(), columnName));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> filterSchema)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : getSchemaNames(session, filterSchema)) {
            for (TpchTable<?> tpchTable : TpchTable.getTables()) {
                builder.add(new SchemaTableName(schemaName, tpchTable.getTableName()));
            }
        }
        return builder.build();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        TpchTableHandle tableHandle = (TpchTableHandle) table;

        Optional<ConnectorTablePartitioning> tablePartitioning = Optional.empty();
        Optional<Set<ColumnHandle>> partitioningColumns = Optional.empty();
        List<LocalProperty<ColumnHandle>> localProperties = ImmutableList.of();

        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);
        if (partitioningEnabled && tableHandle.getTableName().equals(TpchTable.ORDERS.getTableName())) {
            ColumnHandle orderKeyColumn = columns.get(columnNaming.getName(OrderColumn.ORDER_KEY));
            tablePartitioning = Optional.of(new ConnectorTablePartitioning(
                    new TpchPartitioningHandle(
                            TpchTable.ORDERS.getTableName(),
                            calculateTotalRows(OrderGenerator.SCALE_BASE, tableHandle.getScaleFactor())),
                    ImmutableList.of(orderKeyColumn)));
            partitioningColumns = Optional.of(ImmutableSet.of(orderKeyColumn));
            localProperties = ImmutableList.of(new SortingProperty<>(orderKeyColumn, SortOrder.ASC_NULLS_FIRST));
        }
        else if (partitioningEnabled && tableHandle.getTableName().equals(TpchTable.LINE_ITEM.getTableName())) {
            ColumnHandle orderKeyColumn = columns.get(columnNaming.getName(LineItemColumn.ORDER_KEY));
            tablePartitioning = Optional.of(new ConnectorTablePartitioning(
                    new TpchPartitioningHandle(
                            TpchTable.ORDERS.getTableName(),
                            calculateTotalRows(OrderGenerator.SCALE_BASE, tableHandle.getScaleFactor())),
                    ImmutableList.of(orderKeyColumn)));
            partitioningColumns = Optional.of(ImmutableSet.of(orderKeyColumn));
            localProperties = ImmutableList.of(
                    new SortingProperty<>(orderKeyColumn, SortOrder.ASC_NULLS_FIRST),
                    new SortingProperty<>(columns.get(columnNaming.getName(LineItemColumn.LINE_NUMBER)), SortOrder.ASC_NULLS_FIRST));
        }

        return new ConnectorTableProperties(
                tableHandle.getConstraint(),
                tablePartitioning,
                partitioningColumns,
                Optional.empty(),
                localProperties);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        TpchTableHandle handle = (TpchTableHandle) table;

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();

        TupleDomain<ColumnHandle> predicate = TupleDomain.all();
        TupleDomain<ColumnHandle> unenforcedConstraint = constraint.getSummary();
        if (predicatePushdownEnabled && handle.getTableName().equals(TpchTable.ORDERS.getTableName())) {
            predicate = toTupleDomain(ImmutableMap.of(
                    toColumnHandle(OrderColumn.ORDER_STATUS),
                    filterValues(ORDER_STATUS_NULLABLE_VALUES, OrderColumn.ORDER_STATUS, constraint)));
            unenforcedConstraint = filterOutColumnFromPredicate(constraint.getSummary(), toColumnHandle(OrderColumn.ORDER_STATUS));
        }
        else if (predicatePushdownEnabled && handle.getTableName().equals(TpchTable.PART.getTableName())) {
            predicate = toTupleDomain(ImmutableMap.of(
                    toColumnHandle(PartColumn.CONTAINER),
                    filterValues(PART_CONTAINER_NULLABLE_VALUES, PartColumn.CONTAINER, constraint),
                    toColumnHandle(PartColumn.TYPE),
                    filterValues(PART_TYPE_NULLABLE_VALUES, PartColumn.TYPE, constraint)));
            unenforcedConstraint = filterOutColumnFromPredicate(constraint.getSummary(), toColumnHandle(PartColumn.CONTAINER));
            unenforcedConstraint = filterOutColumnFromPredicate(unenforcedConstraint, toColumnHandle(PartColumn.TYPE));
        }

        if (oldDomain.equals(predicate)) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                new TpchTableHandle(
                        handle.getTableName(),
                        handle.getScaleFactor(),
                        oldDomain.intersect(predicate)),
                unenforcedConstraint));
    }

    private TupleDomain<ColumnHandle> toTupleDomain(Map<TpchColumnHandle, Set<NullableValue>> predicate)
    {
        return TupleDomain.withColumnDomains(predicate.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    Type type = entry.getKey().getType();
                    return entry.getValue().stream()
                            .map(nullableValue -> Domain.singleValue(type, nullableValue.getValue()))
                            .reduce((Domain::union))
                            .orElse(Domain.none(type));
                })));
    }

    private List<String> getSchemaNames(ConnectorSession session, Optional<String> schemaName)
    {
        if (!schemaName.isPresent()) {
            return listSchemaNames(session);
        }
        if (schemaNameToScaleFactor(schemaName.get()) > 0) {
            return ImmutableList.of(schemaName.get());
        }
        return ImmutableList.of();
    }

    private static String scaleFactorSchemaName(double scaleFactor)
    {
        return "sf" + scaleFactor;
    }

    public static double schemaNameToScaleFactor(String schemaName)
    {
        if (TINY_SCHEMA_NAME.equals(schemaName)) {
            return TINY_SCALE_FACTOR;
        }

        if (!schemaName.startsWith("sf")) {
            return -1;
        }

        try {
            return Double.parseDouble(schemaName.substring(2));
        }
        catch (Exception ignored) {
            return -1;
        }
    }

    public static Type getPrestoType(TpchColumn<?> column)
    {
        TpchColumnType tpchType = column.getType();
        switch (tpchType.getBase()) {
            case IDENTIFIER:
                return BIGINT;
            case INTEGER:
                return INTEGER;
            case DATE:
                return DATE;
            case DOUBLE:
                return DOUBLE;
            case VARCHAR:
                return createVarcharType((int) (long) tpchType.getPrecision().get());
        }
        throw new IllegalArgumentException("Unsupported type " + tpchType);
    }

    private long calculateTotalRows(int scaleBase, double scaleFactor)
    {
        double totalRows = scaleBase * scaleFactor;
        if (totalRows > Long.MAX_VALUE) {
            throw new IllegalArgumentException("Total rows is larger than 2^64");
        }
        return (long) totalRows;
    }

    /**
     * Presto can only cache execution plans for supported connectors.
     * This method overrides {@link ConnectorMetadata} returns true to indicate
     * execution plan caching is enabled for Tpch connectors.
     *
     * @param session Presto session
     * @param handle Connector specific table handle
     */
    @Override
    public boolean isExecutionPlanCacheSupported(ConnectorSession session, ConnectorTableHandle handle)
    {
        return true;
    }

    @Override
    public boolean isSnapshotSupportedAsInput(ConnectorSession session, ConnectorTableHandle handle)
    {
        return true;
    }

    @Override
    public boolean isSupportTableMonitoring(ConnectorSession session)
    {
        return true;
    }
}
