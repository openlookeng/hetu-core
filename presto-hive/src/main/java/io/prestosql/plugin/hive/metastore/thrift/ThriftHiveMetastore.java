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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.HiveErrorCode;
import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveViewNotSupportedException;
import io.prestosql.plugin.hive.PartitionNotFoundException;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.HiveColumnStatistics;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.PartitionWithStatistics;
import io.prestosql.plugin.hive.util.RetryDriver;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaAlreadyExistsException;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableAlreadyExistsException;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.thrift.TException;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.difference;
import static io.prestosql.plugin.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.security.PrincipalType.USER;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.common.FileUtils.makePartName;
import static org.apache.hadoop.hive.metastore.api.HiveObjectType.TABLE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS;

@ThreadSafe
public class ThriftHiveMetastore
        implements ThriftMetastore
{
    private static final Logger log = Logger.get(ThriftHiveMetastore.class);

    private final ThriftMetastoreStats stats = new ThriftMetastoreStats();
    private final MetastoreLocator clientProvider;
    private final double backoffScaleFactor;
    private final Duration minBackoffDelay;
    private final Duration maxBackoffDelay;
    private final Duration maxRetryTime;
    private final Duration maxWaitForLock;
    private final int maxRetries;
    private final boolean isRoleNameCaseSensitive;
    private final boolean impersonationEnabled;

    private final AtomicInteger chosenGetTableAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private volatile boolean metastoreKnownToSupportTableParamEqualsPredicate;
    private volatile boolean metastoreKnownToSupportTableParamLikePredicate;

    @Inject
    public ThriftHiveMetastore(MetastoreLocator metastoreLocator, ThriftHiveMetastoreConfig thriftConfig)
    {
        this.clientProvider = requireNonNull(metastoreLocator, "metastoreLocator is null");
        this.backoffScaleFactor = thriftConfig.getBackoffScaleFactor();
        this.minBackoffDelay = thriftConfig.getMinBackoffDelay();
        this.maxBackoffDelay = thriftConfig.getMaxBackoffDelay();
        this.maxRetryTime = thriftConfig.getMaxRetryTime();
        this.maxRetries = thriftConfig.getMaxRetries();
        this.maxWaitForLock = thriftConfig.getMaxWaitForTransactionLock();
        this.isRoleNameCaseSensitive = thriftConfig.isRoleNameCaseSensitive();
        this.impersonationEnabled = thriftConfig.isImpersonationEnabled();
    }

    @Managed
    @Flatten
    public ThriftMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getAllDatabases", stats.getGetAllDatabases().wrap(() -> {
                        try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return client.getAllDatabases();
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getDatabase", stats.getGetDatabase().wrap(() -> {
                        try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return Optional.of(client.getDatabase(databaseName));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        Callable<List<String>> getAllTables = stats.getGetAllTables().wrap(() -> {
            try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                return client.getAllTables(databaseName);
            }
        });

        Callable<Void> getDatabase = stats.getGetDatabase().wrap(() -> {
            try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                client.getDatabase(databaseName);
                return null;
            }
        });

        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllTables", () -> {
                        List<String> tables = getAllTables.call();
                        if (tables.isEmpty()) {
                            // Check to see if the database exists
                            getDatabase.call();
                        }
                        return Optional.of(tables);
                    });
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getTable", stats.getGetTable().wrap(() -> {
                        Table table = getTableFromMetastore(identity, databaseName, tableName);
                        // we need delete view type check
                        return Optional.of(table);
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private Table getTableFromMetastore(HiveIdentity identity, String databaseName, String tableName)
            throws TException
    {
        return alternativeCall(
                () -> createMetastoreClient(identity),
                chosenGetTableAlternative,
                client -> client.getTable(databaseName, tableName),
                client -> client.getTableWithCapabilities(databaseName, tableName));
    }

    private Table getTableFromMetastore(String databaseName, String tableName)
            throws TException
    {
        return alternativeCall(
                () -> createMetastoreClient(),
                chosenGetTableAlternative,
                client -> client.getTable(databaseName, tableName),
                client -> client.getTableWithCapabilities(databaseName, tableName));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ThriftMetastoreUtil.getSupportedColumnStatistics(type);
    }

    private static boolean isPrestoView(Table table)
    {
        return "true".equals(table.getParameters().get(PRESTO_VIEW_FLAG));
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity identity, Table table)
    {
        List<String> dataColumns = table.getSd().getCols().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());
        HiveBasicStatistics basicStatistics = ThriftMetastoreUtil.getHiveBasicStatistics(table.getParameters());
        Map<String, HiveColumnStatistics> columnStatistics = getTableColumnStatistics(identity, table.getDbName(), table.getTableName(),
                dataColumns, basicStatistics.getRowCount());
        return new PartitionStatistics(basicStatistics, columnStatistics);
    }

    private Map<String, HiveColumnStatistics> getTableColumnStatistics(HiveIdentity identity, String databaseName, String tableName, List<String> columns, OptionalLong rowCount)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getTableColumnStatistics", stats.getGetTableColumnStatistics().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            return groupStatisticsByColumn(client.getTableColumnStatistics(databaseName, tableName, columns), rowCount);
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, Table table, List<Partition> partitions)
    {
        List<String> dataColumns = table.getSd().getCols().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());
        List<String> partitionColumns = table.getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());

        Map<String, HiveBasicStatistics> partitionBasicStatistics = partitions.stream()
                .collect(toImmutableMap(
                        partition -> makePartName(partitionColumns, partition.getValues()),
                        partition -> ThriftMetastoreUtil.getHiveBasicStatistics(partition.getParameters())));
        Map<String, OptionalLong> partitionRowCounts = partitionBasicStatistics.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getRowCount()));
        Map<String, Map<String, HiveColumnStatistics>> partitionColumnStatistics = getPartitionColumnStatistics(
                identity,
                table.getDbName(),
                table.getTableName(),
                partitionBasicStatistics.keySet(),
                dataColumns,
                partitionRowCounts);
        ImmutableMap.Builder<String, PartitionStatistics> result = ImmutableMap.builder();
        for (String partitionName : partitionBasicStatistics.keySet()) {
            HiveBasicStatistics basicStatistics = partitionBasicStatistics.get(partitionName);
            Map<String, HiveColumnStatistics> columnStatistics = partitionColumnStatistics.getOrDefault(partitionName, ImmutableMap.of());
            result.put(partitionName, new PartitionStatistics(basicStatistics, columnStatistics));
        }

        return result.build();
    }

    @Override
    public Optional<List<FieldSchema>> getFields(HiveIdentity identity, String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(MetaException.class, UnknownTableException.class, UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getFields", stats.getGetFields().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return Optional.of(ImmutableList.copyOf(client.getFields(databaseName, tableName)));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(
            HiveIdentity identity,
            String databaseName,
            String tableName,
            Set<String> partitionNames,
            List<String> columnNames,
            Map<String, OptionalLong> partitionRowCounts)
    {
        return getMetastorePartitionColumnStatistics(identity, databaseName, tableName, partitionNames, columnNames).entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> groupStatisticsByColumn(entry.getValue(), partitionRowCounts.getOrDefault(entry.getKey(), OptionalLong.empty()))));
    }

    private Map<String, List<ColumnStatisticsObj>> getMetastorePartitionColumnStatistics(HiveIdentity identity, String databaseName, String tableName, Set<String> partitionNames, List<String> columnNames)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionColumnStatistics", stats.getGetPartitionColumnStatistics().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            return client.getPartitionColumnStatistics(databaseName, tableName, ImmutableList.copyOf(partitionNames), columnNames);
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private Map<String, HiveColumnStatistics> groupStatisticsByColumn(List<ColumnStatisticsObj> statistics, OptionalLong rowCount)
    {
        return statistics.stream()
                .collect(toImmutableMap(ColumnStatisticsObj::getColName, statisticsObj -> ThriftMetastoreUtil.fromMetastoreApiColumnStatistics(statisticsObj, rowCount)));
    }

    @Override
    public void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        Table originalTable = getTable(identity, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        PartitionStatistics currentStatistics = getTableStatistics(identity, originalTable);
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        Table modifiedTable = originalTable.deepCopy();
        HiveBasicStatistics basicStatistics = updatedStatistics.getBasicStatistics();
        modifiedTable.setParameters(ThriftMetastoreUtil.updateStatisticsParameters(modifiedTable.getParameters(), basicStatistics));
        alterTable(identity, databaseName, tableName, modifiedTable);

        io.prestosql.plugin.hive.metastore.Table table = ThriftMetastoreUtil.fromMetastoreApiTable(modifiedTable);
        OptionalLong rowCount = basicStatistics.getRowCount();
        List<ColumnStatisticsObj> metastoreColumnStatistics = updatedStatistics.getColumnStatistics().entrySet().stream()
                .map(entry -> ThriftMetastoreUtil.createMetastoreColumnStatistics(entry.getKey(), table.getColumn(entry.getKey()).get().getType(), entry.getValue(), rowCount))
                .collect(toImmutableList());
        if (!metastoreColumnStatistics.isEmpty()) {
            setTableColumnStatistics(identity, databaseName, tableName, metastoreColumnStatistics);
        }
        Set<String> removedColumnStatistics = difference(currentStatistics.getColumnStatistics().keySet(), updatedStatistics.getColumnStatistics().keySet());
        removedColumnStatistics.forEach(column -> deleteTableColumnStatistics(identity, databaseName, tableName, column));
    }

    private void setTableColumnStatistics(HiveIdentity identity, String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("setTableColumnStatistics", stats.getCreateDatabase().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            client.setTableColumnStatistics(databaseName, tableName, statistics);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private void deleteTableColumnStatistics(HiveIdentity identity, String databaseName, String tableName, String columnName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("deleteTableColumnStatistics", stats.getCreateDatabase().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            client.deleteTableColumnStatistics(databaseName, tableName, columnName);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity identity, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        List<Partition> partitions = getPartitionsByNames(identity, databaseName, tableName, ImmutableList.of(partitionName));
        if (partitions.size() != 1) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Metastore returned multiple partitions for name: " + partitionName);
        }

        Table table = getTable(identity, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        PartitionStatistics currentStatistics = requireNonNull(
                getPartitionStatistics(identity, table, partitions).get(partitionName), "getPartitionStatistics() returned null");
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        Partition originalPartition = getOnlyElement(partitions);
        Partition modifiedPartition = originalPartition.deepCopy();
        HiveBasicStatistics basicStatistics = updatedStatistics.getBasicStatistics();
        modifiedPartition.setParameters(ThriftMetastoreUtil.updateStatisticsParameters(modifiedPartition.getParameters(), basicStatistics));
        alterPartitionWithoutStatistics(identity, databaseName, tableName, modifiedPartition);

        updatePartitionColumnStatistics(identity, modifiedPartition, databaseName, tableName, partitionName, basicStatistics, currentStatistics, updatedStatistics);
    }

    @Override
    public synchronized void updatePartitionsStatistics(HiveIdentity identity, String databaseName, String tableName, Map<String, Function<PartitionStatistics, PartitionStatistics>> partNamesUpdateFunctionMap)
    {
        ImmutableList.Builder<Partition> modifiedPartitionBuilder = ImmutableList.builder();
        ImmutableMap.Builder<String, PartitionInfo> partitionInfoMapBuilder = ImmutableMap.builder();
        Optional<Table> table = getTable(identity, databaseName, tableName);

        List<Partition> partitions = getPartitionsByNames(identity, databaseName, tableName, partNamesUpdateFunctionMap.keySet().stream().collect(Collectors.toList()));
        Map<String, PartitionStatistics> partitionsStatistics = getPartitionStatistics(identity, table.get(), partitions);

        if (partitions.size() != partitionsStatistics.size() || partitions.size() != partNamesUpdateFunctionMap.size()) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Metastore returned multiple partitions");
        }
        List<String> partColumns = table.get().getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());
        for (int index = 0; index < partitions.size(); index++) {
            String partitionName = makePartName(partColumns, partitions.get(index).getValues());
            PartitionStatistics currentStatistics = requireNonNull(partitionsStatistics.get(partitionName),
                    "getPartitionStatistics() returned null");

            PartitionStatistics updatedStatistics = partNamesUpdateFunctionMap.get(partitionName).apply(currentStatistics);

            Partition originalPartition = partitions.get(index);
            Partition modifiedPartition = originalPartition.deepCopy();
            HiveBasicStatistics basicStatistics = updatedStatistics.getBasicStatistics();
            modifiedPartition.setParameters(ThriftMetastoreUtil.updateStatisticsParameters(modifiedPartition.getParameters(), basicStatistics));
            originalPartition.setParameters(ThriftMetastoreUtil.updateStatisticsParameters(originalPartition.getParameters(), basicStatistics));

            modifiedPartitionBuilder.add(modifiedPartition);
            partitionInfoMapBuilder.put(partitionName, new PartitionInfo(basicStatistics, currentStatistics, originalPartition, updatedStatistics));
        }
        alterPartitionsWithoutStatistics(databaseName, tableName, modifiedPartitionBuilder.build());

        ImmutableMap<String, PartitionInfo> partitionInfoMap = partitionInfoMapBuilder.build();
        partitionInfoMap.forEach((partName, partInfo) ->
                updatePartitionColumnStatistics(identity, partInfo.modifiedPartition, databaseName, tableName, partName,
                        partInfo.basicStatistics, partInfo.currentStatistics, partInfo.updatedStatistics));
    }

    private void updatePartitionColumnStatistics(HiveIdentity identity, Partition modifiedPartition, String databaseName,
            String tableName, String partitionName, HiveBasicStatistics basicStatistics,
            PartitionStatistics currentStatistics, PartitionStatistics updatedStatistics)
    {
        Map<String, HiveType> columns = modifiedPartition.getSd().getCols().stream()
                .collect(toImmutableMap(FieldSchema::getName, schema -> HiveType.valueOf(schema.getType())));
        setPartitionColumnStatistics(identity, databaseName, tableName, partitionName, columns, updatedStatistics.getColumnStatistics(), basicStatistics.getRowCount());

        Set<String> removedStatistics = difference(currentStatistics.getColumnStatistics().keySet(), updatedStatistics.getColumnStatistics().keySet());
        removedStatistics.forEach(column -> deletePartitionColumnStatistics(identity, databaseName, tableName, partitionName, column));
    }

    private class PartitionInfo
    {
        private final HiveBasicStatistics basicStatistics;
        private final PartitionStatistics currentStatistics;
        private final Partition modifiedPartition;
        private final PartitionStatistics updatedStatistics;

        PartitionInfo(HiveBasicStatistics basicStatistics, PartitionStatistics currentStatistics,
                Partition modifiedPartition, PartitionStatistics updatedStatistics)
        {
            this.basicStatistics = basicStatistics;
            this.currentStatistics = currentStatistics;
            this.modifiedPartition = modifiedPartition;
            this.updatedStatistics = updatedStatistics;
        }
    }

    private void setPartitionColumnStatistics(
            HiveIdentity identity,
            String databaseName,
            String tableName,
            String partitionName,
            Map<String, HiveType> columns,
            Map<String, HiveColumnStatistics> columnStatistics,
            OptionalLong rowCount)
    {
        List<ColumnStatisticsObj> metastoreColumnStatistics = columnStatistics.entrySet().stream()
                .filter(entry -> columns.containsKey(entry.getKey()))
                .map(entry -> ThriftMetastoreUtil.createMetastoreColumnStatistics(entry.getKey(), columns.get(entry.getKey()), entry.getValue(), rowCount))
                .collect(toImmutableList());
        if (!metastoreColumnStatistics.isEmpty()) {
            setPartitionColumnStatistics(identity, databaseName, tableName, partitionName, metastoreColumnStatistics);
        }
    }

    private void setPartitionColumnStatistics(HiveIdentity identity, String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("setPartitionColumnStatistics", stats.getCreateDatabase().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            client.setPartitionColumnStatistics(databaseName, tableName, partitionName, statistics);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private void deletePartitionColumnStatistics(HiveIdentity identity, String databaseName, String tableName, String partitionName, String columnName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("deletePartitionColumnStatistics", stats.getCreateDatabase().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            client.deletePartitionColumnStatistics(databaseName, tableName, partitionName, columnName);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void createRole(String role, String grantor)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("createRole", stats.getCreateRole().wrap(() -> {
                        try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.createRole(isRoleNameCaseSensitive ? role : role.toLowerCase(Locale.ENGLISH),
                                    grantor);
                            return null;
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void dropRole(String role)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropRole", stats.getDropRole().wrap(() -> {
                        try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.dropRole(isRoleNameCaseSensitive ? role : role.toLowerCase(Locale.ENGLISH));
                            return null;
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Set<String> listRoles()
    {
        try {
            return retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("listRoles", stats.getListRoles().wrap(() -> {
                        try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                            List<String> roles = new ArrayList<>();
                            for (String role : client.getRoleNames()) {
                                roles.add(isRoleNameCaseSensitive ? role : role.toLowerCase(Locale.ENGLISH));
                            }
                            return ImmutableSet.copyOf(roles);
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean withAdminOption, HivePrincipal grantor)
    {
        for (HivePrincipal grantee : grantees) {
            for (String role : roles) {
                grantRole(
                        role,
                        grantee.getName(), ThriftMetastoreUtil.fromPrestoPrincipalType(grantee.getType()),
                        grantor.getName(), ThriftMetastoreUtil.fromPrestoPrincipalType(grantor.getType()),
                        withAdminOption);
            }
        }
    }

    private void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("grantRole", stats.getGrantRole().wrap(() -> {
                        try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.grantRole(isRoleNameCaseSensitive ? role : role.toLowerCase(Locale.ENGLISH),
                                    (granteeType == PrincipalType.ROLE && isRoleNameCaseSensitive) ? granteeName : granteeName.toLowerCase(Locale.ENGLISH),
                                    granteeType,
                                    (grantorType == PrincipalType.ROLE && isRoleNameCaseSensitive) ? grantorName : grantorName.toLowerCase(Locale.ENGLISH),
                                    grantorType,
                                    grantOption);
                            return null;
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOptionFor, HivePrincipal grantor)
    {
        for (HivePrincipal grantee : grantees) {
            for (String role : roles) {
                revokeRole(
                        role,
                        grantee.getName(), ThriftMetastoreUtil.fromPrestoPrincipalType(grantee.getType()),
                        adminOptionFor);
            }
        }
    }

    private void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("revokeRole", stats.getRevokeRole().wrap(() -> {
                        try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.revokeRole(isRoleNameCaseSensitive ? role : role.toLowerCase(Locale.ENGLISH),
                                    (granteeType == PrincipalType.ROLE && isRoleNameCaseSensitive) ? granteeName : granteeName.toLowerCase(Locale.ENGLISH),
                                    granteeType,
                                    grantOption);
                            return null;
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal sourcePrincipal)
    {
        try {
            return retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("listRoleGrants", stats.getListRoleGrants().wrap(() -> {
                        HivePrincipal principal = ThriftMetastoreUtil.applyRoleNameCaseSensitive(sourcePrincipal, isRoleNameCaseSensitive);
                        try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return ThriftMetastoreUtil.fromRolePrincipalGrants(client.listRoleGrants(principal.getName(),
                                    ThriftMetastoreUtil.fromPrestoPrincipalType(principal.getType())),
                                    isRoleNameCaseSensitive);
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        try {
            return retry()
                    .stopOn(UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllViews", stats.getGetAllViews().wrap(() -> {
                        return Optional.of(getPrestoViews(databaseName));
                    }));
        }
        catch (UnknownDBException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private List<String> getPrestoViews(String databaseName)
            throws TException
    {
        /*
         * Thrift call `get_table_names_by_filter` may be translated by Metastore to a SQL query against Metastore database.
         * Hive 2.3 on some databases uses CLOB for table parameter value column and some databases disallow `=` predicate over
         * CLOB values. At the same time, they allow `LIKE` predicates over them.
         */
        String filterWithEquals = HIVE_FILTER_FIELD_PARAMS + PRESTO_VIEW_FLAG + " = \"true\"";
        String filterWithLike = HIVE_FILTER_FIELD_PARAMS + PRESTO_VIEW_FLAG + " LIKE \"true\"";

        if (metastoreKnownToSupportTableParamEqualsPredicate) {
            try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                return client.getTableNamesByFilter(databaseName, filterWithEquals);
            }
        }
        if (metastoreKnownToSupportTableParamLikePredicate) {
            try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                return client.getTableNamesByFilter(databaseName, filterWithLike);
            }
        }

        try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
            List<String> views = client.getTableNamesByFilter(databaseName, filterWithEquals);
            metastoreKnownToSupportTableParamEqualsPredicate = true;
            return views;
        }
        catch (TException | RuntimeException firstException) {
            try (ThriftMetastoreClient client = clientProvider.createMetastoreClient()) {
                List<String> views = client.getTableNamesByFilter(databaseName, filterWithLike);
                metastoreKnownToSupportTableParamLikePredicate = true;
                return views;
            }
            catch (TException | RuntimeException secondException) {
                if (firstException != secondException) {
                    firstException.addSuppressed(secondException);
                }
            }
            throw firstException;
        }
    }

    @Override
    public void createDatabase(HiveIdentity identity, Database database)
    {
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("createDatabase", stats.getCreateDatabase().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            client.createDatabase(database);
                        }
                        return null;
                    }));
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(database.getName());
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void dropDatabase(HiveIdentity identity, String databaseName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidOperationException.class)
                    .stopOnIllegalExceptions()
                    .run("dropDatabase", stats.getDropDatabase().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            client.dropDatabase(databaseName, true, false);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void alterDatabase(HiveIdentity identity, String databaseName, Database database)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterDatabase", stats.getAlterDatabase().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            client.alterDatabase(databaseName, database);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void createTable(HiveIdentity identity, Table table)
    {
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("createTable", stats.getCreateTable().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            client.createTable(table);
                        }
                        return null;
                    }));
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDbName(), table.getTableName()));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(table.getDbName());
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("dropTable", stats.getDropTable().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            client.dropTable(databaseName, tableName, deleteData);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void alterTable(HiveIdentity identity, String databaseName, String tableName, Table table)
    {
        try {
            retry()
                    .stopOn(InvalidOperationException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterTable", stats.getAlterTable().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            Optional<Table> source = getTable(identity, databaseName, tableName);
                            if (!source.isPresent()) {
                                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                            }
                            client.alterTable(databaseName, tableName, table);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNames(HiveIdentity identity, String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNames", stats.getGetPartitionNames().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            return Optional.of(client.getPartitionNames(databaseName, tableName));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(HiveIdentity identity, String databaseName, String tableName, List<String> parts)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNamesByParts", stats.getGetPartitionNamesPs().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            return Optional.of(client.getPartitionNamesFiltered(databaseName, tableName, parts));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void addPartitions(HiveIdentity identity, String databaseName, String tableName, List<PartitionWithStatistics> partitionsWithStatistics)
    {
        List<Partition> partitions = partitionsWithStatistics.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiPartition)
                .collect(toImmutableList());
        addPartitionsWithoutStatistics(identity, databaseName, tableName, partitions);
        for (PartitionWithStatistics partitionWithStatistics : partitionsWithStatistics) {
            storePartitionColumnStatistics(identity, databaseName, tableName, partitionWithStatistics.getPartitionName(), partitionWithStatistics);
        }
    }

    private void addPartitionsWithoutStatistics(HiveIdentity identity, String databaseName, String tableName, List<Partition> partitions)
    {
        if (partitions.isEmpty()) {
            return;
        }
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class, PrestoException.class)
                    .stopOnIllegalExceptions()
                    .run("addPartitions", stats.getAddPartitions().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            int partitionsAdded = client.addPartitions(partitions);
                            if (partitionsAdded != partitions.size()) {
                                throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR,
                                        format("Hive metastore only added %s of %s partitions", partitionsAdded, partitions.size()));
                            }
                            return null;
                        }
                    }));
        }
        catch (AlreadyExistsException e) {
            throw new PrestoException(ALREADY_EXISTS, format("One or more partitions already exist for table '%s.%s'", databaseName, tableName), e);
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropPartition", stats.getDropPartition().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            client.dropPartition(databaseName, tableName, parts, deleteData);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), parts);
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void dropPartitionByRequest(HiveIdentity identity, String databaseName, String tableName, List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData, boolean ifExists)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropPartitionByName", stats.getDropPartition().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            client.dropPartitionByRequest(databaseName, tableName, partExprs, deleteData, ifExists);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            return;
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void alterPartition(HiveIdentity identity, String databaseName, String tableName, PartitionWithStatistics partitionWithStatistics)
    {
        alterPartitionWithoutStatistics(identity, databaseName, tableName, ThriftMetastoreUtil.toMetastoreApiPartition(partitionWithStatistics));
        storePartitionColumnStatistics(identity, databaseName, tableName, partitionWithStatistics.getPartitionName(), partitionWithStatistics);
        dropExtraColumnStatisticsAfterAlterPartition(identity, databaseName, tableName, partitionWithStatistics);
    }

    private void alterPartitionWithoutStatistics(HiveIdentity identity, String databaseName, String tableName, Partition partition)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterPartition", stats.getAlterPartition().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            client.alterPartition(databaseName, tableName, partition);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partition.getValues());
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private void alterPartitionsWithoutStatistics(String databaseName, String tableName, List<Partition> partitions)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterPartition", stats.getAlterPartition().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.alterPartitions(databaseName, tableName, partitions);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), null);
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private void storePartitionColumnStatistics(HiveIdentity identity, String databaseName, String tableName, String partitionName, PartitionWithStatistics partitionWithStatistics)
    {
        PartitionStatistics statistics = partitionWithStatistics.getStatistics();
        Map<String, HiveColumnStatistics> columnStatistics = statistics.getColumnStatistics();
        if (columnStatistics.isEmpty()) {
            return;
        }
        Map<String, HiveType> columnTypes = partitionWithStatistics.getPartition().getColumns().stream()
                .collect(toImmutableMap(Column::getName, Column::getType));
        setPartitionColumnStatistics(identity, databaseName, tableName, partitionName, columnTypes, columnStatistics, statistics.getBasicStatistics().getRowCount());
    }

    /*
     * After altering a partition metastore preserves all column statistics for that partition.
     *
     * The old statistics are supposed to be replaced by storing the new partition statistics.
     *
     * In case when the new statistics are not present for some columns, or if the table schema has changed
     * if is needed to explicitly remove the statistics from the metastore for that columns.
     */
    private void dropExtraColumnStatisticsAfterAlterPartition(
            HiveIdentity identity,
            String databaseName,
            String tableName,
            PartitionWithStatistics partitionWithStatistics)
    {
        List<String> dataColumns = partitionWithStatistics.getPartition().getColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());

        Set<String> columnsWithMissingStatistics = new HashSet<>(dataColumns);
        columnsWithMissingStatistics.removeAll(partitionWithStatistics.getStatistics().getColumnStatistics().keySet());

        // In case new partition had the statistics computed for all the columns, the storePartitionColumnStatistics
        // call in the alterPartition will just overwrite the old statistics. There is no need to explicitly remove anything.
        if (columnsWithMissingStatistics.isEmpty()) {
            return;
        }

        // check if statistics for the columnsWithMissingStatistics are actually stored in the metastore
        // when trying to remove any missing statistics the metastore throws NoSuchObjectException
        String partitionName = partitionWithStatistics.getPartitionName();
        List<ColumnStatisticsObj> statisticsToBeRemoved = getMetastorePartitionColumnStatistics(
                identity,
                databaseName,
                tableName,
                ImmutableSet.of(partitionName),
                ImmutableList.copyOf(columnsWithMissingStatistics))
                .getOrDefault(partitionName, ImmutableList.of());

        for (ColumnStatisticsObj statistics : statisticsToBeRemoved) {
            deletePartitionColumnStatistics(identity, databaseName, tableName, partitionName, statistics.getColName());
        }
    }

    @Override
    public Optional<Partition> getPartition(HiveIdentity identity, String databaseName, String tableName, List<String> partitionValues)
    {
        requireNonNull(partitionValues, "partitionValues is null");
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartition", stats.getGetPartition().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            return Optional.of(client.getPartition(databaseName, tableName, partitionValues));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public List<Partition> getPartitionsByNames(HiveIdentity identity, String databaseName, String tableName, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNames), "partitionNames is empty");

        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionsByNames", stats.getGetPartitionsByNames().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient(identity)) {
                            return client.getPartitionsByNames(databaseName, tableName, partitionNames);
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            // assume none of the partitions in the batch are available
            return ImmutableList.of();
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, HivePrincipal sourceGrantee, Set<HivePrivilegeInfo> privileges)
    {
        Set<PrivilegeGrantInfo> requestedPrivileges = privileges.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiPrivilegeGrantInfo)
                .collect(Collectors.toSet());
        checkArgument(!containsAllPrivilege(requestedPrivileges), "\"ALL\" not supported in PrivilegeGrantInfo.privilege");
        HivePrincipal grantee = ThriftMetastoreUtil.applyRoleNameCaseSensitive(sourceGrantee, isRoleNameCaseSensitive);

        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("grantTablePrivileges", stats.getGrantTablePrivileges().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            Set<HivePrivilegeInfo> existingPrivileges = listTablePrivileges(databaseName, tableName, grantee);

                            Set<PrivilegeGrantInfo> privilegesToGrant = new HashSet<>(requestedPrivileges);
                            Iterator<PrivilegeGrantInfo> iterator = privilegesToGrant.iterator();
                            while (iterator.hasNext()) {
                                HivePrivilegeInfo requestedPrivilege = getOnlyElement(ThriftMetastoreUtil.parsePrivilege(iterator.next(), Optional.empty()));

                                for (HivePrivilegeInfo existingPrivilege : existingPrivileges) {
                                    if ((requestedPrivilege.isContainedIn(existingPrivilege))) {
                                        iterator.remove();
                                    }
                                    else if (existingPrivilege.isContainedIn(requestedPrivilege)) {
                                        throw new PrestoException(NOT_SUPPORTED, format(
                                                "Granting %s WITH GRANT OPTION is not supported while %s possesses %s",
                                                requestedPrivilege.getHivePrivilege().name(),
                                                grantee,
                                                requestedPrivilege.getHivePrivilege().name()));
                                    }
                                }
                            }

                            if (privilegesToGrant.isEmpty()) {
                                return null;
                            }

                            metastoreClient.grantPrivileges(buildPrivilegeBag(databaseName, tableName, grantee, privilegesToGrant));
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, HivePrincipal sourceGrantee, Set<HivePrivilegeInfo> privileges)
    {
        Set<PrivilegeGrantInfo> requestedPrivileges = privileges.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiPrivilegeGrantInfo)
                .collect(Collectors.toSet());
        checkArgument(!containsAllPrivilege(requestedPrivileges), "\"ALL\" not supported in PrivilegeGrantInfo.privilege");
        HivePrincipal grantee = ThriftMetastoreUtil.applyRoleNameCaseSensitive(sourceGrantee, isRoleNameCaseSensitive);

        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("revokeTablePrivileges", stats.getRevokeTablePrivileges().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            Set<HivePrivilegeInfo.HivePrivilege> existingHivePrivileges = listTablePrivileges(databaseName, tableName, grantee).stream()
                                    .map(HivePrivilegeInfo::getHivePrivilege)
                                    .collect(toSet());

                            Set<PrivilegeGrantInfo> privilegesToRevoke = requestedPrivileges.stream()
                                    .filter(privilegeGrantInfo -> existingHivePrivileges.contains(getOnlyElement(ThriftMetastoreUtil.parsePrivilege(privilegeGrantInfo, Optional.empty())).getHivePrivilege()))
                                    .collect(toSet());

                            if (privilegesToRevoke.isEmpty()) {
                                return null;
                            }

                            metastoreClient.revokePrivileges(buildPrivilegeBag(databaseName, tableName, grantee, privilegesToRevoke));
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, HivePrincipal sourcePrincipal)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("listTablePrivileges", stats.getListTablePrivileges().wrap(() -> {
                        Table table = getTableFromMetastore(databaseName, tableName);
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            ImmutableSet.Builder<HivePrivilegeInfo> privileges = ImmutableSet.builder();
                            List<HiveObjectPrivilege> hiveObjectPrivilegeList;
                            HivePrincipal principal = ThriftMetastoreUtil.applyRoleNameCaseSensitive(sourcePrincipal, isRoleNameCaseSensitive);
                            // principal can be null when we want to list all privileges for admins
                            if (principal == null) {
                                hiveObjectPrivilegeList = client.listPrivileges(
                                        null,
                                        null,
                                        new HiveObjectRef(TABLE, databaseName, tableName, null, null));
                            }
                            else {
                                if (principal.getType() == USER && table.getOwner().equals(principal.getName())) {
                                    privileges.add(new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.OWNERSHIP, true, principal, principal));
                                }
                                hiveObjectPrivilegeList = client.listPrivileges(
                                        principal.getName(),
                                        ThriftMetastoreUtil.fromPrestoPrincipalType(principal.getType()),
                                        new HiveObjectRef(TABLE, databaseName, tableName, null, null));
                            }
                            for (HiveObjectPrivilege hiveObjectPrivilege : hiveObjectPrivilegeList) {
                                HivePrincipal grantee = new HivePrincipal(ThriftMetastoreUtil.fromMetastoreApiPrincipalType(hiveObjectPrivilege.getPrincipalType()), hiveObjectPrivilege.getPrincipalName());

                                privileges.addAll(ThriftMetastoreUtil.parsePrivilege(hiveObjectPrivilege.getGrantInfo(),
                                        Optional.of(ThriftMetastoreUtil.applyRoleNameCaseSensitive(grantee, isRoleNameCaseSensitive))));
                            }
                            return privileges.build();
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    @Override
    public long openTransaction(HiveIdentity identity)
    {
        checkArgument(!identity.getUsername().map(String::isEmpty).orElse(true), "User should be provided to open transaction");
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("openTransaction", stats.getOpenTransaction().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            return metastoreClient.openTransaction(identity.getUsername().get());
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void commitTransaction(HiveIdentity identity, long transactionId)
    {
        try {
            retry()
                    .stopOnIllegalExceptions()
                    .stopOn(TxnAbortedException.class)
                    .run("commitTransaction", stats.getCommitTransaction().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            metastoreClient.commitTransaction(transactionId);
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void abortTransaction(HiveIdentity identity, long transactionId)
    {
        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("abortTransaction", stats.getCommitTransaction().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            metastoreClient.abortTransaction(transactionId);
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void sendTransactionHeartbeat(HiveIdentity identity, long transactionId)
    {
        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("sendTransactionHeartbeat", (() -> {
                        try (ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            metastoreClient.sendTransactionHeartbeat(transactionId);
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void acquireSharedReadLock(HiveIdentity identity, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        DataOperationType operationType = DataOperationType.SELECT;
        acquireLock(identity, queryId, transactionId, fullTables, partitions, operationType);
    }

    @Override
    public void acquireLock(HiveIdentity identity,
                             String queryId,
                             long transactionId,
                             List<SchemaTableName> fullTables,
                             List<HivePartition> partitions,
                             DataOperationType operationType)
    {
        checkArgument(!identity.getUsername().map(String::isEmpty).orElse(true), "User should be provided to acquire locks");
        requireNonNull(queryId, "queryId is null");

        if (fullTables.isEmpty() && partitions.isEmpty()) {
            return;
        }

        LockRequestBuilder request = new LockRequestBuilder(queryId)
                .setTransactionId(transactionId)
                .setUser(identity.getUsername().get());

        for (SchemaTableName table : fullTables) {
            request.addLockComponent(createLockComponent(table, Optional.empty(), operationType));
        }

        for (HivePartition partition : partitions) {
            request.addLockComponent(createLockComponent(partition.getTableName(), Optional.of(partition.getPartitionId()), operationType));
        }

        LockRequest lockRequest = request.build();
        try {
            LockResponse response = retry()
                    .stopOn(NoSuchTxnException.class, TxnAbortedException.class, MetaException.class)
                    .run("acquireLock", stats.getAcquireLock().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            return metastoreClient.acquireLock(lockRequest);
                        }
                    }));

            long waitStart = nanoTime();
            while (response.getState() == LockState.WAITING) {
                long lockId = response.getLockid();

                if (Duration.nanosSince(waitStart).compareTo(maxWaitForLock) > 0) {
                    // timed out
                    throw new PrestoException(HiveErrorCode.HIVE_TABLE_LOCK_NOT_ACQUIRED, format("Timed out waiting for lock %d in hive transaction %s for query %s", lockId, transactionId, queryId));
                }

                log.debug("Waiting for lock %d in hive transaction %s for query %s", lockId, transactionId, queryId);

                response = retry()
                        .stopOn(NoSuchTxnException.class, NoSuchLockException.class, TxnAbortedException.class, MetaException.class)
                        .run("checkLock", stats.getCheckLock().wrap(() -> {
                            try (ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                                return metastoreClient.checkLock(lockId);
                            }
                        }));
            }

            if (response.getState() != LockState.ACQUIRED) {
                throw new PrestoException(HiveErrorCode.HIVE_TABLE_LOCK_NOT_ACQUIRED, "Could not acquire lock. Lock in state " + response.getState());
            }
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private static LockComponent createLockComponent(SchemaTableName table,
                                                     Optional<String> partitionName,
                                                     DataOperationType operationType)
    {
        requireNonNull(table, "table is null");
        requireNonNull(partitionName, "partitionName is null");

        LockComponentBuilder builder = new LockComponentBuilder();
        builder.setOperationType(operationType);
        switch (operationType) {
            case SELECT:
            case INSERT:
                builder.setShared();
                break;
            case DELETE:
            case UPDATE:
                builder.setSemiShared();
                break;
            default:
                throw new PrestoException(HiveErrorCode.HIVE_UNKNOWN_ERROR, "Unexpected operationType to aquireLock " + operationType);
        }

        builder.setDbName(table.getSchemaName());
        builder.setTableName(table.getTableName());
        requireNonNull(partitionName, "partitionName is null").ifPresent(builder::setPartitionName);

        // acquire locks is called only for TransactionalTable
        builder.setIsTransactional(true);
        return builder.build();
    }

    @Override
    public String getValidWriteIds(HiveIdentity identity, List<SchemaTableName> tables, long currentTransactionId, boolean isVacuum)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getValidWriteIds", stats.getValidWriteIds().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            return metastoreClient.getValidWriteIds(
                                    tables.stream()
                                            .map(table -> format("%s.%s", table.getSchemaName(), table.getTableName()))
                                            .collect(toImmutableList()),
                                    currentTransactionId,
                                    isVacuum);
                        }
                    }));
        }
        catch (TException e) {
            // When calling Hive metastore < 3, the call fails with
            // Required field 'open_txns' is unset! Struct:GetOpenTxnsResponse(txn_high_water_mark:4, open_txns:null, min_open_txn:4, abortedBits:null)
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Failed to open transaction. Transactional tables support require Hive metastore version at least 3.0", e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public long getTableWriteId(String dbName, String tableName, long transactionId)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getTableWriteId", stats.getTableWriteId().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            return metastoreClient.getTableWriteId(dbName, tableName, transactionId);
                        }
                    }));
        }
        catch (TException e) {
            if (e.getMessage().contains("Invalid method name")) {
                throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Transactional tables support require Hive metastore version at least 3.0");
            }
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest rqst)
    {
        try {
            try (ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                return metastoreClient.showLocks(rqst);
            }
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getConfigValueFromServer", () -> {
                        try (ThriftMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            return Optional.ofNullable(metastoreClient.get_config_value(name, null));
                        }
                    });
        }
        catch (ConfigValSecurityException e) {
            log.debug(e, "Could not fetch value for config '%s' from Hive", name);
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private PrivilegeBag buildPrivilegeBag(
            String databaseName,
            String tableName,
            HivePrincipal grantee,
            Set<PrivilegeGrantInfo> privilegeGrantInfos)
    {
        ImmutableList.Builder<HiveObjectPrivilege> privilegeBagBuilder = ImmutableList.builder();
        for (PrivilegeGrantInfo privilegeGrantInfo : privilegeGrantInfos) {
            privilegeBagBuilder.add(
                    new HiveObjectPrivilege(
                            new HiveObjectRef(TABLE, databaseName, tableName, null, null),
                            grantee.getName(),
                            ThriftMetastoreUtil.fromPrestoPrincipalType(grantee.getType()),
                            privilegeGrantInfo,
                            "SQL"));
        }
        return new PrivilegeBag(privilegeBagBuilder.build());
    }

    private boolean containsAllPrivilege(Set<PrivilegeGrantInfo> requestedPrivileges)
    {
        return requestedPrivileges.stream()
                .anyMatch(privilege -> privilege.getPrivilege().equalsIgnoreCase("all"));
    }

    @SafeVarargs
    private final <T> T alternativeCall(
            ClientSupplier clientSupplier,
            AtomicInteger chosenAlternative,
            Call<T>... alternatives)
            throws TException
    {
        checkArgument(alternatives.length > 0, "No alternatives");
        int chosen = chosenAlternative.get();
        checkArgument(chosen == Integer.MAX_VALUE || (0 <= chosen && chosen < alternatives.length), "Bad chosen alternative value: %s", chosen);

        if (chosen != Integer.MAX_VALUE) {
            try (ThriftMetastoreClient client = clientSupplier.createMetastoreClient()) {
                return alternatives[chosen].callOn(client);
            }
        }

        Exception firstException = null;
        for (int i = 0; i < alternatives.length; i++) {
            int position = i;
            try (ThriftMetastoreClient client = clientSupplier.createMetastoreClient()) {
                T result = alternatives[i].callOn(client);
                chosenAlternative.updateAndGet(currentChosen -> Math.min(currentChosen, position));
                return result;
            }
            catch (TException | RuntimeException exception) {
                if (firstException == null) {
                    firstException = exception;
                }
                else if (firstException != exception) {
                    firstException.addSuppressed(exception);
                }
            }
        }

        verifyNotNull(firstException);
        propagateIfPossible(firstException, TException.class);
        throw propagate(firstException);
    }

    private ThriftMetastoreClient createMetastoreClient() throws TException
    {
        return clientProvider.createMetastoreClient();
    }

    private ThriftMetastoreClient createMetastoreClient(HiveIdentity identity)
            throws TException
    {
        ThriftMetastoreClient client = createMetastoreClient();
        if (!impersonationEnabled) {
            return client;
        }

        setMetastoreUserOrClose(client, identity.getUsername()
                .orElseThrow(() -> new IllegalStateException("End-user name should exist when metastore impersonation is enabled")));
        return client;
    }

    private static void setMetastoreUserOrClose(ThriftMetastoreClient client, String username)
            throws TException
    {
        try {
            client.setUGI(username);
        }
        catch (Throwable t) {
            // close client and suppress any error from close
            try (Closeable ignored = client) {
                throw t;
            }
            catch (IOException e) {
                // impossible; will be suppressed
            }
        }
    }

    protected RetryDriver retry()
    {
        return RetryDriver.retry()
                .exponentialBackoff(minBackoffDelay, maxBackoffDelay, maxRetryTime, backoffScaleFactor)
                .maxAttempts(maxRetries + 1)
                .stopOn(PrestoException.class);
    }

    protected static RuntimeException propagate(Throwable throwable)
    {
        if (throwable instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        throwIfUnchecked(throwable);
        throw new RuntimeException(throwable);
    }

    @FunctionalInterface
    private interface ClientSupplier
    {
        ThriftMetastoreClient createMetastoreClient()
                throws TException;
    }

    @FunctionalInterface
    private interface Call<T>
    {
        T callOn(ThriftMetastoreClient client)
                throws TException;
    }
}
