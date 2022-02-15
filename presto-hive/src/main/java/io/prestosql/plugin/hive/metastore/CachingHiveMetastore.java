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
package io.prestosql.plugin.hive.metastore;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.ForCachingHiveMetastore;
import io.prestosql.plugin.hive.ForCachingHiveMetastoreTableRefresh;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveErrorCode;
import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.PartitionNotFoundException;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Streams.stream;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.prestosql.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.prestosql.plugin.hive.metastore.HivePartitionName.hivePartitionName;
import static io.prestosql.plugin.hive.metastore.HiveTableName.hiveTableName;
import static io.prestosql.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Hive Metastore Cache
 */
@ThreadSafe
public class CachingHiveMetastore
        implements HiveMetastore
{
    private static final Logger LOG = Logger.get(CachingHiveMetastore.class);
    public static final int PASSIVE_CACHE_VERIFICATION_THRESHOLD = 300 * 1000;
    public static final int TABLE_CACHE_CLEANUP_TIME = 2000;
    public static final int TABLE_CACHE_REFRESH_TIME = 1000;

    protected final HiveMetastore delegate;
    private final LoadingCache<String, Optional<Database>> databaseCache;
    private final LoadingCache<String, List<String>> databaseNamesCache;
    private final LoadingCache<String, Optional<List<String>>> tableNamesCache;
    private final LoadingCache<String, Optional<List<String>>> viewNamesCache;
    private final LoadingCache<String, Set<String>> rolesCache;
    private final LoadingCache<HivePrincipal, Set<RoleGrant>> roleGrantsCache;
    private final LoadingCache<String, Optional<String>> configValuesCache;

    private final LoadingCache<WithIdentity<HiveTableName>, Optional<Table>> tableCache;
    private final LoadingCache<WithIdentity<HiveTableName>, Optional<List<String>>> partitionNamesCache;
    private final LoadingCache<WithIdentity<HiveTableName>, WithValidation<Table, PartitionStatistics>> tableStatisticsCache;
    private final LoadingCache<WithIdentity<HivePartitionName>, WithValidation<Table, PartitionStatistics>> partitionStatisticsCache;
    private final LoadingCache<WithIdentity<HivePartitionName>, Optional<WithValidation<Table, Partition>>> partitionCache;
    private final LoadingCache<WithIdentity<PartitionFilter>, Optional<WithValidation<Table, List<String>>>> partitionFilterCache;

    private final boolean skipCache;
    private final boolean skipTableCache;
    private final boolean dontVerifyCacheEntry;

    @Inject
    public CachingHiveMetastore(@ForCachingHiveMetastore HiveMetastore delegate,
                                @ForCachingHiveMetastore Executor executor,
                                @ForCachingHiveMetastoreTableRefresh Executor tableRefreshExecutor,
                                HiveConfig hiveConfig,
                                NodeManager nodeManager)
    {
        this(
                delegate,
                executor,
                tableRefreshExecutor,
                hiveConfig.getMetastoreCacheTtl(),
                hiveConfig.getMetastoreRefreshInterval(),
                hiveConfig.getMetastoreDBCacheTtl(),
                hiveConfig.getMetastoreDBRefreshInterval(),
                hiveConfig.getMetastoreCacheMaximumSize(),
                !(nodeManager.getCurrentNode().isCoordinator() || hiveConfig.getWorkerMetaStoreCacheEnabled()));
    }

    public CachingHiveMetastore(HiveMetastore delegate, Executor executor, Executor tableRefreshExecutor, Duration cacheTtl, Duration refreshInterval,
                                Duration dbCacheTtl, Duration dbRefreshInterval,
                                long maximumSize, boolean skipCache)
    {
        this(
                delegate,
                executor,
                tableRefreshExecutor, OptionalLong.of(cacheTtl.toMillis()),
                refreshInterval.toMillis() >= cacheTtl.toMillis() ? OptionalLong.empty() : OptionalLong.of(refreshInterval.toMillis()),
                OptionalLong.of(dbCacheTtl.toMillis()),
                dbRefreshInterval.toMillis() >= dbCacheTtl.toMillis() ? OptionalLong.empty() : OptionalLong.of(dbRefreshInterval.toMillis()),
                maximumSize,
                skipCache);
    }

    public static CachingHiveMetastore memoizeMetastore(HiveMetastore delegate, long maximumSize)
    {
        // If delegate is instance of CachingHiveMetastore, we are bypassing directly to second layer of cache, to get cached values.
        return new CachingHiveMetastore(
                delegate,
                newDirectExecutorService(),
                newDirectExecutorService(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                maximumSize,
                false || delegate instanceof CachingHiveMetastore);
    }

    private CachingHiveMetastore(HiveMetastore delegate, Executor executor, Executor tableRefreshExecutor,
                                 OptionalLong expiresAfterWriteMillisTable, OptionalLong refreshMillsTable,
                                 OptionalLong expiresAfterWriteMillisDB, OptionalLong refreshMillsDB,
                                 long maximumSize, boolean skipCache)
    {
        boolean dontVerifyCache;
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(executor, "executor is null");

        // if refreshMillsDB is present and is 0 , keeps cache unrefreshed.
        this.skipCache = skipCache
                || (refreshMillsDB.isPresent() && refreshMillsDB.getAsLong() == 0);
        this.skipTableCache = skipCache
                || (refreshMillsTable.isPresent() && refreshMillsTable.getAsLong() == 0
                    && expiresAfterWriteMillisTable.isPresent() && expiresAfterWriteMillisTable.getAsLong() == 0);

        OptionalLong tableCacheTtl;
        OptionalLong tableRefreshTtl;

        dontVerifyCache = true;
        tableCacheTtl = expiresAfterWriteMillisTable;
        tableRefreshTtl = refreshMillsTable;

        if (this.skipTableCache == false) {
            long refresh = refreshMillsTable.orElse(0);
            long ttl = expiresAfterWriteMillisTable.orElse(0);
            if (refresh > PASSIVE_CACHE_VERIFICATION_THRESHOLD
                    || (0 == refresh && ttl > PASSIVE_CACHE_VERIFICATION_THRESHOLD)) {
                dontVerifyCache = false;
                tableCacheTtl = OptionalLong.of(TABLE_CACHE_CLEANUP_TIME);
                tableRefreshTtl = OptionalLong.of(TABLE_CACHE_REFRESH_TIME);
            }
        }

        dontVerifyCacheEntry = dontVerifyCache;
        databaseNamesCache = newCacheBuilder(expiresAfterWriteMillisDB, refreshMillsDB, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllDatabases), executor));

        databaseCache = newCacheBuilder(expiresAfterWriteMillisDB, refreshMillsDB, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadDatabase), executor));

        tableNamesCache = newCacheBuilder(expiresAfterWriteMillisDB, refreshMillsDB, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllTables), executor));

        viewNamesCache = newCacheBuilder(expiresAfterWriteMillisDB, refreshMillsDB, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllViews), executor));

        tableCache = newCacheBuilder(tableCacheTtl, tableRefreshTtl, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadTable), tableRefreshExecutor));

        partitionNamesCache = newCacheBuilder(tableCacheTtl, tableRefreshTtl, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadPartitionNames), tableRefreshExecutor));

        tableStatisticsCache = newCacheBuilder(expiresAfterWriteMillisTable, refreshMillsTable, maximumSize)
                .build(asyncReloading(new CacheLoader<WithIdentity<HiveTableName>, WithValidation<Table, PartitionStatistics>>()
                {
                    @Override
                    public WithValidation<Table, PartitionStatistics> load(WithIdentity<HiveTableName> key)
                    {
                        Table table = getExistingTable(key.getIdentity(), key.getKey().getDatabaseName(), key.getKey().getTableName());
                        PartitionStatistics ps = loadTableColumnStatistics(key, table);
                        Table validationParams = getCacheValidationParams(key.getIdentity(), table);
                        return new WithValidation<>(validationParams, ps);
                    }
                }, executor));

        partitionStatisticsCache = newCacheBuilder(expiresAfterWriteMillisTable, refreshMillsTable, maximumSize)
                .build(asyncReloading(new CacheLoader<WithIdentity<HivePartitionName>, WithValidation<Table, PartitionStatistics>>()
                {
                    @Override
                    public WithValidation<Table, PartitionStatistics> load(WithIdentity<HivePartitionName> key)
                    {
                        return loadPartitionColumnStatistics(key);
                    }

                    @Override
                    public Map<WithIdentity<HivePartitionName>, WithValidation<Table, PartitionStatistics>> loadAll(Iterable<? extends WithIdentity<HivePartitionName>> keys)
                    {
                        return loadPartitionColumnStatistics(keys);
                    }
                }, executor));

        partitionFilterCache = newCacheBuilder(expiresAfterWriteMillisTable, refreshMillsTable, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadPartitionNamesByParts), executor));

        partitionCache = newCacheBuilder(expiresAfterWriteMillisTable, refreshMillsTable, maximumSize)
                .build(asyncReloading(new CacheLoader<WithIdentity<HivePartitionName>, Optional<WithValidation<Table, Partition>>>()
                {
                    @Override
                    public Optional<WithValidation<Table, Partition>> load(WithIdentity<HivePartitionName> partitionName)
                    {
                        return loadPartitionByName(partitionName);
                    }

                    @Override
                    public Map<WithIdentity<HivePartitionName>, Optional<WithValidation<Table, Partition>>> loadAll(Iterable<? extends WithIdentity<HivePartitionName>> partitionNames)
                    {
                        return loadPartitionsByNames(partitionNames);
                    }
                }, executor));

        rolesCache = newCacheBuilder(expiresAfterWriteMillisDB, refreshMillsDB, maximumSize)
                .build(asyncReloading(CacheLoader.from(() -> loadRoles()), executor));

        roleGrantsCache = newCacheBuilder(expiresAfterWriteMillisDB, refreshMillsDB, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadRoleGrants), executor));

        configValuesCache = newCacheBuilder(expiresAfterWriteMillisDB, refreshMillsDB, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadConfigValue), executor));
    }

    @Override
    public void refreshMetastoreCache()
    {
        if (skipCache) {
            delegate.refreshMetastoreCache();
        }
        flushCache();
    }

    @Managed
    public void flushCache()
    {
        databaseNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
        viewNamesCache.invalidateAll();
        partitionNamesCache.invalidateAll();
        databaseCache.invalidateAll();
        tableCache.invalidateAll();
        partitionCache.invalidateAll();
        partitionFilterCache.invalidateAll();
        tableStatisticsCache.invalidateAll();
        partitionStatisticsCache.invalidateAll();
        rolesCache.invalidateAll();
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key)
    {
        try {
            return cache.getUnchecked(key);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private static <K, V> Map<K, V> getAll(LoadingCache<K, V> cache, Iterable<K> keys)
    {
        try {
            return cache.getAll(keys);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throwIfUnchecked(e);
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        if (skipCache) {
            return this.delegate.getDatabase(databaseName);
        }

        return get(databaseCache, databaseName);
    }

    private Optional<Database> loadDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public List<String> getAllDatabases()
    {
        if (skipCache) {
            return this.delegate.getAllDatabases();
        }

        return get(databaseNamesCache, "");
    }

    private List<String> loadAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    private Table getExistingTable(HiveIdentity identity, String databaseName, String tableName)
    {
        return getTable(identity, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    private Table getCacheValidationParams(HiveIdentity identity, String databaseName, String tableName)
    {
        if (dontVerifyCacheEntry) {
            return null;
        }

        Table table = getTable(identity, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));

        if (table.getPartitionColumns().size() > 0) {
            Table.Builder builder = Table.builder(table);
            getPartitionNames(identity, databaseName, tableName)
                    .ifPresent(list -> builder.setParameter("partitionNames", String.valueOf(list.hashCode())));

            return builder.build();
        }

        return table;
    }

    private Table getCacheValidationParams(HiveIdentity identity, Table table)
    {
        if (dontVerifyCacheEntry) {
            return null;
        }

        if (table.getPartitionColumns().size() > 0) {
            Table.Builder builder = Table.builder(table);
            getPartitionNames(identity, table.getDatabaseName(), table.getTableName())
                    .ifPresent(list -> builder.setParameter("partitionNames", String.valueOf(list.hashCode())));

            return builder.build();
        }

        return table;
    }

    private Table getCacheValidationPartitionParams(Table table, HiveBasicStatistics partition)
    {
        if (dontVerifyCacheEntry) {
            return null;
        }

        if (table.getPartitionColumns().size() > 0) {
            Table.Builder builder = Table.builder(table);
            builder.setParameter("partition::rowCount", partition.getRowCount().toString());
            builder.setParameter("partition::fileCount", partition.getFileCount().toString());
            builder.setParameter("partition::inMemSize", partition.getInMemoryDataSizeInBytes().toString());
            builder.setParameter("partition::onDiskSize", partition.getOnDiskDataSizeInBytes().toString());

            return builder.build();
        }

        return table;
    }

    @Override
    public Optional<Table> getTable(HiveIdentity inputIdentity, String databaseName, String tableName)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        if (skipTableCache) {
            return delegate.getTable(identity, databaseName, tableName);
        }

        return get(tableCache, new WithIdentity<>(identity, HiveTableName.hiveTableName(databaseName, tableName)));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return delegate.getSupportedColumnStatistics(type);
    }

    private Optional<Table> loadTable(WithIdentity<HiveTableName> hiveTableName)
    {
        Optional<Table> table = delegate.getTable(hiveTableName.getIdentity(), hiveTableName.getKey().getDatabaseName(), hiveTableName.getKey().getTableName());
        Map<String, Optional<List<String>>> map = tableNamesCache.asMap();
        String databaseName = hiveTableName.getKey().getDatabaseName();

        if (map.containsKey(databaseName)) {
            Optional<List<String>> allTables = map.get(databaseName);
            if (allTables.isPresent()) {
                /* New Table or Dropped table */
                if ((table.isPresent() && !allTables.get().contains(hiveTableName.getKey().getTableName()))
                        || (!table.isPresent() && allTables.get().contains(hiveTableName.getKey().getTableName()))) {
                    tableNamesCache.invalidate(databaseName);
                }
            }
        }
        return table;
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity identity, Table table)
    {
        if (skipTableCache) {
            return delegate.getTableStatistics(identity, table);
        }

        WithIdentity<HiveTableName> key = new WithIdentity<>(updateIdentity(identity), hiveTableName(table.getDatabaseName(), table.getTableName()));
        WithValidation<Table, PartitionStatistics> ps = get(tableStatisticsCache, key);

        /* Note: table object need not have partition info for validation; as it will come here for only non-partitioned tables*/
        if (dontVerifyCacheEntry || ps.matches(table)) {
            return ps.get();
        }

        tableStatisticsCache.invalidate(key);
        return get(tableStatisticsCache, key).get();
    }

    private PartitionStatistics loadTableColumnStatistics(WithIdentity<HiveTableName> hiveTableName, Table table)
    {
        return delegate.getTableStatistics(hiveTableName.getIdentity(), table);
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, Table table, List<Partition> partitionNames)
    {
        HiveTableName hiveTableName = hiveTableName(table.getDatabaseName(), table.getTableName());
        List<WithIdentity<HivePartitionName>> partitions = partitionNames.stream()
                .map(partition -> new WithIdentity<>(updateIdentity(identity), hivePartitionName(hiveTableName, makePartitionName(table, partition))))
                .collect(toImmutableList());

        if (skipTableCache) {
            HiveIdentity identity1 = updateIdentity(identity);
            return delegate.getPartitionStatistics(identity1, table, partitionNames);
        }

        Map<WithIdentity<HivePartitionName>, WithValidation<Table, PartitionStatistics>> statistics = getAll(partitionStatisticsCache, partitions);
        if (dontVerifyCacheEntry
                || statistics.size() == 0) {
            return statistics.entrySet()
                    .stream()
                    .collect(toImmutableMap(entry -> entry.getKey().getKey().getPartitionName().get(),
                            entry -> entry.getValue().get()));
        }

        Map<WithIdentity<HivePartitionName>, WithValidation<Table, PartitionStatistics>> finalStatistics = statistics;
        boolean allMatch = partitionNames.stream()
                .allMatch(partition -> finalStatistics.get(new WithIdentity<>(updateIdentity(identity), hivePartitionName(hiveTableName, makePartitionName(table, partition))))
                        .matches(getCacheValidationPartitionParams(table,
                                ThriftMetastoreUtil.getHiveBasicStatistics(partition.getParameters()))));
        if (allMatch) {
            return statistics.entrySet()
                    .stream()
                    .collect(toImmutableMap(entry -> entry.getKey().getKey().getPartitionName().get(),
                            entry -> entry.getValue().get()));
        }

        partitionCache.invalidate(partitions);
        partitionStatisticsCache.invalidate(partitions);
        statistics = getAll(partitionStatisticsCache, partitions);
        return statistics.entrySet()
                .stream()
                .collect(toImmutableMap(entry -> entry.getKey().getKey().getPartitionName().get(),
                        entry -> entry.getValue().get()));
    }

    private WithValidation<Table, PartitionStatistics> loadPartitionColumnStatistics(WithIdentity<HivePartitionName> partition)
    {
        HiveTableName hiveTableName = partition.getKey().getHiveTableName();
        HiveIdentity identity = partition.getIdentity();

        Table table = getExistingTable(identity, hiveTableName.getDatabaseName(), hiveTableName.getTableName());
        String partitionName = partition.getKey().getPartitionName().get();
        Map<String, PartitionStatistics> partitionStatistics = delegate.getPartitionStatistics(
                identity,
                table,
                ImmutableList.of(getExistingPartition(identity, table, partition.getKey().getPartitionValues())));
        if (!partitionStatistics.containsKey(partitionName)) {
            throw new PrestoException(HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY, "Statistics result does not contain entry for partition: " + partition.getKey().getPartitionName());
        }

        PartitionStatistics value = partitionStatistics.get(partitionName);
        return new WithValidation<>(getCacheValidationPartitionParams(table, value.getBasicStatistics()), value);
    }

    private Map<WithIdentity<HivePartitionName>, WithValidation<Table, PartitionStatistics>> loadPartitionColumnStatistics(Iterable<? extends WithIdentity<HivePartitionName>> keys)
    {
        SetMultimap<WithIdentity<HiveTableName>, WithIdentity<HivePartitionName>> tablePartitions = stream(keys)
                .collect(toImmutableSetMultimap(value -> new WithIdentity<>(value.getIdentity(), value.getKey().getHiveTableName()), key -> key));
        ImmutableMap.Builder<WithIdentity<HivePartitionName>, WithValidation<Table, PartitionStatistics>> result = ImmutableMap.builder();
        tablePartitions.keySet().forEach(tableName -> {
            Set<WithIdentity<HivePartitionName>> partitionNames = tablePartitions.get(tableName);
            Set<String> partitionNameStrings = partitionNames.stream()
                    .map(partitionName -> partitionName.getKey().getPartitionName().get())
                    .collect(toImmutableSet());
            Table table = getExistingTable(tableName.getIdentity(), tableName.getKey().getDatabaseName(), tableName.getKey().getTableName());
            List<Partition> partitions = getExistingPartitionsByNames(tableName.getIdentity(), table, ImmutableList.copyOf(partitionNameStrings));
            Map<String, PartitionStatistics> statisticsByPartitionName = delegate.getPartitionStatistics(tableName.getIdentity(), table, partitions);
            for (WithIdentity<HivePartitionName> partitionName : partitionNames) {
                String stringNameForPartition = partitionName.getKey().getPartitionName().get();
                PartitionStatistics value = statisticsByPartitionName.get(stringNameForPartition);
                if (value == null) {
                    throw new PrestoException(HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY, "Statistics result does not contain entry for partition: " + stringNameForPartition);
                }
                result.put(partitionName, new WithValidation<>(getCacheValidationPartitionParams(table, value.getBasicStatistics()), value));
            }
        });
        return result.build();
    }

    @Override
    public void updateTableStatistics(HiveIdentity inputIdentity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.updateTableStatistics(identity, databaseName, tableName, update);
        }
        finally {
            tableStatisticsCache.invalidate(new WithIdentity<>(identity, HiveTableName.hiveTableName(databaseName, tableName)));
            tableCache.invalidate(new WithIdentity<>(identity, HiveTableName.hiveTableName(databaseName, tableName)));
        }
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity inputIdentity, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.updatePartitionStatistics(identity, databaseName, tableName, partitionName, update);
        }
        finally {
            partitionStatisticsCache.invalidate(new WithIdentity<>(identity, HivePartitionName.hivePartitionName(databaseName, tableName, partitionName)));
            //statistics updated for partition itself in above call.
            partitionCache.invalidate(new WithIdentity<>(identity, HivePartitionName.hivePartitionName(databaseName, tableName, partitionName)));
            tableCache.invalidate(new WithIdentity<>(identity, HiveTableName.hiveTableName(databaseName, tableName)));
        }
    }

    @Override
    public void updatePartitionsStatistics(HiveIdentity identity, String databaseName, String tableName, Map<String, Function<PartitionStatistics, PartitionStatistics>> partNamesUpdateFunctionMap)
    {
        try {
            delegate.updatePartitionsStatistics(identity, databaseName, tableName, partNamesUpdateFunctionMap);
        }
        finally {
            partNamesUpdateFunctionMap.entrySet().stream().forEach(e -> {
                partitionStatisticsCache.invalidate(new WithIdentity<>(identity, HivePartitionName.hivePartitionName(databaseName, tableName, e.getKey())));
                //statistics updated for partition itself in above call.
                partitionCache.invalidate(new WithIdentity<>(identity, HivePartitionName.hivePartitionName(databaseName, tableName, e.getKey())));
            });
            tableCache.invalidate(new WithIdentity<>(identity, HiveTableName.hiveTableName(databaseName, tableName)));
        }
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        if (skipCache) {
            return delegate.getAllTables(databaseName);
        }
        return get(tableNamesCache, databaseName);
    }

    private Optional<List<String>> loadAllTables(String databaseName)
    {
        return delegate.getAllTables(databaseName);
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        if (skipCache) {
            return delegate.getAllViews(databaseName);
        }
        return get(viewNamesCache, databaseName);
    }

    private Optional<List<String>> loadAllViews(String databaseName)
    {
        return delegate.getAllViews(databaseName);
    }

    @Override
    public void createDatabase(HiveIdentity inputIdentity, Database database)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.createDatabase(identity, database);
        }
        finally {
            invalidateDatabase(database.getDatabaseName());
        }
    }

    @Override
    public void dropDatabase(HiveIdentity inputIdentity, String databaseName)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.dropDatabase(identity, databaseName);
        }
        finally {
            invalidateDatabase(databaseName);
        }
    }

    @Override
    public void renameDatabase(HiveIdentity inputIdentity, String databaseName, String newDatabaseName)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.renameDatabase(identity, databaseName, newDatabaseName);
        }
        finally {
            invalidateDatabase(databaseName);
            invalidateDatabase(newDatabaseName);
        }
    }

    protected void invalidateDatabase(String databaseName)
    {
        databaseCache.invalidate(databaseName);
        databaseNamesCache.invalidateAll();
    }

    @Override
    public void createTable(HiveIdentity inputIdentity, Table table, PrincipalPrivileges principalPrivileges)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.createTable(identity, table, principalPrivileges);
        }
        finally {
            invalidateTable(table.getDatabaseName(), table.getTableName());
        }
    }

    @Override
    public void dropTable(HiveIdentity inputIdentity, String databaseName, String tableName, boolean deleteData)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.dropTable(identity, databaseName, tableName, deleteData);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void replaceTable(HiveIdentity inputIdentity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.replaceTable(identity, databaseName, tableName, newTable, principalPrivileges);
        }
        finally {
            invalidateTable(databaseName, tableName);
            invalidateTable(newTable.getDatabaseName(), newTable.getTableName());
        }
    }

    @Override
    public void renameTable(HiveIdentity inputIdentity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.renameTable(identity, databaseName, tableName, newDatabaseName, newTableName);
        }
        finally {
            invalidateTable(databaseName, tableName);
            invalidateTable(newDatabaseName, newTableName);
        }
    }

    @Override
    public void commentTable(HiveIdentity inputIdentity, String databaseName, String tableName, Optional<String> comment)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.commentTable(identity, databaseName, tableName, comment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void addColumn(HiveIdentity inputIdentity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.addColumn(identity, databaseName, tableName, columnName, columnType, columnComment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void renameColumn(HiveIdentity inputIdentity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.renameColumn(identity, databaseName, tableName, oldColumnName, newColumnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void dropColumn(HiveIdentity inputIdentity, String databaseName, String tableName, String columnName)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.dropColumn(identity, databaseName, tableName, columnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    protected void invalidateTable(String databaseName, String tableName)
    {
        invalidateTableCache(databaseName, tableName);
        tableNamesCache.invalidate(databaseName);
        viewNamesCache.invalidate(databaseName);
        invalidateTableStatisticsCache(databaseName, tableName);
        invalidatePartitionCache(databaseName, tableName);
    }

    private Partition getExistingPartition(HiveIdentity identity, Table table, List<String> partitionValues)
    {
        return getPartition(identity, table.getDatabaseName(), table.getTableName(), partitionValues)
                .orElseThrow(() -> new PartitionNotFoundException(table.getSchemaTableName(), partitionValues));
    }

    private List<Partition> getExistingPartitionsByNames(HiveIdentity identity, Table table, List<String> partitionNames)
    {
        Map<String, Partition> partitions = getPartitionsByNames(identity, table.getDatabaseName(), table.getTableName(), partitionNames).entrySet().stream()
                .map(entry -> immutableEntry(entry.getKey(), entry.getValue().orElseThrow(() ->
                        new PartitionNotFoundException(table.getSchemaTableName(), extractPartitionValues(entry.getKey())))))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        return partitionNames.stream()
                .map(partitions::get)
                .collect(toImmutableList());
    }

    private void invalidateTableCache(String databaseName, String tableName)
    {
        tableCache.asMap().keySet().stream()
                .filter(table -> table.getKey().getDatabaseName().equals(databaseName) && table.getKey().getTableName().equals(tableName))
                .forEach(tableCache::invalidate);
    }

    private void invalidateTableStatisticsCache(String databaseName, String tableName)
    {
        tableStatisticsCache.asMap().keySet().stream()
                .filter(table -> table.getKey().getDatabaseName().equals(databaseName) && table.getKey().getTableName().equals(tableName))
                .forEach(tableStatisticsCache::invalidate);
    }

    @Override
    public Optional<Partition> getPartition(HiveIdentity inputIdentity, String databaseName, String tableName, List<String> partitionValues)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        if (skipTableCache) {
            return delegate.getPartition(identity, databaseName, tableName, partitionValues);
        }

        WithIdentity<HivePartitionName> name = new WithIdentity<>(identity, hivePartitionName(databaseName, tableName, partitionValues));
        Optional<WithValidation<Table, Partition>> partition = get(partitionCache, name);
        if (dontVerifyCacheEntry || !partition.isPresent()) {
            return partition.isPresent() ? Optional.of(partition.get().get()) : Optional.empty();
        }

        Table table = getCacheValidationParams(identity, databaseName, tableName);
        if (partition.get().matches(table)) {
            return Optional.of(partition.get().get());
        }

        partitionCache.invalidate(name);
        partition = get(partitionCache, name);
        return partition.isPresent() ? Optional.of(partition.get().get()) : Optional.empty();
    }

    @Override
    public Optional<List<String>> getPartitionNames(HiveIdentity inputIdentity, String databaseName, String tableName)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        if (skipTableCache) {
            return delegate.getPartitionNames(identity, databaseName, tableName);
        }

        WithIdentity<HiveTableName> key = new WithIdentity<>(identity, HiveTableName.hiveTableName(databaseName, tableName));
        return get(partitionNamesCache, key);
    }

    private Optional<List<String>> loadPartitionNames(WithIdentity<HiveTableName> hiveTableName)
    {
        return delegate.getPartitionNames(hiveTableName.getIdentity(), hiveTableName.getKey().getDatabaseName(), hiveTableName.getKey().getTableName());
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(HiveIdentity inputIdentity, String databaseName, String tableName, List<String> parts)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        if (skipTableCache) {
            return delegate.getPartitionNamesByParts(identity, databaseName, tableName, parts);
        }

        WithIdentity<PartitionFilter> key = new WithIdentity<>(identity, PartitionFilter.partitionFilter(databaseName, tableName, parts));
        Optional<WithValidation<Table, List<String>>> values = get(partitionFilterCache, key);

        if (dontVerifyCacheEntry || !values.isPresent()) {
            return values.isPresent() ? Optional.of(values.get().get()) : Optional.empty();
        }

        Table table = getCacheValidationParams(identity, databaseName, tableName);
        if (values.get().matches(table)) {
            return Optional.of(values.get().get());
        }

        partitionFilterCache.invalidate(key);
        values = get(partitionFilterCache, key);
        return values.isPresent() ? Optional.of(values.get().get()) : Optional.empty();
    }

    private Optional<WithValidation<Table, List<String>>> loadPartitionNamesByParts(WithIdentity<PartitionFilter> partitionFilter)
    {
        Optional<List<String>> result = delegate.getPartitionNamesByParts(
                partitionFilter.getIdentity(),
                partitionFilter.getKey().getHiveTableName().getDatabaseName(),
                partitionFilter.getKey().getHiveTableName().getTableName(),
                partitionFilter.getKey().getParts());

        if (result.isPresent()) {
            Table table = getCacheValidationParams(partitionFilter.getIdentity(),
                    partitionFilter.getKey().getHiveTableName().getDatabaseName(),
                    partitionFilter.getKey().getHiveTableName().getTableName());
            return Optional.of(new WithValidation<>(table, result.get()));
        }

        return Optional.empty();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, String databaseName, String tableName, List<String> partitionNames)
    {
        if (skipTableCache) {
            HiveIdentity identity1 = updateIdentity(identity);
            return delegate.getPartitionsByNames(identity1, databaseName, tableName, partitionNames);
        }

        Iterable<WithIdentity<HivePartitionName>> names = transform(partitionNames, name -> new WithIdentity<>(updateIdentity(identity), HivePartitionName.hivePartitionName(databaseName, tableName, name)));

        Map<WithIdentity<HivePartitionName>, Optional<WithValidation<Table, Partition>>> all = getAll(partitionCache, names);
        ImmutableMap.Builder<String, Optional<Partition>> partitionsByName = ImmutableMap.builder();

        if (dontVerifyCacheEntry || all.size() == 0) {
            return buildParitionsByName(all, partitionsByName);
        }

        Table table = getCacheValidationParams(identity, databaseName, tableName);
        if (Iterables.get(all.values(), 0).get().matches(table)) {
            return buildParitionsByName(all, partitionsByName);
        }

        partitionCache.invalidateAll(names);
        all = getAll(partitionCache, names);
        return buildParitionsByName(all, partitionsByName);
    }

    private Map<String, Optional<Partition>> buildParitionsByName(Map<WithIdentity<HivePartitionName>, Optional<WithValidation<Table, Partition>>> all,
                                                                  ImmutableMap.Builder<String, Optional<Partition>> partitionsByName)
    {
        for (Entry<WithIdentity<HivePartitionName>, Optional<WithValidation<Table, Partition>>> entry : all.entrySet()) {
            partitionsByName.put(entry.getKey().getKey().getPartitionName().get(),
                    entry.getValue().isPresent() ? Optional.of(entry.getValue().get().get()) : Optional.empty());
        }
        return partitionsByName.build();
    }

    private Optional<WithValidation<Table, Partition>> loadPartitionByName(WithIdentity<HivePartitionName> partitionName)
    {
        Optional<Partition> result = delegate.getPartition(
                partitionName.getIdentity(),
                partitionName.getKey().getHiveTableName().getDatabaseName(),
                partitionName.getKey().getHiveTableName().getTableName(),
                partitionName.getKey().getPartitionValues());

        if (result.isPresent()) {
            Table table = getCacheValidationParams(partitionName.getIdentity(),
                    partitionName.getKey().getHiveTableName().getDatabaseName(),
                    partitionName.getKey().getHiveTableName().getTableName());

            return Optional.of(new WithValidation<>(table, result.get()));
        }

        return Optional.empty();
    }

    private Map<WithIdentity<HivePartitionName>, Optional<WithValidation<Table, Partition>>> loadPartitionsByNames(Iterable<? extends WithIdentity<HivePartitionName>> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNames), "partitionNames is empty");

        WithIdentity<HivePartitionName> firstPartition = Iterables.get(partitionNames, 0);

        HiveTableName hiveTableName = firstPartition.getKey().getHiveTableName();
        HiveIdentity identity = updateIdentity(firstPartition.getIdentity());
        String databaseName = hiveTableName.getDatabaseName();
        String tableName = hiveTableName.getTableName();

        List<String> partitionsToFetch = new ArrayList<>();
        for (WithIdentity<HivePartitionName> partitionName : partitionNames) {
            checkArgument(partitionName.getKey().getHiveTableName().equals(hiveTableName), "Expected table name %s but got %s", hiveTableName, partitionName.getKey().getHiveTableName());
            partitionsToFetch.add(partitionName.getKey().getPartitionName().get());
        }

        ImmutableMap.Builder<WithIdentity<HivePartitionName>, Optional<WithValidation<Table, Partition>>> partitions = ImmutableMap.builder();
        Map<String, Optional<Partition>> partitionsByNames = delegate.getPartitionsByNames(identity, databaseName, tableName, partitionsToFetch);
        Table table = getCacheValidationParams(identity, databaseName, tableName);
        for (Entry<String, Optional<Partition>> entry : partitionsByNames.entrySet()) {
            partitions.put(new WithIdentity<>(identity, HivePartitionName.hivePartitionName(hiveTableName, entry.getKey())),
                    entry.getValue().isPresent() ? Optional.of(new WithValidation<>(table, entry.getValue().get())) : Optional.empty());
        }
        return partitions.build();
    }

    @Override
    public void addPartitions(HiveIdentity inputIdentity, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.addPartitions(identity, databaseName, tableName, partitions);
        }
        finally {
            // todo do we need to invalidate all partitions?
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void dropPartition(HiveIdentity inputIdentity, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.dropPartition(identity, databaseName, tableName, parts, deleteData);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void alterPartition(HiveIdentity inputIdentity, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        HiveIdentity identity = inputIdentity;
        identity = updateIdentity(identity);
        try {
            delegate.alterPartition(identity, databaseName, tableName, partition);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void createRole(String role, String grantor)
    {
        try {
            delegate.createRole(role, grantor);
        }
        finally {
            rolesCache.invalidateAll();
        }
    }

    @Override
    public void dropRole(String role)
    {
        try {
            delegate.dropRole(role);
        }
        finally {
            rolesCache.invalidateAll();
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public Set<String> listRoles()
    {
        if (skipCache) {
            return delegate.listRoles();
        }
        return get(rolesCache, "");
    }

    private Set<String> loadRoles()
    {
        return delegate.listRoles();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean withAdminOption, HivePrincipal grantor)
    {
        try {
            delegate.grantRoles(roles, grantees, withAdminOption, grantor);
        }
        finally {
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOptionFor, HivePrincipal grantor)
    {
        try {
            delegate.revokeRoles(roles, grantees, adminOptionFor, grantor);
        }
        finally {
            roleGrantsCache.invalidateAll();
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        if (skipCache) {
            return delegate.listRoleGrants(principal);
        }
        return get(roleGrantsCache, principal);
    }

    private Set<RoleGrant> loadRoleGrants(HivePrincipal principal)
    {
        return delegate.listRoleGrants(principal);
    }

    private void invalidatePartitionCache(String databaseName, String tableName)
    {
        HiveTableName hiveTableName = HiveTableName.hiveTableName(databaseName, tableName);
        partitionNamesCache.asMap().keySet().stream()
                .filter(partitionName -> partitionName.getKey().equals(hiveTableName))
                .forEach(partitionNamesCache::invalidate);
        partitionCache.asMap().keySet().stream()
                .filter(partitionName -> partitionName.getKey().getHiveTableName().equals(hiveTableName))
                .forEach(partitionCache::invalidate);
        partitionFilterCache.asMap().keySet().stream()
                .filter(partitionFilter -> partitionFilter.getKey().getHiveTableName().equals(hiveTableName))
                .forEach(partitionFilterCache::invalidate);
        partitionStatisticsCache.asMap().keySet().stream()
                .filter(partitionFilter -> partitionFilter.getKey().getHiveTableName().equals(hiveTableName))
                .forEach(partitionStatisticsCache::invalidate);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        delegate.grantTablePrivileges(databaseName, tableName, grantee, privileges);
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        delegate.revokeTablePrivileges(databaseName, tableName, grantee, privileges);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, HivePrincipal principal)
    {
        return delegate.listTablePrivileges(databaseName, tableName, principal);
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        if (skipCache) {
            return delegate.getConfigValue(name);
        }
        return get(configValuesCache, name);
    }

    private Optional<String> loadConfigValue(String name)
    {
        return delegate.getConfigValue(name);
    }

    @Override
    public long openTransaction(HiveIdentity identity)
    {
        return delegate.openTransaction(identity);
    }

    @Override
    public void commitTransaction(HiveIdentity identity, long transactionId)
    {
        delegate.commitTransaction(identity, transactionId);
    }

    @Override
    public void abortTransaction(HiveIdentity identity, long transactionId)
    {
        delegate.abortTransaction(identity, transactionId);
    }

    @Override
    public void sendTransactionHeartbeat(HiveIdentity identity, long transactionId)
    {
        delegate.sendTransactionHeartbeat(identity, transactionId);
    }

    @Override
    public void acquireSharedReadLock(HiveIdentity identity, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        delegate.acquireSharedReadLock(identity, queryId, transactionId, fullTables, partitions);
    }

    @Override
    public void acquireLock(HiveIdentity identity, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions, DataOperationType operationType)
    {
        delegate.acquireLock(identity, queryId, transactionId, fullTables, partitions, operationType);
    }

    @Override
    public String getValidWriteIds(HiveIdentity identity, List<SchemaTableName> tables, long currentTransactionId, boolean isVacuum)
    {
        return delegate.getValidWriteIds(identity, tables, currentTransactionId, isVacuum);
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest rqst)
    {
        return delegate.showLocks(rqst);
    }

    @Override
    public long getTableWriteId(String dbName, String tableName, long transactionId)
    {
        return delegate.getTableWriteId(dbName, tableName, transactionId);
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return delegate.isImpersonationEnabled();
    }

    public Set<HivePrivilegeInfo> loadTablePrivileges(String databaseName, String tableName, HivePrincipal principal)
    {
        return delegate.listTablePrivileges(databaseName, tableName, principal);
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(OptionalLong expiresAfterWriteMillis, OptionalLong refreshMillis, long maximumSize)
    {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteMillis.isPresent()) {
            cacheBuilder = cacheBuilder.expireAfterWrite(expiresAfterWriteMillis.getAsLong(), MILLISECONDS);
        }
        if (refreshMillis.isPresent() && (!expiresAfterWriteMillis.isPresent() || expiresAfterWriteMillis.getAsLong() > refreshMillis.getAsLong())) {
            cacheBuilder = cacheBuilder.refreshAfterWrite(refreshMillis.getAsLong(), MILLISECONDS);
        }
        cacheBuilder = cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    /**
     * list the privilege of db
     *
     * @param databaseName
     * @param principal
     * @return
     */
    @Override
    public Set<HivePrivilegeInfo> listSchemaPrivileges(String databaseName, String tableName,
                                                       HivePrincipal principal)
    {
        return delegate.listSchemaPrivileges(databaseName, tableName, principal);
    }

    /**
     * list the privilege of column
     *
     * @param databaseName
     * @param tableName
     * @param columnName
     * @param principal
     * @return
     */
    @Override
    public Set<HivePrivilegeInfo> listColumnPrivileges(String databaseName, String tableName, String columnName,
                                                       HivePrincipal principal)
    {
        return delegate.listColumnPrivileges(databaseName, tableName, columnName, principal);
    }

    private static class WithIdentity<T>
    {
        private final HiveIdentity identity;
        private final T key;

        public WithIdentity(HiveIdentity identity, T key)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.key = requireNonNull(key, "key is null");
        }

        public HiveIdentity getIdentity()
        {
            return identity;
        }

        public T getKey()
        {
            return key;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WithIdentity<?> other = (WithIdentity<?>) o;
            return Objects.equals(identity, other.identity) &&
                    Objects.equals(key, other.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(identity, key);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("identity", identity)
                    .add("key", key)
                    .toString();
        }
    }

    private HiveIdentity updateIdentity(HiveIdentity identity)
    {
        // remove identity if not doing impersonation
        return delegate.isImpersonationEnabled() ? identity : HiveIdentity.none();
    }

    private static class WithValidation<K, T>
    {
        private final K key;
        private final T payload;

        public WithValidation(K key, T payload)
        {
            this.key = key;
            this.payload = payload;
        }

        public T get()
        {
            return payload;
        }

        public boolean matches(K key)
        {
            if (this.key == null) {
                return true;
            }
            return Objects.equals(key, this.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            WithValidation that = (WithValidation) o;
            return Objects.equals(key, that.key);
        }
    }
}
