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
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.ForCachingHiveMetastore;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveErrorCode;
import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.PartitionNotFoundException;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
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
    protected final HiveMetastore delegate;
    private final LoadingCache<String, Optional<Database>> databaseCache;
    private final LoadingCache<String, List<String>> databaseNamesCache;
    private final LoadingCache<WithIdentity<HiveTableName>, Optional<Table>> tableCache;
    private final LoadingCache<String, Optional<List<String>>> tableNamesCache;
    private final LoadingCache<WithIdentity<HiveTableName>, PartitionStatistics> tableStatisticsCache;
    private final LoadingCache<WithIdentity<HivePartitionName>, PartitionStatistics> partitionStatisticsCache;
    private final LoadingCache<String, Optional<List<String>>> viewNamesCache;
    private final LoadingCache<WithIdentity<HivePartitionName>, Optional<Partition>> partitionCache;
    private final LoadingCache<WithIdentity<PartitionFilter>, Optional<List<String>>> partitionFilterCache;
    private final LoadingCache<WithIdentity<HiveTableName>, Optional<List<String>>> partitionNamesCache;
    private final LoadingCache<UserTableKey, Set<HivePrivilegeInfo>> tablePrivilegesCache;
    private final LoadingCache<String, Set<String>> rolesCache;
    private final LoadingCache<HivePrincipal, Set<RoleGrant>> roleGrantsCache;
    private final LoadingCache<String, Optional<String>> configValuesCache;
    private final LoadingCache<UserTableKey, Set<HivePrivilegeInfo>> coulumnPrivilegesCache;
    private final LoadingCache<UserTableKey, Set<HivePrivilegeInfo>> schemaPrivilegesCache;
    private final boolean skipCache;

    @Inject
    public CachingHiveMetastore(@ForCachingHiveMetastore HiveMetastore delegate, @ForCachingHiveMetastore Executor executor, HiveConfig hiveConfig, NodeManager nodeManager)
    {
        this(
                delegate,
                executor,
                hiveConfig.getMetastoreCacheTtl(),
                hiveConfig.getMetastoreRefreshInterval(),
                hiveConfig.getMetastoreCacheMaximumSize(),
                !(nodeManager.getCurrentNode().isCoordinator() || hiveConfig.getWorkerMetaStoreCacheEnabled()));
    }

    public CachingHiveMetastore(HiveMetastore delegate, Executor executor, Duration cacheTtl, Duration refreshInterval, long maximumSize, boolean skipCache)
    {
        this(
                delegate,
                executor,
                OptionalLong.of(cacheTtl.toMillis()),
                refreshInterval.toMillis() >= cacheTtl.toMillis() ? OptionalLong.empty() : OptionalLong.of(refreshInterval.toMillis()),
                maximumSize,
                skipCache);
    }

    public static CachingHiveMetastore memoizeMetastore(HiveMetastore delegate, long maximumSize)
    {
        return new CachingHiveMetastore(
                delegate,
                newDirectExecutorService(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                maximumSize,
                false);
    }

    private CachingHiveMetastore(HiveMetastore delegate, Executor executor, OptionalLong expiresAfterWriteMillis, OptionalLong refreshMills, long maximumSize, boolean skipCache)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(executor, "executor is null");

        this.skipCache = skipCache;

        databaseNamesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllDatabases), executor));

        databaseCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadDatabase), executor));

        tableNamesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllTables), executor));

        tableStatisticsCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<WithIdentity<HiveTableName>, PartitionStatistics>()
                {
                    @Override
                    public PartitionStatistics load(WithIdentity<HiveTableName> key)
                    {
                        return loadTableColumnStatistics(key);
                    }
                }, executor));

        partitionStatisticsCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<WithIdentity<HivePartitionName>, PartitionStatistics>()
                {
                    @Override
                    public PartitionStatistics load(WithIdentity<HivePartitionName> key)
                    {
                        return loadPartitionColumnStatistics(key);
                    }

                    @Override
                    public Map<WithIdentity<HivePartitionName>, PartitionStatistics> loadAll(Iterable<? extends WithIdentity<HivePartitionName>> keys)
                    {
                        return loadPartitionColumnStatistics(keys);
                    }
                }, executor));

        tableCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadTable), executor));

        viewNamesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadAllViews), executor));

        partitionNamesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadPartitionNames), executor));

        partitionFilterCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadPartitionNamesByParts), executor));

        partitionCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(new CacheLoader<WithIdentity<HivePartitionName>, Optional<Partition>>()
                {
                    @Override
                    public Optional<Partition> load(WithIdentity<HivePartitionName> partitionName)
                    {
                        return loadPartitionByName(partitionName);
                    }

                    @Override
                    public Map<WithIdentity<HivePartitionName>, Optional<Partition>> loadAll(Iterable<? extends WithIdentity<HivePartitionName>> partitionNames)
                    {
                        return loadPartitionsByNames(partitionNames);
                    }
                }, executor));

        tablePrivilegesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(key -> loadTablePrivileges(key.getDatabase(), key.getTable(), key.getPrincipal())), executor));

        rolesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(() -> loadRoles()), executor));

        roleGrantsCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadRoleGrants), executor));

        configValuesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadConfigValue), executor));

        coulumnPrivilegesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(key -> loadColumnPrivileges(key.getDatabase(), key.getTable(),
                        key.getColumn(), key.getPrincipal())), executor));
        schemaPrivilegesCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(key -> loadSchemaPrivileges(key.getDatabase(), key.getTable(),
                        key.getPrincipal())), executor));
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
        tablePrivilegesCache.invalidateAll();
        tableStatisticsCache.invalidateAll();
        partitionStatisticsCache.invalidateAll();
        rolesCache.invalidateAll();
        coulumnPrivilegesCache.invalidateAll();
        schemaPrivilegesCache.invalidateAll();
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

    @Override
    public Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        identity = updateIdentity(identity);
        if (skipCache) {
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
        return delegate.getTable(hiveTableName.getIdentity(), hiveTableName.getKey().getDatabaseName(), hiveTableName.getKey().getTableName());
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity identity, Table table)
    {
        if (skipCache) {
            return delegate.getTableStatistics(identity, table);
        }
        return get(tableStatisticsCache, new WithIdentity<>(updateIdentity(identity), hiveTableName(table.getDatabaseName(), table.getTableName())));
    }

    private PartitionStatistics loadTableColumnStatistics(WithIdentity<HiveTableName> hiveTableName)
    {
        Table table = getExistingTable(hiveTableName.getIdentity(), hiveTableName.getKey().getDatabaseName(), hiveTableName.getKey().getTableName());
        return delegate.getTableStatistics(hiveTableName.getIdentity(), table);
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, Table table, List<Partition> partitionNames)
    {
        HiveTableName hiveTableName = hiveTableName(table.getDatabaseName(), table.getTableName());
        List<WithIdentity<HivePartitionName>> partitions = partitionNames.stream()
                .map(partition -> new WithIdentity<>(updateIdentity(identity), hivePartitionName(hiveTableName, makePartitionName(table, partition))))
                .collect(toImmutableList());

        if (skipCache) {
            return loadPartitionColumnStatistics(partitions).entrySet()
                    .stream()
                    .collect(toImmutableMap(entry -> entry.getKey().getKey().getPartitionName().get(), Entry::getValue));
        }

        Map<WithIdentity<HivePartitionName>, PartitionStatistics> statistics = getAll(partitionStatisticsCache, partitions);
        return statistics.entrySet()
                .stream()
                .collect(toImmutableMap(entry -> entry.getKey().getKey().getPartitionName().get(), Entry::getValue));
    }

    private PartitionStatistics loadPartitionColumnStatistics(WithIdentity<HivePartitionName> partition)
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
        return partitionStatistics.get(partitionName);
    }

    private Map<WithIdentity<HivePartitionName>, PartitionStatistics> loadPartitionColumnStatistics(Iterable<? extends WithIdentity<HivePartitionName>> keys)
    {
        SetMultimap<WithIdentity<HiveTableName>, WithIdentity<HivePartitionName>> tablePartitions = stream(keys)
                .collect(toImmutableSetMultimap(value -> new WithIdentity<>(value.getIdentity(), value.getKey().getHiveTableName()), key -> key));
        ImmutableMap.Builder<WithIdentity<HivePartitionName>, PartitionStatistics> result = ImmutableMap.builder();
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
                result.put(partitionName, value);
            }
        });
        return result.build();
    }

    @Override
    public void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
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
    public void updatePartitionStatistics(HiveIdentity identity, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
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
    public void updatePartitionsStatistics(HiveIdentity identity, String databaseName, String tableName, List<String> partitionNames, List<Function<PartitionStatistics, PartitionStatistics>> updateFunctionList)
    {
        try {
            delegate.updatePartitionsStatistics(identity, databaseName, tableName, partitionNames, updateFunctionList);
        }
        finally {
            for (int i = 0; i < partitionNames.size(); i++) {
                partitionStatisticsCache.invalidate(new WithIdentity<>(identity, HivePartitionName.hivePartitionName(databaseName, tableName, partitionNames.get(i))));
                //statistics updated for partition itself in above call.
                partitionCache.invalidate(new WithIdentity<>(identity, HivePartitionName.hivePartitionName(databaseName, tableName, partitionNames.get(i))));
            }
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
    public void createDatabase(HiveIdentity identity, Database database)
    {
        identity = updateIdentity(identity);
        try {
            delegate.createDatabase(identity, database);
        }
        finally {
            invalidateDatabase(database.getDatabaseName());
        }
    }

    @Override
    public void dropDatabase(HiveIdentity identity, String databaseName)
    {
        identity = updateIdentity(identity);
        try {
            delegate.dropDatabase(identity, databaseName);
        }
        finally {
            invalidateDatabase(databaseName);
        }
    }

    @Override
    public void renameDatabase(HiveIdentity identity, String databaseName, String newDatabaseName)
    {
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
    public void createTable(HiveIdentity identity, Table table, PrincipalPrivileges principalPrivileges)
    {
        identity = updateIdentity(identity);
        try {
            delegate.createTable(identity, table, principalPrivileges);
        }
        finally {
            invalidateTable(table.getDatabaseName(), table.getTableName());
        }
    }

    @Override
    public void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData)
    {
        identity = updateIdentity(identity);
        try {
            delegate.dropTable(identity, databaseName, tableName, deleteData);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void replaceTable(HiveIdentity identity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
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
    public void renameTable(HiveIdentity identity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
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
    public void commentTable(HiveIdentity identity, String databaseName, String tableName, Optional<String> comment)
    {
        identity = updateIdentity(identity);
        try {
            delegate.commentTable(identity, databaseName, tableName, comment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void addColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        identity = updateIdentity(identity);
        try {
            delegate.addColumn(identity, databaseName, tableName, columnName, columnType, columnComment);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void renameColumn(HiveIdentity identity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        identity = updateIdentity(identity);
        try {
            delegate.renameColumn(identity, databaseName, tableName, oldColumnName, newColumnName);
        }
        finally {
            invalidateTable(databaseName, tableName);
        }
    }

    @Override
    public void dropColumn(HiveIdentity identity, String databaseName, String tableName, String columnName)
    {
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
        tablePrivilegesCache.asMap().keySet().stream()
                .filter(userTableKey -> userTableKey.matches(databaseName, tableName))
                .forEach(tablePrivilegesCache::invalidate);
        invalidateTableStatisticsCache(databaseName, tableName);
        invalidatePartitionCache(databaseName, tableName);
        coulumnPrivilegesCache.asMap().keySet().stream()
                .filter(UserTableKey -> UserTableKey.matches(databaseName, tableName))
                .forEach(coulumnPrivilegesCache::invalidate);
        schemaPrivilegesCache.asMap().keySet().stream()
                .filter(UserTableKey -> UserTableKey.matches(databaseName, tableName))
                .forEach(schemaPrivilegesCache::invalidate);
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
    public Optional<Partition> getPartition(HiveIdentity identity, String databaseName, String tableName, List<String> partitionValues)
    {
        identity = updateIdentity(identity);
        if (skipCache) {
            return delegate.getPartition(identity, databaseName, tableName, partitionValues);
        }

        WithIdentity<HivePartitionName> name = new WithIdentity<>(identity, hivePartitionName(databaseName, tableName, partitionValues));
        return get(partitionCache, name);
    }

    @Override
    public Optional<List<String>> getPartitionNames(HiveIdentity identity, String databaseName, String tableName)
    {
        identity = updateIdentity(identity);
        if (skipCache) {
            return delegate.getPartitionNames(identity, databaseName, tableName);
        }
        return get(partitionNamesCache, new WithIdentity<>(identity, HiveTableName.hiveTableName(databaseName, tableName)));
    }

    private Optional<List<String>> loadPartitionNames(WithIdentity<HiveTableName> hiveTableName)
    {
        return delegate.getPartitionNames(hiveTableName.getIdentity(), hiveTableName.getKey().getDatabaseName(), hiveTableName.getKey().getTableName());
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(HiveIdentity identity, String databaseName, String tableName, List<String> parts)
    {
        identity = updateIdentity(identity);
        if (skipCache) {
            return delegate.getPartitionNamesByParts(identity, databaseName, tableName, parts);
        }

        return get(partitionFilterCache, new WithIdentity<>(identity, PartitionFilter.partitionFilter(databaseName, tableName, parts)));
    }

    private Optional<List<String>> loadPartitionNamesByParts(WithIdentity<PartitionFilter> partitionFilter)
    {
        return delegate.getPartitionNamesByParts(
                partitionFilter.getIdentity(),
                partitionFilter.getKey().getHiveTableName().getDatabaseName(),
                partitionFilter.getKey().getHiveTableName().getTableName(),
                partitionFilter.getKey().getParts());
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, String databaseName, String tableName, List<String> partitionNames)
    {
        if (skipCache) {
            HiveIdentity identity1 = updateIdentity(identity);
            return delegate.getPartitionsByNames(identity1, databaseName, tableName, partitionNames);
        }

        Iterable<WithIdentity<HivePartitionName>> names = transform(partitionNames, name -> new WithIdentity<>(updateIdentity(identity), HivePartitionName.hivePartitionName(databaseName, tableName, name)));

        Map<WithIdentity<HivePartitionName>, Optional<Partition>> all = getAll(partitionCache, names);
        ImmutableMap.Builder<String, Optional<Partition>> partitionsByName = ImmutableMap.builder();
        for (Entry<WithIdentity<HivePartitionName>, Optional<Partition>> entry : all.entrySet()) {
            partitionsByName.put(entry.getKey().getKey().getPartitionName().get(), entry.getValue());
        }
        return partitionsByName.build();
    }

    private Optional<Partition> loadPartitionByName(WithIdentity<HivePartitionName> partitionName)
    {
        return delegate.getPartition(
                partitionName.getIdentity(),
                partitionName.getKey().getHiveTableName().getDatabaseName(),
                partitionName.getKey().getHiveTableName().getTableName(),
                partitionName.getKey().getPartitionValues());
    }

    private Map<WithIdentity<HivePartitionName>, Optional<Partition>> loadPartitionsByNames(Iterable<? extends WithIdentity<HivePartitionName>> partitionNames)
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

        ImmutableMap.Builder<WithIdentity<HivePartitionName>, Optional<Partition>> partitions = ImmutableMap.builder();
        Map<String, Optional<Partition>> partitionsByNames = delegate.getPartitionsByNames(identity, databaseName, tableName, partitionsToFetch);
        for (Entry<String, Optional<Partition>> entry : partitionsByNames.entrySet()) {
            partitions.put(new WithIdentity<>(identity, HivePartitionName.hivePartitionName(hiveTableName, entry.getKey())), entry.getValue());
        }
        return partitions.build();
    }

    @Override
    public void addPartitions(HiveIdentity identity, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
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
    public void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        identity = updateIdentity(identity);
        try {
            delegate.dropPartition(identity, databaseName, tableName, parts, deleteData);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void alterPartition(HiveIdentity identity, String databaseName, String tableName, PartitionWithStatistics partition)
    {
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
        try {
            delegate.grantTablePrivileges(databaseName, tableName, grantee, privileges);
        }
        finally {
            tablePrivilegesCache.invalidate(new UserTableKey(grantee, databaseName, tableName));
        }
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            delegate.revokeTablePrivileges(databaseName, tableName, grantee, privileges);
        }
        finally {
            tablePrivilegesCache.invalidate(new UserTableKey(grantee, databaseName, tableName));
        }
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, HivePrincipal principal)
    {
        if (skipCache) {
            return delegate.listTablePrivileges(databaseName, tableName, principal);
        }
        return get(tablePrivilegesCache, new UserTableKey(principal, databaseName, tableName));
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
     * load the privilege from cache
     *
     * @param databaseName databaseName
     * @param tableName tableName
     * @param columnName columnName
     * @param principal principal
     * @return Set HivePrivilegeInfo
     */
    public Set<HivePrivilegeInfo> loadColumnPrivileges(String databaseName, String tableName, String columnName,
                                                       HivePrincipal principal)
    {
        return delegate.listColumnPrivileges(databaseName, tableName, columnName, principal);
    }

    /**
     * load the privilege of schemas from cache
     *
     * @param databaseName databaseName
     * @param tableName tableName
     * @param principal principal
     * @return Set HivePrivilegeInfo
     */
    public Set<HivePrivilegeInfo> loadSchemaPrivileges(String databaseName, String tableName,
                                                       HivePrincipal principal)
    {
        return delegate.listSchemaPrivileges(databaseName, tableName, principal);
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
        if (skipCache) {
            return delegate.listSchemaPrivileges(databaseName, tableName, principal);
        }

        return get(schemaPrivilegesCache, new UserTableKey(principal, databaseName, tableName));
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
        if (skipCache) {
            return delegate.listColumnPrivileges(databaseName, tableName, columnName, principal);
        }

        return get(coulumnPrivilegesCache, new UserTableKey(principal, databaseName, tableName, columnName));
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
}
