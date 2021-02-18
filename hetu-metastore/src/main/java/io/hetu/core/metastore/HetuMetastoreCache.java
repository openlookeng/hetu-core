/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.metastore;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.airlift.log.Logger;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;

import javax.inject.Inject;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class HetuMetastoreCache
        implements HetuMetastore
{
    private static final Logger log = Logger.get(HetuMetastoreCache.class);

    private HetuMetastore delegate;

    private Cache<String, Optional<CatalogEntity>> catalogCache;
    private Cache<String, List<CatalogEntity>> catalogsCache;
    private Cache<String, Optional<DatabaseEntity>> databaseCache;
    private Cache<String, List<DatabaseEntity>> databasesCache;
    private Cache<String, Optional<TableEntity>> tableCache;
    private Cache<String, List<TableEntity>> tablesCache;

    @Inject
    public HetuMetastoreCache(@ForHetuMetastoreCache HetuMetastore delegate, HetuMetastoreCacheConfig hetuMetastoreCacheConfig)
    {
        this.delegate = delegate;

        buildMetaStoreCache(hetuMetastoreCacheConfig.getMetaStoreCacheMaxSize(),
                Duration.ofMillis(hetuMetastoreCacheConfig.getMetaStoreCacheTtl().toMillis()));
    }

    @Override
    public void createCatalog(CatalogEntity catalog)
    {
        try {
            delegate.createCatalog(catalog);
        }
        finally {
            catalogsCache.invalidateAll();
        }
    }

    @Override
    public void createCatalogIfNotExist(CatalogEntity catalog)
    {
        try {
            delegate.createCatalogIfNotExist(catalog);
        }
        finally {
            catalogsCache.invalidateAll();
        }
    }

    @Override
    public void alterCatalog(String catalogName, CatalogEntity newCatalog)
    {
        try {
            delegate.alterCatalog(catalogName, newCatalog);
        }
        finally {
            catalogCache.invalidate(catalogName);
            catalogsCache.invalidateAll();
        }
    }

    @Override
    public void dropCatalog(String catalogName)
    {
        try {
            delegate.dropCatalog(catalogName);
        }
        finally {
            catalogCache.invalidate(catalogName);
            catalogsCache.invalidateAll();
        }
    }

    @Override
    public Optional<CatalogEntity> getCatalog(String catalogName)
    {
        try {
            return catalogCache.get(catalogName, () -> {
                return delegate.getCatalog(catalogName);
            });
        }
        catch (ExecutionException executionException) {
            log.debug(executionException.getCause(),
                    String.format("Error while caching catalog[%s] metadata. Falling back to default flow", catalogName));
            return delegate.getCatalog(catalogName);
        }
    }

    @Override
    public List<CatalogEntity> getCatalogs()
    {
        try {
            return catalogsCache.get("", () -> {
                return delegate.getCatalogs();
            });
        }
        catch (ExecutionException executionException) {
            log.debug(executionException.getCause(),
                    "Error while caching all catalogs metadata. Falling back to default flow");
            return delegate.getCatalogs();
        }
    }

    @Override
    public void createDatabase(DatabaseEntity database)
    {
        try {
            delegate.createDatabase(database);
        }
        finally {
            databasesCache.invalidate(database.getCatalogName());
        }
    }

    @Override
    public void createDatabaseIfNotExist(DatabaseEntity database)
    {
        try {
            delegate.createDatabaseIfNotExist(database);
        }
        finally {
            databasesCache.invalidate(database.getCatalogName());
        }
    }

    @Override
    public void alterDatabase(String catalogName, String databaseName, DatabaseEntity newDatabase)
    {
        try {
            delegate.alterDatabase(catalogName, databaseName, newDatabase);
        }
        finally {
            String key = catalogName + "." + databaseName;
            databaseCache.invalidate(key);
            databasesCache.invalidate(catalogName);
        }
    }

    @Override
    public void dropDatabase(String catalogName, String databaseName)
    {
        try {
            delegate.dropDatabase(catalogName, databaseName);
        }
        finally {
            String key = catalogName + "." + databaseName;
            databaseCache.invalidate(key);
            databasesCache.invalidate(catalogName);
        }
    }

    @Override
    public Optional<DatabaseEntity> getDatabase(String catalogName, String databaseName)
    {
        try {
            String key = catalogName + "." + databaseName;
            return databaseCache.get(key, () -> {
                return delegate.getDatabase(catalogName, databaseName);
            });
        }
        catch (ExecutionException executionException) {
            log.debug(executionException.getCause(),
                    String.format("Error while caching database[%s.%s] metadata. Falling back to default flow"), catalogName, databaseName);
            return delegate.getDatabase(catalogName, databaseName);
        }
    }

    @Override
    public List<DatabaseEntity> getAllDatabases(String catalogName)
    {
        try {
            return databasesCache.get(catalogName, () -> {
                return delegate.getAllDatabases(catalogName);
            });
        }
        catch (ExecutionException executionException) {
            log.debug(executionException.getCause(),
                    String.format("Error while caching all databases metadata in catalog[%s]. Falling back to default flow", catalogName));
            return delegate.getAllDatabases(catalogName);
        }
    }

    @Override
    public void createTable(TableEntity table)
    {
        try {
            delegate.createTable(table);
        }
        finally {
            String key = table.getCatalogName() + "." + table.getDatabaseName();
            tablesCache.invalidate(key);
        }
    }

    @Override
    public void createTableIfNotExist(TableEntity table)
    {
        try {
            delegate.createTableIfNotExist(table);
        }
        finally {
            String key = table.getCatalogName() + "." + table.getDatabaseName();
            tablesCache.invalidate(key);
        }
    }

    @Override
    public void dropTable(String catalogName, String databaseName, String tableName)
    {
        try {
            delegate.dropTable(catalogName, databaseName, tableName);
        }
        finally {
            String databaseKey = catalogName + '.' + databaseName;
            String tableKey = catalogName + '.' + databaseName + '.' + tableName;
            tableCache.invalidate(tableKey);
            tablesCache.invalidate(databaseKey);
        }
    }

    @Override
    public void alterTable(String catalogName, String databaseName, String oldTableName, TableEntity newTable)
    {
        try {
            delegate.alterTable(catalogName, databaseName, oldTableName, newTable);
        }
        finally {
            String databaseKey = catalogName + '.' + databaseName;
            String tableKey = catalogName + "." + databaseName + "." + oldTableName;
            tableCache.invalidate(tableKey);
            tablesCache.invalidate(databaseKey);
        }
    }

    @Override
    public Optional<TableEntity> getTable(String catalogName, String databaseName, String tableName)
    {
        try {
            String key = catalogName + '.' + databaseName + '.' + tableName;
            return tableCache.get(key, () -> {
                return delegate.getTable(catalogName, databaseName, tableName);
            });
        }
        catch (ExecutionException executionException) {
            log.debug(executionException.getCause(),
                    String.format("Error while caching table[%s.%s.%s] metadata. Falling back to default flow", catalogName, databaseName, tableName));
            return delegate.getTable(catalogName, databaseName, tableName);
        }
    }

    @Override
    public List<TableEntity> getAllTables(String catalogName, String databaseName)
    {
        try {
            String key = catalogName + "." + databaseName;
            return tablesCache.get(key, () -> {
                return delegate.getAllTables(catalogName, databaseName);
            });
        }
        catch (ExecutionException executionException) {
            log.debug(executionException.getCause(),
                    String.format("Error while caching all tables metadata in %s.%s. Falling back to default flow"), catalogName, databaseName);
            return delegate.getAllTables(catalogName, databaseName);
        }
    }

    private void buildMetaStoreCache(long maximumSize, Duration ttl)
    {
        catalogCache = CacheBuilder.newBuilder().maximumSize(maximumSize).expireAfterAccess(ttl).build();
        catalogsCache = CacheBuilder.newBuilder().maximumSize(maximumSize).expireAfterAccess(ttl).build();
        databaseCache = CacheBuilder.newBuilder().maximumSize(maximumSize).expireAfterAccess(ttl).build();
        databasesCache = CacheBuilder.newBuilder().maximumSize(maximumSize).expireAfterAccess(ttl).build();
        tableCache = CacheBuilder.newBuilder().maximumSize(maximumSize).expireAfterAccess(ttl).build();
        tablesCache = CacheBuilder.newBuilder().maximumSize(maximumSize).expireAfterAccess(ttl).build();
    }
}
