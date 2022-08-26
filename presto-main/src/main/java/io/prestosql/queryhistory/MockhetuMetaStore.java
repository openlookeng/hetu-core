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
package io.prestosql.queryhistory;

import io.prestosql.spi.favorite.FavoriteEntity;
import io.prestosql.spi.favorite.FavoriteResult;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;
import io.prestosql.spi.queryhistory.QueryHistoryEntity;
import io.prestosql.spi.queryhistory.QueryHistoryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MockhetuMetaStore
        implements HetuMetastore
{
    private static final MockhetuMetaStore instance = new MockhetuMetaStore();
    private final List<CatalogEntity> catalogs = new ArrayList<>();
    private final List<DatabaseEntity> databases = new ArrayList<>();
    private final List<TableEntity> tables = new ArrayList<>();

    private MockhetuMetaStore()
    {
    }

    public static MockhetuMetaStore getInstance()
    {
        return instance;
    }

    @Override
    public void createCatalog(CatalogEntity catalog)
    {
        catalogs.add(catalog);
    }

    @Override
    public void createCatalogIfNotExist(CatalogEntity catalog)
    {
        if (!catalogs.contains(catalog)) {
            createCatalog(catalog);
        }
    }

    @Override
    public void alterCatalog(String catalogName, CatalogEntity newCatalog)
    {
    }

    @Override
    public void dropCatalog(String catalogName)
    {
    }

    @Override
    public Optional<CatalogEntity> getCatalog(String catalogName)
    {
        if (catalogs.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(catalogs.get(0));
    }

    @Override
    public List<CatalogEntity> getCatalogs()
    {
        return catalogs;
    }

    @Override
    public void createDatabase(DatabaseEntity database)
    {
        databases.add(database);
    }

    @Override
    public void createDatabaseIfNotExist(DatabaseEntity database)
    {
        if (!databases.contains(database)) {
            createDatabase(database);
        }
    }

    @Override
    public void alterDatabase(String catalogName, String databaseName, DatabaseEntity newDatabase)
    {
    }

    @Override
    public void dropDatabase(String catalogName, String databaseName)
    {
    }

    @Override
    public Optional<DatabaseEntity> getDatabase(String catalogName, String databaseName)
    {
        if (databases.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(databases.get(0));
    }

    @Override
    public List<DatabaseEntity> getAllDatabases(String catalogName)
    {
        return databases;
    }

    @Override
    public void createTable(TableEntity table)
    {
        tables.add(table);
    }

    @Override
    public void createTableIfNotExist(TableEntity table)
    {
        if (!tables.contains(table)) {
            createTable(table);
        }
    }

    @Override
    public void dropTable(String catalogName, String databaseName, String tableName)
    {
        tables.removeIf(table -> table.getName().equals(tableName));
    }

    @Override
    public void alterTable(String catalogName, String databaseName, String oldTableName, TableEntity newTable)
    {
    }

    @Override
    public Optional<TableEntity> getTable(String catalogName, String databaseName, String table)
    {
        return tables.stream().filter(t -> t.getName().equals(table)).findFirst();
    }

    @Override
    public List<TableEntity> getAllTables(String catalogName, String databaseName)
    {
        return tables;
    }

    @Override
    public void alterCatalogParameter(String catalogName, String key, String value) {}

    @Override
    public void alterDatabaseParameter(String catalogName, String databaseName, String key, String value) {}

    @Override
    public void alterTableParameter(String catalogName, String databaseName, String tableName, String key, String value) {}

    @Override
    public void insertQueryHistory(QueryHistoryEntity queryHistoryEntity, String jsonString)
    {
    }

    @Override
    public void deleteQueryHistoryBatch()
    {
    }

    @Override
    public long getAllQueryHistoryNum()
    {
        return 0;
    }

    @Override
    public String getQueryDetail(String queryId)
    {
        return null;
    }

    @Override
    public QueryHistoryResult getQueryHistory(int startNum, int pageSize,
                                              String user, String startTime, String endTime,
                                              String queryId, String query, String resourceGroup,
                                              String resource, List<String> state, List<String> failed,
                                              String sort, String sortOrder)
    {
        return null;
    }

    @Override
    public void insertFavorite(FavoriteEntity favoriteEntity)
    {
    }

    @Override
    public void deleteFavorite(FavoriteEntity favoriteEntity)
    {
    }

    @Override
    public FavoriteResult getFavorite(int startNum, int pageSize, String user)
    {
        return null;
    }
}
