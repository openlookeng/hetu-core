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
package io.hetu.core.metastore.jdbc;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.ColumnEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.hetu.core.metastore.jdbc.JdbcMetadataUtil.onDemand;
import static io.hetu.core.metastore.jdbc.JdbcMetadataUtil.runTransaction;
import static io.hetu.core.metastore.jdbc.JdbcMetadataUtil.runTransactionWithLock;
import static io.prestosql.spi.metastore.HetuErrorCode.HETU_METASTORE_CODE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * jdbc hetu metastore
 *
 * @since 2020-02-27
 */
public class JdbcHetuMetastore
        implements HetuMetastore
{
    private static final String DATABASE_ERROR_MESSAGE = "Database '%s.%s' does not exist:";
    private static final String CATALOG_ERROR_MESSAGE = "Catalog '%s' does not exist:";
    private final Jdbi jdbi;
    private final JdbcMetadataDao dao;

    /**
     * jdbc hetu metastore
     *
     * @param config config
     */
    @Inject
    public JdbcHetuMetastore(JdbcMetastoreConfig config)
    {
        requireNonNull(config, "jdbc metastore config is null");
        jdbi = Jdbi.create(config.getDbUrl(), config.getDbUser(), config.getDbPassword())
                .installPlugin(new SqlObjectPlugin());
        JdbcMetadataUtil.createTablesWithRetry(jdbi);
        this.dao = onDemand(jdbi, JdbcMetadataDao.class);
    }

    @Override
    public synchronized void createCatalog(CatalogEntity catalog)
    {
        runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            long catalogId = transactionDao.insertCatalog(catalog);
            Optional<List<PropertyEntity>> properties = mapToList(catalog.getParameters());
            properties.ifPresent(parameters -> transactionDao.insertCatalogProperty(catalogId, parameters));
        });
    }

    @Override
    public synchronized void createCatalogIfNotExist(CatalogEntity catalog)
    {
        try {
            createCatalog(catalog);
        }
        catch (PrestoException e) {
            Optional<CatalogEntity> existedCatalog = getCatalog(catalog.getName());
            if (!(existedCatalog.isPresent() && existedCatalog.get().equals(catalog))) {
                throw new PrestoException(HETU_METASTORE_CODE, e.getMessage(), e.getCause());
            }
        }
    }

    @Override
    public synchronized void alterCatalog(String catalogName, CatalogEntity newCatalog)
    {
        if (!catalogName.equals(newCatalog.getName())) {
            throw new PrestoException(HETU_METASTORE_CODE, "Cannot alter a catalog's name");
        }

        runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);

            Long catalogId = getCatalogId(transactionDao, catalogName);

            CatalogEntity catalog = CatalogEntity.builder(newCatalog).build();
            transactionDao.dropCatalogProperty(catalogId);
            Optional<List<PropertyEntity>> properties = mapToList(catalog.getParameters());
            properties.ifPresent(parameters -> transactionDao.insertCatalogProperty(catalogId, parameters));

            transactionDao.alterCatalog(catalogName, catalog);
        });
    }

    @Override
    public synchronized void dropCatalog(String catalogName)
    {
        runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);

            Long catalogId = getCatalogId(transactionDao, catalogName);
            transactionDao.dropCatalogProperty(catalogId);
            transactionDao.dropCatalog(catalogName);
        });
    }

    private synchronized Long getCatalogId(JdbcMetadataDao metadataDao, String catalogName)
    {
        Long catalogId = metadataDao.getCatalogId(catalogName);
        if (catalogId == null) {
            throw new PrestoException(HETU_METASTORE_CODE, format(CATALOG_ERROR_MESSAGE, catalogName));
        }

        return catalogId;
    }

    private synchronized void checkCatalogExists(JdbcMetadataDao metadataDao, String catalogName)
    {
        getCatalogId(metadataDao, catalogName);
    }

    @Override
    public synchronized Optional<CatalogEntity> getCatalog(String catalogName)
    {
        return dao.getCatalog(catalogName);
    }

    @Override
    public synchronized List<CatalogEntity> getCatalogs()
    {
        return dao.getAllCatalogs();
    }

    @Override
    public synchronized void createDatabase(DatabaseEntity database)
    {
        runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            checkCatalogExists(transactionDao, database.getCatalogName());
            long databaseId = transactionDao.insertDatabase(database);
            Optional<List<PropertyEntity>> properties = mapToList(database.getParameters());
            properties.ifPresent(parameters -> transactionDao.insertDatabaseProperty(databaseId, parameters));
        });
    }

    @Override
    public synchronized void createDatabaseIfNotExist(DatabaseEntity database)
    {
        try {
            createDatabase(database);
        }
        catch (PrestoException e) {
            Optional<DatabaseEntity> existedDatabase = getDatabase(database.getCatalogName(), database.getName());
            if (!(existedDatabase.isPresent() && existedDatabase.get().equals(database))) {
                throw new PrestoException(HETU_METASTORE_CODE, e.getMessage(), e.getCause());
            }
        }
    }

    @Override
    public synchronized void alterDatabase(String catalogName, String databaseName, DatabaseEntity newDatabase)
    {
        if (!catalogName.equals(newDatabase.getCatalogName())) {
            throw new PrestoException(HETU_METASTORE_CODE,
                    format("alter database cannot cross catalog[%s,%s]", catalogName, newDatabase.getCatalogName()));
        }

        runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);

            Long databaseId = transactionDao.getDatabaseId(catalogName, databaseName);
            DatabaseEntity database = DatabaseEntity.builder(newDatabase).build();

            transactionDao.dropDatabaseProperty(databaseId);
            Optional<List<PropertyEntity>> properties = mapToList(database.getParameters());
            properties.ifPresent(parameters -> transactionDao.insertDatabaseProperty(databaseId, parameters));
            transactionDao.alterDatabase(catalogName, databaseName, database);
        });
    }

    @Override
    public synchronized void dropDatabase(String catalogName, String databaseName)
    {
        runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            Long databaseId = getDatabaseId(catalogName, databaseName, transactionDao);
            transactionDao.dropDatabaseProperty(databaseId);
            transactionDao.dropDatabase(catalogName, databaseName);
        });
    }

    @Override
    public synchronized Optional<DatabaseEntity> getDatabase(String catalogName, String databaseName)
    {
        requireNonNull(databaseName, "database name is null");
        return dao.getDatabase(catalogName, databaseName);
    }

    @Override
    public synchronized List<DatabaseEntity> getAllDatabases(String catalogName)
    {
        return dao.getAllDatabases(catalogName);
    }

    @Override
    public synchronized void createTable(TableEntity table)
    {
        runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);

            Long databaseId = getDatabaseId(table.getCatalogName(), table.getDatabaseName(), transactionDao);

            long tableId = transactionDao.insertTable(databaseId, table);
            Optional<List<PropertyEntity>> tableProps = mapToList(table.getParameters());
            tableProps.ifPresent(props -> transactionDao.insertTableProperty(tableId, props));

            insertColumnInfo(transactionDao, tableId, table.getColumns());
        });
    }

    @Override
    public synchronized void createTableIfNotExist(TableEntity table)
    {
        try {
            createTable(table);
        }
        catch (PrestoException e) {
            Optional<TableEntity> existedTable = getTable(table.getCatalogName(), table.getDatabaseName(), table.getName());
            if (!(existedTable.isPresent() && existedTable.get().equals(table))) {
                throw new PrestoException(HETU_METASTORE_CODE, e.getMessage(), e.getCause());
            }
        }
    }

    @Override
    public synchronized void dropTable(String catalogName, String databaseName, String tableName)
    {
        runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            // get table id
            Long tableId = transactionDao.getTableId(catalogName, databaseName, tableName);
            if (tableId == null) {
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            }

            // drop table property
            transactionDao.dropTableProperty(tableId);
            // get column id
            transactionDao.getColumnId(tableId).forEach(transactionDao::dropColumnProperty);
            // drop column
            transactionDao.dropColumn(tableId);
            // drop table
            transactionDao.dropTable(tableId);
        });
    }

    @Override
    public synchronized void alterTable(String catalogName, String databaseName, String oldTableName, TableEntity newTable)
    {
        runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);

            // get table id
            Long tableId = getTableId(transactionDao, catalogName, databaseName, oldTableName);

            // drop table property
            transactionDao.dropTableProperty(tableId);
            // get column id and drop column property
            transactionDao.getColumnId(tableId).forEach(transactionDao::dropColumnProperty);
            // drop column
            transactionDao.dropColumn(tableId);

            TableEntity table = TableEntity.builder(newTable).build();
            Optional<List<PropertyEntity>> tableProps = mapToList(table.getParameters());
            tableProps.ifPresent(props -> transactionDao.insertTableProperty(tableId, props));

            insertColumnInfo(transactionDao, tableId, table.getColumns());

            transactionDao.alterTable(tableId, table);
        });
    }

    private synchronized void insertColumnInfo(JdbcMetadataDao metadataDao, long tableId, List<ColumnEntity> columns)
    {
        if (columns == null) {
            return;
        }

        columns.forEach(column -> {
            long columnId = metadataDao.insertColumn(tableId, column);
            Optional<List<PropertyEntity>> columnParams = mapToList(column.getParameters());
            columnParams.ifPresent(parameters -> metadataDao.insertColumnProperty(columnId, parameters));
        });
    }

    private synchronized Long getTableId(JdbcMetadataDao metadataDao, String catalogName, String databaseName, String tableName)
    {
        Long tableId = metadataDao.getTableId(catalogName, databaseName, tableName);
        if (tableId == null) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        return tableId;
    }

    @Override
    public synchronized Optional<TableEntity> getTable(String catalogName, String databaseName, String tableName)
    {
        requireNonNull(tableName, "table name is null");
        ImmutableList.Builder<TableEntity> tables = ImmutableList.builder();
        runTransaction(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            List<Map.Entry<Long, TableEntity>> entries = transactionDao.getTable(catalogName, databaseName, tableName);

            if (entries.size() > 1) {
                throw new PrestoException(HETU_METASTORE_CODE, "Multiple table appear.");
            }
            getTableColumns(entries, tables, transactionDao);
        });
        return tables.build().stream().collect(toOptional());
    }

    @Override
    public synchronized List<TableEntity> getAllTables(String catalogName, String databaseName)
    {
        ImmutableList.Builder<TableEntity> tables = ImmutableList.builder();
        runTransaction(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            List<Map.Entry<Long, TableEntity>> entries = transactionDao.getAllTables(catalogName, databaseName);
            getTableColumns(entries, tables, transactionDao);
        });
        return tables.build().stream().collect(toImmutableList());
    }

    private synchronized void getTableColumns(List<Map.Entry<Long, TableEntity>> tableEntries,
            ImmutableList.Builder<TableEntity> tables, JdbcMetadataDao transactionDao)
    {
        tableEntries.forEach(entry -> {
            TableEntity table = entry.getValue();
            List<ColumnEntity> columns = transactionDao.getAllColumns(entry.getKey());
            table.setColumns(columns);
            tables.add(table);
        });
    }

    private synchronized Optional<List<PropertyEntity>> mapToList(Map<String, String> map)
    {
        if (map == null || map.isEmpty()) {
            return Optional.empty();
        }
        ImmutableList.Builder<PropertyEntity> propBuilder = ImmutableList.builder();
        map.forEach((key, value) -> propBuilder.add(new PropertyEntity(key, value)));
        return Optional.of(propBuilder.build());
    }

    private synchronized Long getDatabaseId(String catalogName, String databaseName, JdbcMetadataDao metadataDao)
    {
        Long databaseId = metadataDao.getDatabaseId(catalogName, databaseName);
        if (databaseId == null) {
            throw new PrestoException(HETU_METASTORE_CODE,
                    format(DATABASE_ERROR_MESSAGE, catalogName, databaseName));
        }
        return databaseId;
    }
}
