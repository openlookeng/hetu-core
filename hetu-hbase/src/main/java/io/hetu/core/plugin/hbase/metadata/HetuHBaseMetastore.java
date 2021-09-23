/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.hbase.metadata;

import com.google.common.collect.ImmutableMap;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.connector.HBaseConnectorId;
import io.hetu.core.plugin.hbase.utils.Constants;
import io.hetu.core.plugin.hbase.utils.Utils;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.ColumnEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class HetuHBaseMetastore
        implements HBaseMetastore
{
    private static final String ID = "Hetu";
    private static final String CATALOG_NAME = "hbase_catalog";
    private static final String TABLE_TYPE = "hbase_table";
    private static final int SPLIT_NUM = 2;

    private final Map<String, HBaseTable> hbaseTables = new ConcurrentHashMap<>();
    private HetuMetastore hetuMetastore;
    private final AtomicBoolean hasAllTables = new AtomicBoolean(false);
    private String catalogName;

    public HetuHBaseMetastore(HetuMetastore hetuMetastore)
    {
        this.hetuMetastore = requireNonNull(hetuMetastore, "hetuMetastore is null, please config 'etc/hetu-metastore.properties'.");
        this.catalogName = CATALOG_NAME + HBaseConnectorId.getConnectorId();
    }

    @Override
    public void init()
    {
        if (!hetuMetastore.getCatalog(catalogName).isPresent()) {
            hetuMetastore.createCatalog(
                    CatalogEntity.builder()
                            .setCatalogName(catalogName)
                            .setCreateTime(new Date().getTime())
                            .build());
        }
    }

    @Override
    public String getId()
    {
        return ID;
    }

    @Override
    public void addHBaseTable(HBaseTable hBaseTable)
    {
        synchronized (hbaseTables) {
            TableEntity tableEntity = hBaseTableToTableEntity(hBaseTable);
            if (!hetuMetastore.getDatabase(catalogName, hBaseTable.getSchema()).isPresent()) {
                DatabaseEntity databaseEntity = DatabaseEntity.builder()
                        .setCatalogName(catalogName)
                        .setDatabaseName(hBaseTable.getSchema())
                        .setCreateTime(new Date().getTime())
                        .build();
                hetuMetastore.createDatabase(databaseEntity);
            }
            hetuMetastore.createTable(tableEntity);
            hbaseTables.put(hBaseTable.getFullTableName(), hBaseTable);
        }
    }

    @Override
    public void renameHBaseTable(HBaseTable newTable, String oldTable)
    {
        synchronized (hbaseTables) {
            String[] table = oldTable.split(Constants.SPLITPOINT);

            if (table.length != SPLIT_NUM) {
                return;
            }

            TableEntity tableEntity = hBaseTableToTableEntity(newTable);
            hetuMetastore.alterTable(catalogName, table[0], table[1], tableEntity);

            hbaseTables.remove(oldTable);
            hbaseTables.put(newTable.getFullTableName(), newTable);
        }
    }

    @Override
    public void dropHBaseTable(HBaseTable hBaseTable)
    {
        synchronized (hbaseTables) {
            hetuMetastore.dropTable(catalogName, hBaseTable.getSchema(), hBaseTable.getTable());
            if (hbaseTables.containsKey(hBaseTable.getFullTableName())) {
                hbaseTables.remove(hBaseTable.getFullTableName());
            }
        }
    }

    @Override
    public Map<String, HBaseTable> getAllHBaseTables()
    {
        synchronized (hbaseTables) {
            if (hasAllTables.get()) {
                return ImmutableMap.copyOf(hbaseTables);
            }
            Map<String, HBaseTable> hbaseTablesTemp = new ConcurrentHashMap<>();

            List<DatabaseEntity> dbs = hetuMetastore.getAllDatabases(catalogName);

            for (DatabaseEntity db : dbs) {
                List<TableEntity> tables = hetuMetastore.getAllTables(db.getCatalogName(), db.getName());
                for (TableEntity table : tables) {
                    HBaseTable hBaseTable = getHBaseTableFromHetuMetastore(table.getCatalogName(), table.getDatabaseName(),
                            table.getName());
                    hbaseTablesTemp.put(hBaseTable.getFullTableName(), hBaseTable);
                }
            }

            hbaseTables.clear();
            hbaseTables.putAll(hbaseTablesTemp);
            hasAllTables.set(true);

            return ImmutableMap.copyOf(hbaseTables);
        }
    }

    @Override
    public HBaseTable getHBaseTable(String tableName)
    {
        if (hbaseTables.get(tableName) != null) {
            return hbaseTables.get(tableName);
        }

        String[] table = tableName.split(Constants.SPLITPOINT);

        if (table.length != SPLIT_NUM) {
            return null;
        }

        HBaseTable hBaseTable = getHBaseTableFromHetuMetastore(catalogName, table[0], table[1]);

        if (hBaseTable != null) {
            hbaseTables.put(hBaseTable.getFullTableName(), hBaseTable);
        }

        return hBaseTable;
    }

    private HBaseTable getHBaseTableFromHetuMetastore(String catalog, String schema, String table)
    {
        TableEntity tableEntity = hetuMetastore.getTable(catalog, schema, table).orElse(null);

        if (tableEntity == null) {
            return null;
        }

        Map<String, ColumnHandle> columnsMap = new HashMap<>();
        List<HBaseColumnHandle> columns = new ArrayList<>();

        for (ColumnEntity entity : tableEntity.getColumns()) {
            Map<String, String> columnParam = entity.getParameters();
            HBaseColumnHandle columnHandle = new HBaseColumnHandle(
                    entity.getName(),
                    Optional.ofNullable(columnParam.get(Constants.S_FAMILY)),
                    Optional.ofNullable(columnParam.get(Constants.S_QUALIFIER)),
                    Utils.createTypeByName(entity.getType()),
                    Integer.parseInt(columnParam.get(Constants.S_ORDINAL)),
                    entity.getComment(),
                    Constants.S_TRUE.equals(columnParam.get(Constants.S_INDEXED).toLowerCase(Locale.ENGLISH)));
            columns.add(columnHandle);
            columnsMap.put(entity.getName(), columnHandle);
        }

        Map<String, String> tableParam = tableEntity.getParameters();
        HBaseTable hBaseTable = new HBaseTable(
                tableEntity.getDatabaseName(),
                tableEntity.getName(),
                columns,
                tableParam.get(Constants.S_ROWID),
                Constants.S_TRUE.equals(tableParam.get(Constants.S_EXTERNAL).toLowerCase(Locale.ENGLISH)),
                Optional.ofNullable(tableParam.get(Constants.S_SERIALIZER_CLASS_NAME)),
                Optional.ofNullable(tableParam.get(Constants.S_INDEX_COLUMNS)),
                Optional.ofNullable(tableParam.get(Constants.S_HBASE_TABLE_NAME)),
                Optional.ofNullable(tableParam.get(Constants.S_SPLIT_BY_CHAR)));
        hBaseTable.setColumnsToMap(columnsMap);

        return hBaseTable;
    }

    private TableEntity hBaseTableToTableEntity(HBaseTable hBaseTable)
    {
        TableEntity tableEntity = new TableEntity();
        tableEntity.setCatalogName(catalogName);
        tableEntity.setDatabaseName(hBaseTable.getSchema());
        tableEntity.setName(hBaseTable.getTable());
        tableEntity.setType(TABLE_TYPE);

        List<ColumnEntity> columns = new ArrayList<>();
        for (HBaseColumnHandle columnHandle : hBaseTable.getColumns()) {
            ColumnEntity columnEntity = new ColumnEntity();
            columnEntity.setName(columnHandle.getColumnName());
            columnEntity.setType(columnHandle.getType().getClass().getName());
            columnEntity.setComment(columnHandle.getComment());

            Map<String, String> columnParam = new HashMap<>();
            columnParam.put(Constants.S_FAMILY, columnHandle.getFamily().orElse(null));
            columnParam.put(Constants.S_QUALIFIER, columnHandle.getQualifier().orElse(null));
            columnParam.put(Constants.S_ORDINAL, String.valueOf(columnHandle.getOrdinal()));
            columnParam.put(Constants.S_INDEXED, String.valueOf(columnHandle.isIndexed()).toLowerCase(Locale.ENGLISH));
            columnEntity.setParameters(columnParam);

            columns.add(columnEntity);
        }
        tableEntity.setColumns(columns);

        Map<String, String> tableParam = new HashMap<>();
        tableParam.put(Constants.S_EXTERNAL, String.valueOf(hBaseTable.isExternal()).toLowerCase(Locale.ENGLISH));
        tableParam.put(Constants.S_SERIALIZER_CLASS_NAME, hBaseTable.getSerializerClassName());
        tableParam.put(Constants.S_ROWID, hBaseTable.getRowId());
        tableParam.put(Constants.S_INDEX_COLUMNS, hBaseTable.getIndexColumns());
        tableParam.put(Constants.S_HBASE_TABLE_NAME, hBaseTable.getHbaseTableName().orElse(null));
        tableParam.put(Constants.S_SPLIT_BY_CHAR, hBaseTable.getSplitByChar().orElse(null));

        tableEntity.setParameters(tableParam);

        return tableEntity;
    }
}
