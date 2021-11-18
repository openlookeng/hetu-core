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
package io.hetu.core.heuristicindex;

import io.airlift.log.Logger;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;
import io.prestosql.spi.metastore.model.TableEntityType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Index record is stored in hetu metastore as parameters at table level. For each table entity,
 * See {@link IndexRecord} for more details.
 */
public class IndexRecordManager
{
    private static final Logger LOG = Logger.get(IndexRecordManager.class);

    private final HetuMetastore metastore;

    public IndexRecordManager(HetuMetastore metastore)
    {
        this.metastore = metastore;
    }

    /**
     * List all parameters at table level and filter those with hindex prefix.
     *
     * Construct {@code IndexRecord} objects from them.
     *
     * @return a list of deserialized {@code IndexRecord} objects.
     */
    public List<IndexRecord> getIndexRecords()
    {
        List<IndexRecord> records = new ArrayList<>();
        for (CatalogEntity catalogEntity : metastore.getCatalogs()) {
            for (DatabaseEntity databaseEntity : metastore.getAllDatabases(catalogEntity.getName())) {
                for (TableEntity tableEntity : metastore.getAllTables(catalogEntity.getName(), databaseEntity.getName())) {
                    for (Map.Entry<String, String> param : tableEntity.getParameters().entrySet()) {
                        if (param.getKey().startsWith(IndexRecord.INDEX_METASTORE_PREFIX)) {
                            records.add(new IndexRecord(tableEntity, param));
                        }
                    }
                }
            }
        }
        return records;
    }

    /**
     * Look up index record according to name.
     */
    public IndexRecord lookUpIndexRecord(String name)
    {
        return getIndexRecords().stream().filter(indexRecord -> indexRecord.name.equals(name)).findFirst().orElse(null);
    }

    /**
     * Look up index record according to what it is for (triplet of [table, column, type]).
     */
    public IndexRecord lookUpIndexRecord(String table, String[] columns, String indexType)
    {
        String[] tableQualified = table.split("\\.");
        if (tableQualified.length != 3) {
            throw new IllegalArgumentException(String.format("Illegal table name: %s", table));
        }
        Optional<TableEntity> tableEntity = metastore.getTable(tableQualified[0], tableQualified[1], tableQualified[2]);

        if (tableEntity.isPresent()) {
            for (Map.Entry<String, String> parameter : tableEntity.get().getParameters().entrySet()) {
                IndexRecord read = new IndexRecord(tableEntity.get(), parameter);
                if (Arrays.equals(read.columns, columns) && read.indexType.equals(indexType)) {
                    return read;
                }
            }
        }

        return null;
    }

    /**
     * Add IndexRecord into record file. If the method is called with a name that already exists,
     *
     * it will OVERWRITE the existing entry but COMBINE the partition columns (if it previously was partitioned)
     */
    public synchronized void addIndexRecord(String name, String user, String table, String[] columns, String indexType, long indexSize, List<String> indexProperties, List<String> partitions)
    {
        IndexRecord record = new IndexRecord(name, user, table, columns, indexType, indexSize, indexProperties, partitions);
        IndexRecord old = lookUpIndexRecord(name);
        if (old != null) {
            record.partitions.addAll(0, old.partitions);
        }

        Optional<CatalogEntity> oldCatalog = metastore.getCatalog(record.catalog);
        if (!oldCatalog.isPresent()) {
            CatalogEntity newCatalog = CatalogEntity.builder()
                    .setCatalogName(record.catalog)
                    .build();
            metastore.createCatalogIfNotExist(newCatalog);
        }

        Optional<DatabaseEntity> oldSchema = metastore.getDatabase(record.catalog, record.schema);
        if (!oldSchema.isPresent()) {
            DatabaseEntity newSchema = DatabaseEntity.builder()
                    .setCatalogName(record.catalog)
                    .setDatabaseName(record.schema)
                    .build();
            metastore.createDatabaseIfNotExist(newSchema);
        }

        Optional<TableEntity> oldTable = metastore.getTable(record.catalog, record.schema, record.table);
        if (!oldTable.isPresent()) {
            TableEntity newTable = TableEntity.builder()
                    .setCatalogName(record.catalog)
                    .setDatabaseName(record.schema)
                    .setTableName(record.table)
                    .setTableType(TableEntityType.TABLE.toString())
                    .build();
            metastore.createTableIfNotExist(newTable);
        }

        metastore.alterTableParameter(record.catalog,
                record.schema,
                record.table,
                record.serializeKey(),
                record.serializeValue());
    }

    /**
     * Delete index record from metastore according to name. Also allows partial deletion.
     *
     * @param name name of index to delete
     * @param partitionsToRemove the partitions to remove. If this list is empty, remove all.
     */
    public synchronized void deleteIndexRecord(String name, List<String> partitionsToRemove)
    {
        getIndexRecords().stream().filter(record -> record.name.equals(name))
                .forEach(record -> {
                    if (partitionsToRemove.isEmpty()) {
                        metastore.alterTableParameter(
                                record.catalog,
                                record.schema,
                                record.table,
                                record.serializeKey(),
                                null);
                    }
                    else {
                        record.partitions.removeAll(partitionsToRemove);
                        IndexRecord newRecord = new IndexRecord(record.name, record.user, record.qualifiedTable, record.columns,
                                record.indexType, record.indexSize, record.propertiesAsList, record.partitions);
                        metastore.alterTableParameter(
                                record.catalog,
                                record.schema,
                                record.table,
                                newRecord.serializeKey(),
                                newRecord.partitions.isEmpty() ? null : newRecord.serializeValue()); // if the last partition of the index has been dropped, remove the record
                    }
                });
    }
}
