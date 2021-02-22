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
package io.hetu.core.heuristicindex;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;
import io.prestosql.spi.metastore.model.TableEntityType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class IndexRecordManager
{
    private static final Logger LOG = Logger.get(IndexRecordManager.class);

    private final HetuMetastore metastore;

    public IndexRecordManager(HetuMetastore metastore)
    {
        this.metastore = metastore;
    }

    public List<IndexRecord> getIndexRecords()
    {
        long startTime = System.currentTimeMillis();

        ImmutableList.Builder<IndexRecord> records = ImmutableList.builder();
        getNewIndexStream().forEach(records::add);
        LOG.debug("{}ms spent on index record scan from hetu metastore", System.currentTimeMillis() - startTime);

        return records.build();
    }

    public IndexRecord lookUpIndexRecord(String name)
            throws IOException
    {
        return getNewIndexStream().filter(indexRecord -> indexRecord.name.equals(name)).findFirst().orElse(null);
    }

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
     * it will OVERWRITE the existing entry but combine the partition column
     */
    public synchronized void addIndexRecord(String name, String user, String table, String[] columns, String indexType, List<String> indexProperties, List<String> partitions)
            throws IOException
    {
        IndexRecord record = new IndexRecord(name, user, table, columns, indexType, indexProperties, partitions);

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

        Optional<TableEntity> tableEntity = metastore.getTable(record.catalog, record.schema, record.table);
        if (tableEntity.isPresent()) {
            TableEntity newTable = tableEntity.get();
            newTable.getParameters().put(record.serializeKey(), record.serializeValue());
            metastore.alterTable(record.catalog, record.schema, record.table, newTable);
        }
        else {
            throw new IllegalStateException("Failed to create table entity in hetu metastore");
        }
    }

    public synchronized void deleteIndexRecord(String name, List<String> partitionsToRemove)
            throws IOException
    {
        getNewIndexStream().filter(record -> record.name.equals(name))
                .forEach(record -> {
                    Optional<TableEntity> tableEntity = metastore.getTable(record.catalog, record.schema, record.table);
                    if (tableEntity.isPresent()) {
                        TableEntity newTable = tableEntity.get();
                        if (partitionsToRemove.isEmpty()) {
                            tableEntity.get().getParameters().remove(record.serializeKey());
                        }
                        else {
                            record.partitions.removeAll(partitionsToRemove);
                            IndexRecord newRecord = new IndexRecord(record.name, record.user, record.qualifiedTable, record.columns,
                                    record.indexType, record.properties, record.partitions);
                            tableEntity.get().getParameters().put(newRecord.serializeKey(), newRecord.serializeValue());
                        }
                        metastore.alterTable(record.catalog, record.schema, record.table, newTable);
                    }
                });
    }

    private Stream<IndexRecord> getNewIndexStream()
    {
        return metastore.getCatalogs().stream()
                .flatMap(catalogEntity -> metastore.getAllDatabases(catalogEntity.getName()).stream())
                .flatMap(databaseEntity -> metastore.getAllTables(databaseEntity.getCatalogName(), databaseEntity.getName()).stream())
                .flatMap(tableEntity -> tableEntity.getParameters().entrySet().stream()
                        .filter(e -> e.getKey().startsWith(IndexRecord.INDEX_METASTORE_PREFIX))
                        .map(e -> new IndexRecord(tableEntity, e)));
    }
}
