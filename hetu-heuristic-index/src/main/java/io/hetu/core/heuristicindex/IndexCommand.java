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

import io.hetu.core.common.util.SecurePathWhiteList;
import io.hetu.core.heuristicindex.util.IndexCommandUtils;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexFactory;
import io.prestosql.spi.heuristicindex.IndexWriter;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.hetu.core.heuristicindex.util.IndexCommandUtils.loadDataSourceProperties;
import static io.hetu.core.heuristicindex.util.IndexCommandUtils.loadIndexStore;
import static java.util.Objects.requireNonNull;

/**
 * Entry class for indexer
 */
public class IndexCommand
{
    String configDirPath;
    String table;
    String[] columns;
    String[] partitions;
    String indexType;
    String[] indexProps;
    boolean parallelCreation;
    public static boolean verbose; // disabled by default
    String indexName;
    String user;

    public IndexCommand(String configDirPath, String name, boolean verbose)
    {
        this.configDirPath = configDirPath;
        this.indexName = name;
        IndexCommand.verbose = verbose;
    }

    public IndexCommand(String configDirPath, String name, boolean verbose, String user)
    {
        this.configDirPath = configDirPath;
        this.indexName = name;
        IndexCommand.verbose = verbose;
        this.user = user;
    }

    public IndexCommand(String configDirPath, String name, String table, String[] columns, String[] partitions, String indexType, String[] indexProps, boolean parallelCreation,
            boolean verbose, String user)
    {
        this.configDirPath = configDirPath;
        this.table = table;
        this.columns = columns;
        this.partitions = partitions;
        this.indexType = indexType.toLowerCase(Locale.ENGLISH);
        this.indexProps = indexProps;
        this.parallelCreation = parallelCreation;
        IndexCommand.verbose = verbose;
        this.indexName = name;
        this.user = user;
    }

    public IndexRecordManager.IndexRecord getIndex()
    {
        try {
            validatePaths();
            IndexFactory factory = IndexCommandUtils.getIndexFactory();
            IndexCommandUtils.IndexStore indexStore = loadIndexStore(configDirPath);
            return IndexRecordManager.lookUpIndexRecord(indexStore.getFs(), indexStore.getRoot(), indexName);
        }
        catch (IOException e) {
            e.printStackTrace(System.err);
            return null;
        }
    }

    public List<IndexRecordManager.IndexRecord> getIndexes()
    {
        try {
            validatePaths();
            IndexCommandUtils.IndexStore indexStore = loadIndexStore(configDirPath);

            if (indexName.equals("")) {
                return IndexRecordManager.readAllIndexRecords(indexStore.getFs(), indexStore.getRoot());
            }
            else {
                List<IndexRecordManager.IndexRecord> records = Collections.singletonList(
                        IndexRecordManager.lookUpIndexRecord(indexStore.getFs(), indexStore.getRoot(), indexName));
                if (records.get(0) == null) {
                    return Collections.emptyList();
                }
                return records;
            }
        }
        catch (IOException e) {
            e.printStackTrace(System.err);
        }
        return Collections.emptyList();
    }

    public void deleteIndex()
    {
        try {
            validatePaths();
            IndexFactory factory = IndexCommandUtils.getIndexFactory();
            IndexCommandUtils.IndexStore indexStore = loadIndexStore(configDirPath);
            IndexClient deleteClient = factory.getIndexClient(indexStore.getFs(), indexStore.getRoot());
            IndexRecordManager.IndexRecord record = IndexRecordManager.lookUpIndexRecord(indexStore.getFs(), indexStore.getRoot(), indexName);
            if (record == null) {
                System.out.printf("Index with name [%s] does not exist.%n%n", indexName);
                return;
            }
            if (!record.user.equals("") && !record.user.equals(user)) {
                System.out.printf("Index [%s] is owned by [%s]. Can't be modified with current user [%s].%n%n", indexName, record.user, user);
                return;
            }
            deleteClient.deleteIndex(record.table, record.columns, record.indexType);
            IndexRecordManager.deleteIndexRecord(indexStore.getFs(), indexStore.getRoot(), indexName);
            System.out.format("Deleted index [%s].%n%n", indexName);
        }
        catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }

    public void createIndex()
    {
        try {
            // validate inputs
            // security check required before using values in Path
            // e.g. catalog.schema.table or dc.catalog.schema.table
            checkArgument(table.matches("([\\p{Alnum}_]+\\.){2,3}[\\p{Alnum}_]+"), "Invalid table name");
            if (columns != null) {
                for (String column : columns) {
                    checkArgument(column.matches("[\\p{Alnum}_]+"), "Invalid column name");
                }
            }

            validatePaths();
            IndexCommandUtils.IndexStore indexStore = loadIndexStore(configDirPath);
            IndexFactory factory = IndexCommandUtils.getIndexFactory();
            IndexRecordManager.IndexRecord sameNameRecord = IndexRecordManager.lookUpIndexRecord(indexStore.getFs(), indexStore.getRoot(), indexName);
            IndexRecordManager.IndexRecord sameIndexRecord = IndexRecordManager.lookUpIndexRecord(indexStore.getFs(), indexStore.getRoot(), table, columns, indexType);

            if (sameNameRecord == null) {
                if (sameIndexRecord != null) {
                    System.out.printf("Index with same (table,column,indexType) already exists with name [%s]%n%n", sameIndexRecord.name);
                    return;
                }
            }
            else {
                if (sameIndexRecord != null) {
                    if (!parallelCreation) {
                        System.out.printf("Same entry already exists. To update, please delete old index first. " +
                                "If this is parallel creation, add WITH (parallelCreation=true).%n%n");
                        return;
                    }
                }
                else {
                    System.out.printf("Index with name [%s] already exists with different content: [%s]%n%n", indexName, sameNameRecord);
                    return;
                }
            }

            Properties dsProperties = loadDataSourceProperties(table, configDirPath);
            Properties ixProperties = new Properties();
            if (indexProps != null) {
                for (String s : indexProps) {
                    if (!s.contains("=")) {
                        throw new IllegalArgumentException("Index properties should be like 'xx.xx=xx'");
                    }
                    String key = s.split("=")[0];
                    String val = s.split("=")[1];
                    ixProperties.setProperty(key, val);
                }
            }
            requireNonNull(indexType, "No index type specified for create command");
            requireNonNull(columns, "No columns specified for create command");
            IndexWriter writer = factory.getIndexWriter(dsProperties, ixProperties, indexStore.getFs(), indexStore.getRoot());
            writer.createIndex(table, columns, partitions, indexType, parallelCreation);
            IndexRecordManager.addIndexRecord(indexStore.getFs(), indexStore.getRoot(), indexName, user, table, columns, indexType, partitions);
            if (!verbose) {
                System.out.print("\n");
            }
            System.out.print("\n");
        }
        catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }

    private void validatePaths()
            throws IOException
    {
        checkArgument(!configDirPath.contains("../") && SecurePathWhiteList.isSecurePath(configDirPath),
                "Invalid config directory path " + configDirPath + ". " +
                        "Config directory path must be absolute and under one of the following directories: "
                        + SecurePathWhiteList.getSecurePathWhiteList().toString());
        checkArgument(Paths.get(configDirPath).toFile().exists(), "Config directory " + configDirPath + " does not exist");
    }
}
