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
package io.prestosql.heuristicindex;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.hetu.core.common.util.SecurePathWhiteList;
import io.prestosql.execution.QueryInfo;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexFactory;
import io.prestosql.spi.heuristicindex.IndexFilter;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.heuristicindex.IndexWriter;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.testing.NoOpIndexClient;
import io.prestosql.testing.NoOpIndexWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;

public class HeuristicIndexerManager
{
    private final FileSystemClientManager fileSystemClientManager;
    private final HetuMetaStoreManager hetuMetaStoreManager;
    private static IndexFactory factory;
    private static final Logger LOG = Logger.get(HeuristicIndexerManager.class);

    private Path root;
    private HetuFileSystemClient fs;
    private HetuMetastore metastore;
    private IndexClient indexClient = new NoOpIndexClient();
    private IndexWriter indexWriter = new NoOpIndexWriter();

    @Inject
    public HeuristicIndexerManager(FileSystemClientManager fileSystemClientManager, HetuMetaStoreManager hetuMetaStoreManager)
    {
        this.fileSystemClientManager = fileSystemClientManager;
        this.hetuMetaStoreManager = hetuMetaStoreManager;
    }

    public static HeuristicIndexerManager getNoOpHeuristicIndexerManager()
    {
        return new HeuristicIndexerManager(new FileSystemClientManager(), new HetuMetaStoreManager());
    }

    public void initCache()
    {
        if (PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED)) {
            SplitFiltering.getCache(getIndexClient());
        }
    }

    public void loadIndexFactories(IndexFactory indexFactory)
    {
        HeuristicIndexerManager.factory = indexFactory;
    }

    public IndexClient getIndexClient()
    {
        return indexClient;
    }

    private IndexCache getIndexCache()
    {
        return SplitFiltering.getCache(indexClient);
    }

    public List<IndexRecord> getAllIndexRecordsWithUsage()
            throws IOException
    {
        List<IndexRecord> records = getIndexClient().getAllIndexRecords();
        updateIndexRecordUsage(records);
        return records;
    }

    public List<IndexRecord> getIndexRecordWithUsage(String indexName)
            throws IOException
    {
        List<IndexRecord> records = Collections.singletonList(getIndexClient().lookUpIndexRecord(indexName));
        if (records.get(0) == null) {
            return Collections.emptyList();
        }
        updateIndexRecordUsage(records);
        return records;
    }

    private void updateIndexRecordUsage(List<IndexRecord> targetRecords)
    {
        HashMap<IndexRecord, Long> indexRecordMemoryUse = new HashMap<IndexRecord, Long>();
        HashMap<IndexRecord, Long> indexRecordDiskUse = new HashMap<IndexRecord, Long>();
        for (IndexRecord record : targetRecords) {
            indexRecordMemoryUse.put(record, 0L);
            indexRecordDiskUse.put(record, 0L);
        }

        // get the memory and disk usage of the records from cache
        getIndexCache().readUsage(indexRecordMemoryUse, indexRecordDiskUse);

        for (IndexRecord record : targetRecords) {
            // update the indexRecord memory and disk usage field
            record.setMemoryUsage(indexRecordMemoryUse.get(record));
            record.setDiskUsage(indexRecordDiskUse.get(record));
        }
    }

    public IndexWriter getIndexWriter(CreateIndexMetadata createIndexMetadata, Properties connectorMetadata)
    {
        if (PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED)) {
            if (factory != null) {
                return factory.getIndexWriter(createIndexMetadata, connectorMetadata, fs, root);
            }
        }

        return indexWriter;
    }

    public IndexFilter getIndexFilter(Map<String, List<IndexMetadata>> indices)
    {
        return factory.getIndexFilter(indices);
    }

    public void buildIndexClient()
            throws IOException
    {
        if (PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED)) {
            String fsProfile = PropertyService.getStringProperty(HetuConstant.INDEXSTORE_FILESYSTEM_PROFILE);
            String indexStoreRoot = PropertyService.getStringProperty(HetuConstant.INDEXSTORE_URI);

            root = Paths.get(indexStoreRoot);

            // although the root is already checked in HetuConfig#getIndexStoreUri
            // when we set and get it from PropertyService, the code scan tool loses track and complains
            // add the check here again
            checkArgument(!root.toString().contains("../"),
                    HetuConstant.INDEXSTORE_URI + " must be absolute and under one of the following whitelisted directories:  " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(root),
                    HetuConstant.INDEXSTORE_URI + " must be under one of the following whitelisted directories: " + SecurePathWhiteList.getSecurePathWhiteList().toString());

            fs = fileSystemClientManager.getFileSystemClient(fsProfile, root);
            if (fileSystemClientManager.isFileSystemLocal(fsProfile)) {
                throw new IllegalArgumentException("Indexer does not support local filesystem: " + fsProfile);
            }

            metastore = hetuMetaStoreManager.getHetuMetastore();
            if (metastore == null) {
                throw new IllegalStateException("Hetu metastore is not properly configured. Heuristic indexer needs it to manage index metadata. " +
                        "Please check documentation for how to set it up.");
            }
            if (factory != null) {
                indexClient = factory.getIndexClient(fs, metastore, root);
            }
        }
    }

    public void preloadIndex()
            throws IOException
    {
        if (PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED) && indexClient != null) {
            String preloadNames = PropertyService.getStringProperty(HetuConstant.FILTER_CACHE_PRELOAD_INDICES);
            List<String> preloadNameList = Arrays.asList(preloadNames.split(","));
            try {
                SplitFiltering.preloadCache(indexClient, preloadNameList);
            }
            catch (Exception e) {
                LOG.info("Error loading index: " + e);
            }
        }
    }

    public void cleanUpIndexRecord(QueryInfo queryInfo)
    {
        try {
            String query = queryInfo.getQuery();
            LOG.debug("Clean up index record after this query failed: %s", query);
            String indexName = query.split(" ")[2];
            IndexRecord record = indexClient.lookUpIndexRecord(indexName);
            if (record != null && record.isInProgressRecord()) {
                indexClient.deleteIndex(indexName, Collections.emptyList());
            }
        }
        catch (Exception e) {
            LOG.debug("Failed to clean index record for : %s", queryInfo.getQuery());
        }
    }
}
