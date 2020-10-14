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
package io.prestosql.heuristicindex;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexFactory;
import io.prestosql.spi.heuristicindex.IndexFilter;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.IndexWriter;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.testing.NoOpIndexClient;
import io.prestosql.testing.NoOpIndexWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HeuristicIndexerManager
{
    private final FileSystemClientManager fileSystemClientManager;
    private static IndexFactory factory;
    private static final Logger LOG = Logger.get(HeuristicIndexerManager.class);

    private IndexClient indexClient = new NoOpIndexClient();
    private IndexWriter indexWriter = new NoOpIndexWriter();

    @Inject
    public HeuristicIndexerManager(FileSystemClientManager fileSystemClientManager)
    {
        this.fileSystemClientManager = fileSystemClientManager;
    }

    public void loadIndexFactories(IndexFactory indexFactory)
    {
        HeuristicIndexerManager.factory = indexFactory;
    }

    public void buildIndexClient()
            throws IOException
    {
        if (PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED)) {
            String fsProfile = PropertyService.getStringProperty(HetuConstant.INDEXSTORE_FILESYSTEM_PROFILE);
            String indexStoreRoot = PropertyService.getStringProperty(HetuConstant.INDEXSTORE_URI);

            Path root = Paths.get(indexStoreRoot);
            HetuFileSystemClient fs = fileSystemClientManager.getFileSystemClient(fsProfile, root);

            indexClient = factory.getIndexClient(fs, root);
            LOG.info("Heuristic Indexer Client created on %s at %s", fsProfile, indexStoreRoot);
        }
    }

    public void buildIndexWriter()
            throws IOException
    {
        String fsProfile = PropertyService.getStringProperty(HetuConstant.INDEXSTORE_FILESYSTEM_PROFILE);
        String indexStoreRoot = PropertyService.getStringProperty(HetuConstant.INDEXSTORE_URI);

        Path root = Paths.get(indexStoreRoot);
        HetuFileSystemClient fs = fileSystemClientManager.getFileSystemClient(fsProfile, root);

        // TODO: Update IndexWriter API, should remove the data source properties part
        // TODO: Index properties should be provided through create index command
        Properties dataSourceProperties = new Properties();
        Properties indexProperties = new Properties();

        indexWriter = factory.getIndexWriter(dataSourceProperties, indexProperties, fs, root);
        LOG.info("Heuristic Indexer Writer created on %s at %s", fsProfile, indexStoreRoot);
    }

    public IndexClient getIndexClient()
    {
        return indexClient;
    }

    public IndexWriter getIndexWriter()
    {
        return indexWriter;
    }

    public IndexFilter getIndexFilter(Map<String, List<IndexMetadata>> indices)
    {
        return factory.getIndexFilter(indices);
    }
}
