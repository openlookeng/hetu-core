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
import io.prestosql.spi.service.PropertyService;
import io.prestosql.testing.NoOpIndexClient;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HeuristicIndexerManager
{
    private final FileSystemClientManager fileSystemClientManager;
    private IndexClient indexClient = new NoOpIndexClient();
    private static final Map<String, IndexFactory> indexFactories = new ConcurrentHashMap<>();

    private static final Logger LOG = Logger.get(HeuristicIndexerManager.class);

    @Inject
    public HeuristicIndexerManager(FileSystemClientManager fileSystemClientManager)
    {
        this.fileSystemClientManager = fileSystemClientManager;
    }

    public void loadIndexFactories(IndexFactory indexFactory)
    {
        indexFactories.put("default", indexFactory);
    }

    public void buildIndexClient()
            throws IOException
    {
        IndexFactory factory = indexFactories.get("default");
        if (PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED)) {
            String fsProfile = PropertyService.getStringProperty(HetuConstant.INDEXSTORE_FILESYSTEM_PROFILE);
            String indexStoreRoot = PropertyService.getStringProperty(HetuConstant.INDEXSTORE_URI);

            HetuFileSystemClient fs = fileSystemClientManager.getFileSystemClient(fsProfile);
            Path root = Paths.get(indexStoreRoot);

            indexClient = factory.getIndexClient(fs, root);
            LOG.info("Heuristic Indexer Client created on %s at %s", fsProfile, indexStoreRoot);
        }
    }

    public IndexClient getIndexClient()
    {
        return indexClient;
    }
}
