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
package io.prestosql.spi.heuristicindex;

import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.metastore.HetuMetastore;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface IndexFactory
{
    /**
     * Creates an IndexWriter configured with config files, HetuFileSystemClient and root Path
     * configRoot/config.properties -> ixProperty
     *
     * @param createIndexMetadata index metadata
     * @param connectorMetadata datasource information
     * @param fs filesystem client to read index
     * @param root path to the index root folder
     * @return Returns the IndexWriter loaded with the corresponding indexMetadata, Index and filesystem client
     */
    public IndexWriter getIndexWriter(CreateIndexMetadata createIndexMetadata, Properties connectorMetadata, HetuFileSystemClient fs, Path root);

    /**
     * Creates an IndexClient configured with the IndexStore using the provided filesystem client and root folder.
     *
     * @param fs filesystem client to read index
     * @param metastore
     * @param root path to the index root folder
     * @return An IndexClient which has the IndexStore configured
     */
    public IndexClient getIndexClient(HetuFileSystemClient fs, HetuMetastore metastore, Path root);

    public IndexFilter getIndexFilter(Map<String, List<IndexMetadata>> indices);
}
