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

import io.airlift.log.Logger;
import io.hetu.core.heuristicindex.filter.HeuristicIndexFilter;
import io.hetu.core.plugin.heuristicindex.index.bloom.BloomIndex;
import io.hetu.core.plugin.heuristicindex.index.minmax.MinMaxIndex;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexFactory;
import io.prestosql.spi.heuristicindex.IndexFilter;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.IndexWriter;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * The IndexerFactory is used to load the provided plugins (SPI implementations)
 * and use the provided configurations to configure these implementations.
 * It will return configured IndexClient and IndexWriter objects to be used for
 * reading, writing and deleting indexes.
 */
public class HeuristicIndexFactory
        implements IndexFactory
{
    private static final Logger LOG = Logger.get(HeuristicIndexFactory.class);

    public HeuristicIndexFactory()
    {
    }

    public static Index createIndex(String indexType)
    {
        switch (indexType.toUpperCase(Locale.ENGLISH)) {
            case BloomIndex.ID:
                return new BloomIndex();
            case MinMaxIndex.ID:
                return new MinMaxIndex();
            default:
                throw new IllegalArgumentException("Index type " + indexType + " not supported.");
        }
    }

    @Override
    public IndexWriter getIndexWriter(CreateIndexMetadata createIndexMetadata, Properties connectorMetadata, HetuFileSystemClient fs, Path root)
    {
        LOG.debug("Creating index writer for catalogName: %s", connectorMetadata.getProperty(HetuConstant.DATASOURCE_CATALOG));

        Properties indexProps = createIndexMetadata.getProperties();
        LOG.debug("indexProps: %s", indexProps);

        String indexType = createIndexMetadata.getIndexType();

        switch (indexType.toUpperCase(Locale.ENGLISH)) {
            case BloomIndex.ID:
            case MinMaxIndex.ID:
                return new FileIndexWriter(createIndexMetadata, connectorMetadata, fs, root);
            default:
                throw new IllegalArgumentException(indexType + " has no supported index writer");
        }
    }

    @Override
    public IndexClient getIndexClient(HetuFileSystemClient fs, Path root)
    {
        requireNonNull(root, "No root path specified");

        LOG.debug("Creating IndexClient with given filesystem client with root path %s", root);

        return new HeuristicIndexClient(fs, root);
    }

    @Override
    public IndexFilter getIndexFilter(Map<String, List<IndexMetadata>> indices)
    {
        return new HeuristicIndexFilter(indices);
    }
}
