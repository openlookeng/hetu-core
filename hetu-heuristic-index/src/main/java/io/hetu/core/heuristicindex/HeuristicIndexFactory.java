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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.hetu.core.heuristicindex.filter.HeuristicIndexFilter;
import io.hetu.core.plugin.heuristicindex.index.bitmap.BitmapIndex;
import io.hetu.core.plugin.heuristicindex.index.bloom.BloomIndex;
import io.hetu.core.plugin.heuristicindex.index.btree.BTreeIndex;
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
import io.prestosql.spi.metastore.HetuMetastore;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.List;
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
    private static final List<Index> supportedIndices = ImmutableList.of(new BloomIndex(), new MinMaxIndex(), new BitmapIndex(), new BTreeIndex());

    public HeuristicIndexFactory()
    {
    }

    public static Index createIndex(String indexType)
    {
        try {
            return getIndexObjFromID(indexType).getClass().getConstructor().newInstance();
        }
        catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Error creating new instance of class: %s. Check the coordinator log for more details.", indexType), e);
        }
    }

    private static Index getIndexObjFromID(String indexType)
    {
        for (Index i : supportedIndices) {
            if (i.getId().equalsIgnoreCase(indexType)) {
                return i;
            }
        }

        throw new IllegalArgumentException("Index type " + indexType + " not supported.");
    }

    @Override
    public IndexClient getIndexClient(HetuFileSystemClient fs, HetuMetastore metastore, Path root)
    {
        requireNonNull(root, "No root path specified");

        LOG.debug("Creating IndexClient with given filesystem client with root path %s", root);

        return new HeuristicIndexClient(fs, metastore, root);
    }

    @Override
    public IndexFilter getIndexFilter(Map<String, List<IndexMetadata>> indices)
    {
        return new HeuristicIndexFilter(indices);
    }

    @Override
    public IndexWriter getIndexWriter(CreateIndexMetadata createIndexMetadata, Properties connectorMetadata, HetuFileSystemClient fs, Path root)
    {
        LOG.debug("Creating index writer for catalogName: %s", connectorMetadata.getProperty(HetuConstant.DATASOURCE_CATALOG));

        Properties indexProps = createIndexMetadata.getProperties();
        LOG.debug("indexProps: %s", indexProps);

        String indexType = createIndexMetadata.getIndexType();
        Index index = getIndexObjFromID(indexType);

        if (!index.getSupportedIndexLevels().contains(createIndexMetadata.getCreateLevel())) {
            throw new IllegalArgumentException(indexType + " does not support specified index level");
        }

        switch (createIndexMetadata.getCreateLevel()) {
            case STRIPE:
                return new FileIndexWriter(createIndexMetadata, connectorMetadata, fs, root);
            case PARTITION:
            case TABLE:
                return new PartitionIndexWriter(createIndexMetadata, fs, root);
            default:
                throw new IllegalArgumentException(indexType + " has no supported index writer");
        }
    }
}
