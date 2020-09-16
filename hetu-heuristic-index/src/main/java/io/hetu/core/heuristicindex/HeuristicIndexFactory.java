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

import com.google.common.collect.ImmutableSet;
import io.hetu.core.heuristicindex.util.IndexConstants;
import io.hetu.core.heuristicindex.util.IndexServiceUtils;
import io.hetu.core.plugin.heuristicindex.datasource.base.EmptyDataSource;
import io.hetu.core.plugin.heuristicindex.datasource.hive.HiveDataSource;
import io.hetu.core.plugin.heuristicindex.index.bitmap.BitMapIndex;
import io.hetu.core.plugin.heuristicindex.index.bloom.BloomIndex;
import io.hetu.core.plugin.heuristicindex.index.minmax.MinMaxIndex;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.DataSource;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexFactory;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import static io.hetu.core.heuristicindex.util.IndexServiceUtils.printVerboseMsg;
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
    // Add new Index and DataSources here in the future
    private final Set<Index> supportedIndex = ImmutableSet.of(new BloomIndex(), new MinMaxIndex(), new BitMapIndex());
    private final Set<DataSource> supportedDataSource = ImmutableSet.of(new EmptyDataSource(), new HiveDataSource());

    public HeuristicIndexFactory()
    {
    }

    @Override
    public HeuristicIndexWriter getIndexWriter(Properties dataSourceProps, Properties indexProps, HetuFileSystemClient fs, Path root)
    {
        printVerboseMsg(String.format("dataSourceProps: %s", dataSourceProps));
        printVerboseMsg(String.format("indexProps: %s", indexProps));

        // Load DataSource
        String dataSourceName = dataSourceProps.getProperty(IndexConstants.DATASTORE_TYPE_KEY);
        requireNonNull(dataSourceName, "No datasource found");
        DataSource dataSource = null;
        for (DataSource ds : supportedDataSource) {
            if (dataSourceName.equalsIgnoreCase(ds.getId())) {
                dataSource = ds;
                break;
            }
        }

        if (dataSource == null) {
            throw new IllegalArgumentException("DataSource not supported: " + dataSourceName);
        }

        printVerboseMsg(String.format("Using DataSource: %s", dataSource));
        dataSource.setProperties(dataSourceProps);

        // Load Index
        Set<Index> indices = new HashSet<>(supportedIndex);
        for (Index index : indices) {
            printVerboseMsg(String.format("Using Index: %s", index));
            index.setProperties(IndexServiceUtils.getPropertiesSubset(
                    indexProps, index.getId().toLowerCase(Locale.ENGLISH) + "."));
        }

        return new HeuristicIndexWriter(dataSource, indices, fs, root);
    }

    @Override
    public IndexClient getIndexClient(HetuFileSystemClient fs, Path root)
    {
        requireNonNull(root, "No root path specified");

        printVerboseMsg(String.format("Creating IndexClient with given filesystem client with root path %s", root));

        return new HeuristicIndexClient(new HashSet<>(supportedIndex), fs, root);
    }
}
