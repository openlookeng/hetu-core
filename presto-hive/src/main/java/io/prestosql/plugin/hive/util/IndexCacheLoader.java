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
package io.prestosql.plugin.hive.util;

import com.google.common.cache.CacheLoader;
import com.google.inject.Inject;
import io.prestosql.spi.heuristicindex.IndexCacheKey;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.IndexNotCreatedException;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public class IndexCacheLoader
        extends CacheLoader<IndexCacheKey, List<IndexMetadata>>
{
    private static IndexClient indexClient;

    @Inject
    public IndexCacheLoader(IndexClient indexClient)
    {
        IndexCacheLoader.indexClient = indexClient;
    }

    @Override
    public List<IndexMetadata> load(IndexCacheKey key)
            throws Exception
    {
        requireNonNull(key);
        requireNonNull(indexClient);

        // only load index files if index lastModified matches key lastModified
        long lastModified;

        try {
            lastModified = indexClient.getLastModified(key.getPath());
        }
        catch (Exception e) {
            // no lastModified file found, i.e. index doesn't exist
            throw new IndexNotCreatedException();
        }

        if (lastModified != key.getLastModifiedTime()) {
            throw new Exception("Index files are expired for key " + key);
        }

        List<IndexMetadata> indices;
        try {
            indices = indexClient.readSplitIndex(key.getPath());
        }
        catch (Exception e) {
            throw new Exception("No valid index files found for key " + key, e);
        }

        // null indicates that the index is not registered in index records
        if (indices == null) {
            throw new IndexNotCreatedException();
        }

        // lastModified file was valid, but no index files for the given types
        if (indices.isEmpty()) {
            throw new Exception("No index files found for key " + key);
        }

        // Sort the indices based on split starting position
        return indices.stream()
                .sorted(comparingLong(IndexMetadata::getSplitStart))
                .collect(Collectors.toList());
    }
}
