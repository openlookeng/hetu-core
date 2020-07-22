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

import com.google.common.cache.CacheLoader;
import io.hetu.core.common.heuristicindex.IndexCacheKey;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexMetadata;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public class IndexCacheLoader
        extends CacheLoader<IndexCacheKey, List<IndexMetadata>>
{
    private static IndexClient indexClient;

    public IndexCacheLoader(IndexClient client)
    {
        IndexCacheLoader.indexClient = client;
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
            throw new Exception(String.format("No index file found for key %s.", key), e);
        }

        if (lastModified != key.getLastModifiedTime()) {
            throw new Exception(String.format("Index file(s) are expired for key %s.", key));
        }

        List<IndexMetadata> indices;
        try {
            indices = indexClient.readSplitIndex(key.getPath(), key.getIndexTypes());
        }
        catch (Exception e) {
            throw new Exception(String.format("No valid index file found for key %s.", key), e);
        }

        // lastModified file was valid, but no index files for the given types
        if (indices.isEmpty()) {
            throw new Exception(String.format("No %s index files found for key %s.", Arrays.toString(key.getIndexTypes()), key));
        }

        // Sort the indices based on split starting position
        return indices.stream()
                .sorted(comparingLong(IndexMetadata::getSplitStart))
                .collect(Collectors.toList());
    }
}
