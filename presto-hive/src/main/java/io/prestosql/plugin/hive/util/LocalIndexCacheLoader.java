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
import io.hetu.core.heuristicindex.IndexClient;
import io.hetu.core.heuristicindex.IndexFactory;
import io.hetu.core.spi.heuristicindex.SplitIndexMetadata;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.service.PropertyService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public class LocalIndexCacheLoader
        extends CacheLoader<IndexCacheKey, List<SplitIndexMetadata>>
{
    private static IndexClient indexClient;

    // Initialize indexClient in a static block to ensure it only gets initialized once
    // Also, this ensures the correct thread loads in the plugins for the indexer
    // As only the thread that injected this class is able to load the plugins
    static {
        if (PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED)) {
            IndexFactory factory = IndexFactory.getInstance();

            List<String> plugins = PropertyService.getCommaSeparatedList(HetuConstant.FILTER_PLUGINS);
            try {
                factory.loadPlugins(plugins.toArray(new String[0]));
            }
            catch (IOException e) {
                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Failed to load Indexer plugins", e);
            }
            indexClient = factory.getIndexClient(getIndexStoreProperties());
        }
    }

    public LocalIndexCacheLoader()
    {
    }

    public LocalIndexCacheLoader(IndexClient client)
    {
        indexClient = client;
    }

    @Override
    public List<SplitIndexMetadata> load(IndexCacheKey key)
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
            throw new Exception(String.format("No index file found for key %s.", key), e.getCause());
        }

        if (lastModified != key.getLastModifiedTime()) {
            throw new Exception(String.format("Index file(s) are expired for key %s.", key));
        }

        List<SplitIndexMetadata> indices;
        try {
            indices = indexClient.readSplitIndex(key.getPath(), key.getIndexTypes());
        }
        catch (Exception e) {
            throw new Exception(String.format("No valid index file found for key %s. %s", key, e.getMessage()), e.getCause());
        }

        // lastModified file was valid, but no index files for the given types
        if (indices.isEmpty()) {
            throw new Exception(String.format("No %s index files found for key %s.", Arrays.toString(key.getIndexTypes()), key));
        }

        // Sort the indices based on split starting position
        return indices.stream()
                .sorted(comparingLong(SplitIndexMetadata::getSplitStart))
                .collect(Collectors.toList());
    }

    protected static Properties getIndexStoreProperties()
    {
        Properties properties = new Properties();

        if (PropertyService.containsProperty(HetuConstant.INDEXSTORE_URI)) {
            properties.setProperty(
                    HetuConstant.INDEXSTORE_URI.replaceFirst(HetuConstant.INDEXSTORE_KEYS_PREFIX, ""),
                    PropertyService.getStringProperty(HetuConstant.INDEXSTORE_URI));
        }

        if (PropertyService.containsProperty(HetuConstant.INDEXSTORE_TYPE)) {
            properties.setProperty(
                    HetuConstant.INDEXSTORE_TYPE.replaceFirst(HetuConstant.INDEXSTORE_KEYS_PREFIX, ""),
                    PropertyService.getStringProperty(HetuConstant.INDEXSTORE_TYPE));
        }

        if (PropertyService.containsProperty(HetuConstant.INDEXSTORE_HDFS_CONFIG_RESOURCES)) {
            properties.setProperty(HetuConstant.INDEXSTORE_HDFS_CONFIG_RESOURCES.replaceFirst(
                    HetuConstant.INDEXSTORE_KEYS_PREFIX, ""),
                    PropertyService.getStringProperty(HetuConstant.INDEXSTORE_HDFS_CONFIG_RESOURCES));
        }

        if (PropertyService.containsProperty(HetuConstant.INDEXSTORE_HDFS_AUTHENTICATION_TYPE)) {
            properties.setProperty(HetuConstant.INDEXSTORE_HDFS_AUTHENTICATION_TYPE.replaceFirst(
                    HetuConstant.INDEXSTORE_KEYS_PREFIX, ""),
                    PropertyService.getStringProperty(HetuConstant.INDEXSTORE_HDFS_AUTHENTICATION_TYPE));
        }

        if (PropertyService.containsProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_CONFIG_PATH)) {
            properties.setProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_CONFIG_PATH.replaceFirst(
                    HetuConstant.INDEXSTORE_KEYS_PREFIX, ""),
                    PropertyService.getStringProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_CONFIG_PATH));
        }

        if (PropertyService.containsProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_KEYTAB_PATH)) {
            properties.setProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_KEYTAB_PATH.replaceFirst(
                    HetuConstant.INDEXSTORE_KEYS_PREFIX, ""),
                    PropertyService.getStringProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_KEYTAB_PATH));
        }

        if (PropertyService.containsProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_PRINCIPAL)) {
            properties.setProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_PRINCIPAL.replaceFirst(
                    HetuConstant.INDEXSTORE_KEYS_PREFIX, ""),
                    PropertyService.getStringProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_PRINCIPAL));
        }

        return properties;
    }
}
