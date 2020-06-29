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

import com.google.common.collect.ImmutableMap;
import io.hetu.core.heuristicindex.util.ConstantHelper;
import io.hetu.core.heuristicindex.util.IndexServiceUtils;
import io.hetu.core.spi.heuristicindex.Index;
import io.hetu.core.spi.heuristicindex.IndexStore;
import io.hetu.core.spi.heuristicindex.SplitIndexMetadata;
import io.hetu.core.spi.heuristicindex.SplitMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Class for reading and deleting the indices
 *
 * @since 2019-10-15
 */
public class IndexClient
{
    private static final Logger LOG = LoggerFactory.getLogger(IndexClient.class);

    private IndexStore indexStore;
    private Map<String, Index> indexTypesMap;

    /**
     * Injected Constructor
     *
     * @param indexTypes Types to create indexes for(each type gets its own index)
     * @param indexStore Where to persist/store the indexes
     */
    @Inject
    public IndexClient(Set<Index> indexTypes, IndexStore indexStore)
    {
        this.indexStore = indexStore;
        indexTypesMap = indexTypes.stream().collect(Collectors.toMap(
                type -> type.getId().toLowerCase(Locale.ENGLISH),
                Function.identity()));
    }

    /**
     * <pre>
     * Reads all split indexes at the specified path recursively.
     * It is assumed that the provided path is relative to the indexstore root uri, if one was set
     *
     * All split indexes are loaded into Index objects based on their file extensions.
     * For example, if a file name is filename.bloom, then the file will be loaded
     * as a BloomIndex.
     *
     * The split file paths must match the following pattern:
     * <b>IndexStore root uri</b>/<b>table</b>/<b>column</b>/<b>source file path</b>/<b>index file</b>
     *
     * where <i>index file</i> is of format:
     *
     * <i>source file name</i>#<i>splitStart</i>.<i>indexType</i>
     *
     * for example:
     * /tmp/hetu/indices/hive.farhan.index_test_1k/g5/user/hive/warehouse/farhan.db/index_test_1k/20190904_221627_0
     * 0003_ygvh9_1e9b8c7c-cbe8-4ff2-a1d5-73e3fecd1138/20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-
     * a1d5-73e3fecd1138#0.bloom
     *
     * where:
     * IndexStore root uri = /tmp/hetu/indices
     * table = hive.farhan.index_test_1k
     * column = g5
     * source file path = /user/hive/warehouse/farhan.db/index_test_1k/20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-
     * a1d5-73e3fecd1138
     * index file = 20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-a1d5-73e3fecd1138#0.bloom
     *
     * </pre>
     *
     * @param path             relative path to the split index file or dir, if dir, it will be searched recursively (relative to \
     *                         the indexstore root uri, if one was set)
     * @param filterIndexTypes only load index types matching these types, if empty or null, all types will be loaded
     * @return all split indexes that were read, with the split metadata set based on the split path
     * @throws IOException thrown by doing IO operations using IndexStore
     */
    public List<SplitIndexMetadata> readSplitIndex(String path, String... filterIndexTypes) throws IOException
    {
        requireNonNull(path, "no path specified");

        String indexRootUri = indexStore.getProperties().getProperty(IndexStore.ROOT_URI_KEY);

        List<SplitIndexMetadata> indexes = new LinkedList<>();

        for (Map.Entry<String, Index> entry : readIndexMap(path, filterIndexTypes).entrySet()) {
            String absolutePath = entry.getKey();
            Path remainder = Paths.get(absolutePath.replaceFirst(indexRootUri, ""));
            Path table = remainder.subpath(0, 1);
            remainder = Paths.get(remainder.toString().replaceFirst(table.toString(), ""));
            Path column = remainder.subpath(0, 1);
            remainder = Paths.get(remainder.toString().replaceFirst(column.toString(), ""));

            Path filenamePath = remainder.getFileName();
            if (filenamePath == null) {
                throw new IllegalArgumentException("Split path cannot be resolved: " + path);
            }
            remainder = remainder.getParent();
            table = table.getFileName();
            column = column.getFileName();
            if (remainder == null || table == null || column == null) {
                throw new IllegalArgumentException("Split path cannot be resolved: " + path);
            }

            String filename = filenamePath.toString();
            long splitStart = Long.parseLong(filename.substring(0, filename.lastIndexOf('.')).split("#")[1]);
            String timeDir = Paths.get(table.toString(), column.toString(),
                    remainder.toString()).toString();
            long lastUpdated = getLastModified(timeDir);

            SplitMetadata split = new SplitMetadata(
                    table.toString(),
                    column.toString(),
                    Paths.get(indexRootUri).toString(),
                    remainder.toString(),
                    splitStart);
            SplitIndexMetadata index = new SplitIndexMetadata(entry.getValue(), split, lastUpdated);

            indexes.add(index);
        }

        return indexes;
    }

    /**
     * Reads all files at the specified path recursively.
     * <br>
     * If the file extension matches a supported index type id, the index is loaded.
     * For example, if a file name is filename.bloom, then the file will be loaded
     * as a BloomIndex.
     *
     * @param path             relative path to the index file or dir, if dir, it will be searched recursively (relative to the
     *                         indexstore root uri, if one was set)
     * @param filterIndexTypes only load index types matching these types, if empty or null, all types will be loaded
     * @return an immutable mapping from all index files read to the corresponding index that was loaded
     * @throws IOException
     */
    private Map<String, Index> readIndexMap(String path, String... filterIndexTypes) throws IOException
    {
        ImmutableMap.Builder<String, Index> result = ImmutableMap.builder();

        // get the absolute path to the file being read
        String indexRootUri = indexStore.getProperties().getProperty(IndexStore.ROOT_URI_KEY);
        String absolutePath = Paths.get(indexRootUri, path).toString();

        for (String child : indexStore.listChildren(absolutePath, true)) {
            LOG.debug("Processing file {}.", child);

            Path childFilePath = Paths.get(child).getFileName();
            if (childFilePath == null) {
                throw new IllegalStateException("Path cannot be resolved: " + child);
            }
            String filename = childFilePath.toString();

            if (!filename.contains(".")) {
                continue;
            }

            String indexType = filename.substring(filename.lastIndexOf('.') + 1).toLowerCase(Locale.ENGLISH);

            if (filterIndexTypes != null && filterIndexTypes.length != 0) {
                // check if indexType matches any of the indexTypes expected to be loaded
                boolean found = false;
                for (int i = 0; i < filterIndexTypes.length; i++) {
                    if (filterIndexTypes[i].equalsIgnoreCase(indexType)) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    continue;
                }
            }

            Index index = indexTypesMap.get(indexType);
            if (index != null) {
                try {
                    Constructor<? extends Index> constructor = index.getClass().getConstructor();
                    index = constructor.newInstance();
                }
                catch (InstantiationException | IllegalAccessException | InvocationTargetException
                        | NoSuchMethodException e) {
                    throw new IOException(e);
                }
                try (InputStream is = indexStore.read(child)) {
                    index.load(is);
                }
                LOG.debug("Loaded {} index from {}.", index.getId(), child);
                result.put(child, index);
            }
        }

        Map<String, Index> resultMap = result.build();

        return resultMap;
    }

    /**
     * Searches the path for lastModified file and returns the value as a long.
     * The filename is expected to be in the form: lastModified=123456.
     * <p>
     * If the path is a directory, only the direct children will be searched
     * (i.e. not recursively).
     * <p>
     * If multiple lastModified files are present, only the first one will be read
     *
     * @param path URI to the file
     * @return last modified time or 0 if not found.
     * @throws IOException thrown by IndexStore
     */
    public long getLastModified(String path) throws IOException
    {
        // get the absolute path to the file being read
        String indexRootUri = indexStore.getProperties().getProperty(IndexStore.ROOT_URI_KEY);
        String absolutePath = Paths.get(indexRootUri, path).toString();

        for (String child : indexStore.listChildren(absolutePath, false)) {
            Path filenamePath = Paths.get(child).getFileName();
            if (filenamePath != null) {
                String filename = filenamePath.toString();
                if (filename.startsWith(ConstantHelper.LAST_MODIFIED_FILE_PREFIX)) {
                    String timeStr = filename.substring(filename.lastIndexOf('=') + 1).toLowerCase(Locale.ENGLISH);
                    return Long.parseLong(timeStr);
                }
            }
            else {
                LOG.debug("File path not valid: {}", child);
                return 0;
            }
        }

        return 0;
    }

    /**
     * Delete the indexes for the table, if columns are specified, only indexes
     * for the specified columns will be deleted.
     *
     * @param table   fully qualified table name
     * @param columns columns to delete index of, if null the indexes for the entire
     *                table are deleted
     * @throws IOException any IOException thrown by IndexStore object during file deletion
     */
    public void deleteIndex(String table, String[] columns) throws IOException
    {
        // get the parts just to validate the table name
        IndexServiceUtils.getTableParts(table);

        String indexRootUri = indexStore.getProperties().getProperty(IndexStore.ROOT_URI_KEY);
        String tablePath = Paths.get(indexRootUri, table).toString();
        IndexLock lockFile = new IndexLock(indexStore, tablePath);
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                lockFile.release();
                try {
                    indexStore.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Error closing IndexStore: " + indexStore.getId(), e);
                }
            }));
            lockFile.lock();

            if (columns == null) {
                LOG.info("Deleted index for table {}", table);
                indexStore.delete(tablePath);
            }
            else {
                for (String column : columns) {
                    String indexFilePath = Paths.get(tablePath, column).toString();
                    if (!indexStore.exists(indexFilePath)) {
                        LOG.warn("No index found for column {}", column);
                    }
                    else {
                        indexStore.delete(indexFilePath);
                        LOG.info("Deleted index for column {}", column);
                    }
                }
            }
        }
        finally {
            lockFile.release();
        }
    }
}
