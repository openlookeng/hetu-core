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

import io.hetu.core.heuristicindex.util.ConstantHelper;
import io.hetu.core.heuristicindex.util.IndexServiceUtils;
import io.hetu.core.spi.heuristicindex.DataSource;
import io.hetu.core.spi.heuristicindex.Index;
import io.hetu.core.spi.heuristicindex.IndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Class for persisting the index files to some IndexStore objects
 *
 * @since 2019-10-11
 */
public class IndexWriter
{
    private static final Logger LOG = LoggerFactory.getLogger(IndexWriter.class);

    private static final String PART_FILE_SUFFIX = ".part";

    /**
     * Used for getting the database name from an array of ['catalog', 'schema', 'table']
     */
    private static final int DATABASE_NAME_INDEX = 1;
    private static final int TABLE_NAME_INDEX = 2;

    private DataSource dataSource;
    private Map<String, Index> indexTypesMap = new HashMap<>(1);
    private IndexStore indexStore;

    private boolean isCleanedUp;

    /**
     * Constructor
     *
     * @param dataSource that IndexWriter reads from
     * @param indexTypes Type of indexes that will be created(each type gets its own index)
     * @param indexStore where the indexes are persisted/stored
     */
    @Inject
    public IndexWriter(DataSource dataSource, Set<Index> indexTypes, IndexStore indexStore)
    {
        this.dataSource = requireNonNull(dataSource);
        this.indexStore = requireNonNull(indexStore);
        for (Index indexType : indexTypes) {
            indexTypesMap.put(indexType.getId().toLowerCase(Locale.ENGLISH), indexType);
        }
    }

    public void createIndex(String table, String[] columns, String[] partitions, String... indexTypes) throws IOException
    {
        createIndex(table, columns, partitions, indexTypes, true, false);
    }

    /**
     * <pre>
     * Creates the index for the specified columns. Filters on partitions if specified.
     *
     * The configured DataSource is responsible for reading the column values.
     * Indexes are created for each specified type and populated with columns
     * and the Index is written to the IndexStore.
     *
     * The index files will be stored at:
     * [IndexStore root uri]/[table]/[column]/[source file path]/[index file]
     *
     * where [index file] is of format:
     *
     * [source file name]#[splitStart].[indexType]
     *
     * for example:
     * /tmp/hetu/indices/hive.farhan.index_test_1k/g5/user/hive/warehouse/farhan.db/index_test_1k/20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-a1d5-73e3fecd1138/20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-a1d5-73e3fecd1138#0.bloom
     *
     * where:
     *
     * IndexStore root uri = /tmp/hetu/indices
     * table = hive.farhan.index_test_1k
     * column = g5
     * source file path = <!--
     * -->/user/hive/warehouse/farhan.db/index_test_1k/20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-\a1d5-73e3fecd1138
     * index file = 20190904_221627_00003_ygvh9_1e9b8c7c-cbe8-4ff2-a1d5-73e3fecd1138#0.bloom
     *
     * the index file is of type bloom and the splt has a start of 0
     * </pre>
     *
     * @param table          fully qualified table name
     * @param columns        columns to index
     * @param partitions     only index specified partitions, if null, index all partitions
     * @param indexTypes     type of the index to be created (its string ID returned from {@link Index#getId()})
     * @param lockingEnabled if enabled, the table will be locked and multiple callers can't create index for the table in parallel
     * @param debugEnabled   writes the raw split data to a file alongside the index file
     * @throws IOException thrown during index creation
     */
    public void createIndex(String table, String[] columns, String[] partitions, String[] indexTypes, boolean lockingEnabled, boolean debugEnabled)
            throws IOException
    {
        requireNonNull(table, "no table specified");
        requireNonNull(columns, "no columns specified");
        requireNonNull(indexTypes, "no index types specified");

        LOG.info("Creating index for: table={} columns={} partitions={}", table, Arrays.toString(columns),
                partitions == null ? "all" : Arrays.toString(partitions));

        String[] parts = IndexServiceUtils.getTableParts(table);
        String databaseName = parts[DATABASE_NAME_INDEX];
        String tableName = parts[TABLE_NAME_INDEX];

        Set<String> indexedColumns = ConcurrentHashMap.newKeySet();

        // the index files will first be stored in partFiles so that a file is
        // not modified while it is being read
        Set<String> partFiles = ConcurrentHashMap.newKeySet();

        String indexRootUri = indexStore.getProperties().getProperty(IndexStore.ROOT_URI_KEY);
        String tableIndexDirPath = Paths.get(indexRootUri, table).toString();

        // lock table if enabled so multiple callers can't index the same table
        IndexLock lockFile = null;
        if (lockingEnabled) {
            lockFile = new IndexLock(indexStore, tableIndexDirPath);
            // cleanup hook in case regular execution is interrupted
            IndexLock finalLockFile = lockFile;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                cleanPartFiles(partFiles);
                finalLockFile.release();
                try {
                    indexStore.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Error closing the IndexStore: " + indexStore.getId(), e);
                }
            }));
            lockFile.lock();
        }

        // Each datasource will read the specified table's column and will use the callback when a split has been read.
        // The datasource will determine what a split is, for example for ORC, the datasource may read the file
        // and create a split for stripe
        // The callback will also provide the values read from the split, these values will then be added to the index.
        // The datasource will also return the lastModified date of the split that was read
        try {
            dataSource.readSplits(databaseName, tableName, columns, partitions,
                    (column, values, uri, splitStart, lastModified) -> {
                        LOG.debug("split read: column={}; uri={}; splitOffset={}", column, uri, splitStart);

                        if (values == null) {
                            LOG.debug("values were null, skipping column={}; uri={}; splitOffset={}", column, uri, splitStart);
                            return;
                        }

                        String columnIndexDirPath = Paths.get(tableIndexDirPath, column).toString();
                        indexedColumns.add(column);

                        // save the indexes in a.part dir first, it will be moved later
                        URI uriObj = URI.create(uri);
                        String uriIndexDirPath = Paths.get(
                                columnIndexDirPath, uriObj.getPath()).toString();
                        partFiles.add(uriIndexDirPath); // store the path without the part suffix
                        uriIndexDirPath += PART_FILE_SUFFIX; // append the part suffix

                        String indexFileNamePrefix = Paths.get(uri).getFileName() + "#" + splitStart + ".";

                        // write the source last modified time
                        String lastModifiedFileName = ConstantHelper.LAST_MODIFIED_FILE_PREFIX + lastModified;
                        String lastModifiedFilePath = Paths.get(uriIndexDirPath, lastModifiedFileName).toString();
                        try (OutputStream outputStream = indexStore.getOutputStream(lastModifiedFilePath, true)) {
                            outputStream.write(0);
                        }
                        catch (IOException e) {
                            LOG.error(String.format(Locale.ENGLISH,
                                    "error writing lastModified file: %s", lastModifiedFilePath), e);
                            throw new UncheckedIOException("error writing lastModified file: " + lastModifiedFilePath, e);
                        }

                        // write index files
                        for (String indexType : indexTypes) {
                            // the indexTypesMap contains all the supported index types
                            // the instances in the map are the "base" instances bc they have their properties set
                            // we need to create a new Index instance for each split and copy the properties the base has
                            Index indexTypeBaseObj = indexTypesMap.get(indexType);
                            if (indexTypeBaseObj == null) {
                                String msg = String.format(Locale.ENGLISH, "Index type %s not supported.", indexType);
                                LOG.error(msg);
                                throw new IllegalArgumentException(msg);
                            }

                            Index splitIndex;
                            try {
                                Constructor<? extends Index> constructor = indexTypeBaseObj.getClass().getConstructor();
                                splitIndex = constructor.newInstance();
                                splitIndex.setProperties(indexTypeBaseObj.getProperties());
                                LOG.debug("creating split index: {}", splitIndex.getId());
                            }
                            catch (InstantiationException
                                    | IllegalAccessException
                                    | NoSuchMethodException
                                    | InvocationTargetException e) {
                                LOG.error("unable to create instance of index: ", e);
                                throw new IllegalStateException("unable to create instance of index: " + indexType, e);
                            }

                            String indexFileName = indexFileNamePrefix + splitIndex.getId().toLowerCase(Locale.ENGLISH);
                            String indexFilePath = Paths.get(uriIndexDirPath, indexFileName).toString();

                            splitIndex.addValues(values);

                            LOG.debug("writing split index to: {}", indexFilePath);
                            try (OutputStream outputStream = indexStore.getOutputStream(indexFilePath, true)) {
                                splitIndex.persist(outputStream);
                            }
                            catch (IOException e) {
                                LOG.error(String.format(Locale.ENGLISH,
                                        "error writing index file: %s", indexFilePath), e);
                                throw new UncheckedIOException("error writing index file: " + indexFilePath, e);
                            }

                            if (debugEnabled) {
                                String dataFileName = indexFileNamePrefix + "data";
                                String dataFilePath = Paths.get(uriIndexDirPath, dataFileName).toString();
                                LOG.debug("writing split data to: {}", dataFilePath);
                                try (OutputStream outputStream = indexStore.getOutputStream(dataFilePath, true)) {
                                    for (int i = 0; i < values.length; i++) {
                                        outputStream.write(values[i] == null ? "NULL".getBytes() : values[i].toString().getBytes());
                                        outputStream.write('\n');
                                    }
                                }
                                catch (IOException e) {
                                    LOG.error(String.format(Locale.ENGLISH,
                                            "error writing data file: %s", dataFilePath), e);
                                    throw new UncheckedIOException("error writing data file: " + dataFilePath, e);
                                }
                            }
                        }
                    });

            if (partFiles.isEmpty()) {
                String msg = "No index was created. Table may be empty.";
                LOG.error(msg);
                throw new IllegalStateException(msg);
            }

            // move all part dirs
            // originalDir will be something like: /tmp/indicies/catalog.schema.table/UT_test_column/UT_test
            // partDir will be something like: /tmp/indicies/catalog.schema.table/UT_test_column/UT_test.part
            // 1. if original does not exist, simply rename part dir
            // i.e. /tmp/indicies/catalog.schema.table/UT_test_column/UT_test.part -> /tmp/indicies/catalog.schema.table/UT_test_column/UT_test
            // 2. if original exists but has a different lastModifiedTime, delete original and replace it
            // this is because if the original dir's lastModifiedTime is different, the indexes in the dir are no long valid
            // 3. if original exists and has same lastModifiedTime, merge the two dirs
            // i.e. move the files from /tmp/indicies/catalog.schema.table/UT_test_column/UT_test.part to
            // /tmp/indicies/catalog.schema.table/UT_test_column/UT_test
            for (String originalDir : partFiles) {
                String partDir = originalDir + PART_FILE_SUFFIX;

                if (!indexStore.exists(partDir)) {
                    continue;
                }

                // 1. no original
                if (!indexStore.exists(originalDir)) {
                    indexStore.renameTo(partDir, originalDir);
                }
                else {
                    long previousLastModifiedTime = getLastModifiedTime(originalDir);
                    long newLastModifiedTime = getLastModifiedTime(partDir);

                    // 2. expired original
                    if (previousLastModifiedTime != newLastModifiedTime) {
                        if (indexStore.exists(originalDir)) {
                            LOG.debug("Removing expired index at {}.", originalDir);
                            indexStore.delete(originalDir);
                        }
                        indexStore.renameTo(partDir, originalDir);
                    }
                    else {
                        // 3. merge
                        for (String childPath : indexStore.listChildren(partDir, false)) {
                            String childName = Paths.get(childPath).getFileName().toString();
                            String newPath = Paths.get(originalDir, childName).toString();
                            LOG.debug("Moving {} to {}.", childPath, newPath);
                            indexStore.renameTo(childPath, newPath);
                        }

                        // should be empty now
                        indexStore.delete(partDir);
                    }
                }

                LOG.debug("Created index at {}.", originalDir);
            }

            for (String indexedColumn : indexedColumns) {
                LOG.info("Created index for column {}.", indexedColumn);
            }
        }
        finally {
            cleanPartFiles(partFiles);

            if (lockFile != null) {
                lockFile.release();
            }
        }
    }

    private void cleanPartFiles(Collection<String> partFiles)
    {
        if (!isCleanedUp) {
            Set<String> failedDeletes = new HashSet<>(1);

            for (String partFile : partFiles) {
                String partDir = partFile + PART_FILE_SUFFIX;
                try {
                    if (indexStore.exists(partDir) && !indexStore.delete(partDir)) {
                        failedDeletes.add(partDir);
                    }
                }
                catch (IOException e) {
                    failedDeletes.add(partDir);
                }
            }

            if (!failedDeletes.isEmpty()) {
                LOG.warn("Failed to delete the following files, please delete them manually.");
                failedDeletes.forEach(LOG::warn);
            }

            isCleanedUp = true;
        }
    }

    private long getLastModifiedTime(String path) throws IOException
    {
        for (String child : indexStore.listChildren(path, false)) {
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
}
