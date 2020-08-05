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

import io.hetu.core.common.filesystem.FileBasedLock;
import io.hetu.core.heuristicindex.util.IndexConstants;
import io.hetu.core.heuristicindex.util.IndexServiceUtils;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.DataSource;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Class for persisting the index files to some filesystem
 *
 * @since 2019-10-11
 */
public class HeuristicIndexWriter
        implements IndexWriter
{
    private static final Logger LOG = LoggerFactory.getLogger(HeuristicIndexWriter.class);

    private static final String PART_FILE_SUFFIX = ".part";

    /**
     * Used for getting the database name from an array of ['catalog', 'schema', 'table']
     */
    private static final int DATABASE_NAME_INDEX = 1;
    private static final int TABLE_NAME_INDEX = 2;

    private DataSource dataSource;
    private Map<String, Index> indexTypesMap = new HashMap<>(1);
    private HetuFileSystemClient fs;

    private boolean isCleanedUp;
    private Path root;

    /**
     * Constructor
     *
     * @param dataSource that IndexWriter reads from
     * @param indexTypes Type of indexes that will be created(each type gets its own index)
     * @param fs filesystem client to access filesystem where the indexes are persisted/stored
     */
    public HeuristicIndexWriter(DataSource dataSource, Set<Index> indexTypes, HetuFileSystemClient fs, Path root)
    {
        this.dataSource = requireNonNull(dataSource);
        this.fs = requireNonNull(fs);
        this.root = root;
        for (Index indexType : indexTypes) {
            indexTypesMap.put(indexType.getId().toLowerCase(Locale.ENGLISH), indexType);
        }
    }

    @Override
    public void createIndex(String table, String[] columns, String[] partitions, String... indexTypes)
            throws IOException
    {
        createIndex(table, columns, partitions, indexTypes, true, false);
    }

    @Override
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

        // lock table so multiple callers can't index the same table
        Path tableIndexDirPath = root.resolve(table);

        Lock lock = null;
        if (lockingEnabled) {
            lock = new FileBasedLock(fs, tableIndexDirPath);
            // cleanup hook in case regular execution is interrupted
            Lock finalLock = lock;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                cleanPartFiles(partFiles);
                finalLock.unlock();
                try {
                    fs.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Error closing FileSystem Client: " + fs.getClass().getName(), e);
                }
            }));
            lock.lock();
        }

        // Each datasource will read the specified table's column and will use the callback when a split has been read.
        // The datasource will determine what a split is, for example for ORC, the datasource may read the file
        // and create a split for every 200000 rows.
        // The callback will also provide the values read from the split, these values will then be added to the index.
        // The datasource will also return the lastModified date of the split that was read
        try {
            dataSource.readSplits(databaseName, tableName, columns, partitions,
                    (column, values, uri, splitStart, lastModified) -> {
                        LOG.debug("split read: column={}; uri={}; splitOffset={}", column, uri, splitStart);

                        if (values == null || values.length == 0) {
                            LOG.debug("values were null or empty, skipping column={}; uri={}; splitOffset={}", column, uri, splitStart);
                            return;
                        }

                        Path columnIndexDirPath = tableIndexDirPath.resolve(column);
                        indexedColumns.add(column);

                        // save the indexes in a.part dir first, it will be moved later
                        URI uriObj = URI.create(uri);
                        String uriIndexDirPath = Paths.get(
                                columnIndexDirPath.toString(), uriObj.getPath()).toString();
                        partFiles.add(uriIndexDirPath); // store the path without the part suffix
                        uriIndexDirPath += PART_FILE_SUFFIX; // append the part suffix

                        String indexFileNamePrefix = splitStart + ".";

                        // write the source last modified time
                        String lastModifiedFileName = IndexConstants.LAST_MODIFIED_FILE_PREFIX + lastModified;
                        Path lastModifiedFilePath = Paths.get(uriIndexDirPath, lastModifiedFileName);
                        try {
                            if (!fs.exists(Paths.get(uriIndexDirPath))) {
                                fs.createDirectories(Paths.get(uriIndexDirPath));
                            }
                            try (OutputStream outputStream = fs.newOutputStream(lastModifiedFilePath)) {
                                outputStream.write(0);
                            }
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
                            Index indexTypeBaseObj = indexTypesMap.get(indexType.toLowerCase(Locale.ENGLISH));
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
                                splitIndex.setExpectedNumOfEntries(values.length);
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
                            Path indexFilePath = Paths.get(uriIndexDirPath, indexFileName);

                            splitIndex.addValues(values);

                            LOG.debug("writing split index to: {}", indexFilePath);
                            try (OutputStream outputStream = fs.newOutputStream(indexFilePath)) {
                                splitIndex.persist(outputStream);
                            }
                            catch (IOException e) {
                                LOG.error(String.format(Locale.ENGLISH,
                                        "error writing index file: %s", indexFilePath), e);
                                throw new UncheckedIOException("error writing index file: " + indexFilePath, e);
                            }

                            if (debugEnabled) {
                                String dataFileName = indexFileNamePrefix + "data";
                                Path dataFilePath = Paths.get(uriIndexDirPath, dataFileName);
                                LOG.debug("writing split data to: {}", dataFilePath);
                                try (OutputStream outputStream = fs.newOutputStream(dataFilePath)) {
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
            for (String original : partFiles) {
                Path originalDir = Paths.get(original);
                Path partDir = Paths.get(original + PART_FILE_SUFFIX);

                if (!fs.exists(partDir)) {
                    continue;
                }

                // 1. no original
                if (!fs.exists(originalDir)) {
                    fs.move(partDir, originalDir);
                }
                else {
                    long previousLastModifiedTime = getLastModifiedTime(originalDir);
                    long newLastModifiedTime = getLastModifiedTime(partDir);

                    // 2. expired original
                    if (previousLastModifiedTime != newLastModifiedTime) {
                        if (fs.exists(originalDir)) {
                            LOG.debug("Removing expired index at {}.", originalDir);
                            fs.deleteRecursively(originalDir);
                        }
                        fs.move(partDir, originalDir);
                    }
                    else {
                        // 3. merge
                        try (Stream<Path> children = fs.list(partDir)) {
                            for (Path child : (Iterable<Path>) children::iterator) {
                                String childName = child.getFileName().toString();
                                Path newPath = originalDir.resolve(childName);
                                LOG.debug("Moving {} to {}.", child, newPath);
                                // file "lastModified=..." with same name may exist
                                fs.deleteIfExists(newPath);
                                fs.move(child, newPath);
                            }

                            // should be empty now
                            fs.deleteRecursively(partDir);
                        }
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

            if (lock != null) {
                lock.unlock();
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
                    if (fs.exists(Paths.get(partDir)) && !fs.deleteRecursively(Paths.get(partDir))) {
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

    private long getLastModifiedTime(Path path) throws IOException
    {
        try (Stream<Path> children = fs.list(path).filter(this::notDirectory)) {
            for (Path child : (Iterable<Path>) children::iterator) {
                Path filenamePath = child.getFileName();
                if (filenamePath != null) {
                    String filename = filenamePath.toString();
                    if (filename.startsWith(IndexConstants.LAST_MODIFIED_FILE_PREFIX)) {
                        String timeStr = filename.substring(filename.lastIndexOf('=') + 1).toLowerCase(Locale.ENGLISH);
                        return Long.parseLong(timeStr);
                    }
                }
                else {
                    LOG.debug("File path not valid: {}", child);
                    return 0;
                }
            }
        }

        return 0;
    }

    private boolean notDirectory(Path path)
    {
        return !fs.isDirectory(path);
    }
}
