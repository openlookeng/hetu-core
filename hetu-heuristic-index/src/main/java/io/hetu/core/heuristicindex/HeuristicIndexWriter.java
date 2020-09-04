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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicDouble;
import io.hetu.core.common.util.SecurePathWhiteList;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.hetu.core.heuristicindex.util.IndexConstants;
import io.hetu.core.heuristicindex.util.IndexServiceUtils;
import io.prestosql.spi.filesystem.FileBasedLock;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.DataSource;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.hetu.core.heuristicindex.util.IndexServiceUtils.printVerboseMsg;
import static java.util.Objects.requireNonNull;

/**
 * Class for persisting the index files to some filesystem
 *
 * @since 2019-10-11
 */
public class HeuristicIndexWriter
        implements IndexWriter
{
    private static final int PROGRESS_BAR_LENGTH = 48;
    private static final HetuFileSystemClient LOCAL_FS_CLIENT = new HetuLocalFileSystemClient(
            new LocalConfig(new Properties()), Paths.get("/"));

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
    public void createIndex(String table, String[] columns, String[] partitions, String indexType)
            throws IOException
    {
        createIndex(table, columns, partitions, indexType, true);
    }

    @Override
    public void createIndex(String table, String[] columns, String[] partitions, String indexType, boolean parallelCreation)
            throws IOException
    {
        requireNonNull(table, "no table specified");
        requireNonNull(columns, "no columns specified");
        requireNonNull(indexType, "no index type specified");
        Index indexTypeBaseObj = indexTypesMap.get(indexType.toLowerCase(Locale.ENGLISH));
        if (indexTypeBaseObj == null) {
            String msg = String.format(Locale.ENGLISH, "Index type %s not supported.", indexType);
            throw new IllegalArgumentException(msg);
        }

        printVerboseMsg(String.format("Creating index for: table=%s columns=%s partitions=%s", table, Arrays.toString(columns),
                partitions == null ? "all" : Arrays.toString(partitions)));

        String[] parts = IndexServiceUtils.getTableParts(table);
        String databaseName = parts[DATABASE_NAME_INDEX];
        String tableName = parts[TABLE_NAME_INDEX];

        Path tmpPath = Files.createTempDirectory("tmp-indexwriter-");
        String strTmpPath = tmpPath.toString();
        printVerboseMsg("Local folder to hold temp index files: " + strTmpPath);

        Set<String> indexedColumns = ConcurrentHashMap.newKeySet();

        // the index files will first be stored in partFiles so that a file is
        // not modified while it is being read
        Set<String> partFiles = ConcurrentHashMap.newKeySet();

        // lock table so multiple callers can't index the same table
        Path tableIndexDirPath = Paths.get(strTmpPath, root.toString(), table);
        checkArgument(SecurePathWhiteList.isSecurePath(tableIndexDirPath.toString()),
                "Create index temp directory path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());

        Lock lock = null;
        if (!parallelCreation) {
            lock = new FileBasedLock(LOCAL_FS_CLIENT, tableIndexDirPath);
            // cleanup hook in case regular execution is interrupted
            Lock finalLock = lock;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                cleanPartFiles(partFiles);
                finalLock.unlock();
                try {
                    LOCAL_FS_CLIENT.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Error closing FileSystem Client: " + LOCAL_FS_CLIENT.getClass().getName(), e);
                }
            }));
            lock.lock();
        }
        AtomicDouble progress = new AtomicDouble();
        if (!IndexCommand.verbose) {
            System.out.print("\rProgress: [" + Strings.repeat(" ", PROGRESS_BAR_LENGTH) + "] 0%");
        }

        // Each datasource will read the specified table's column and will use the callback when a split has been read.
        // The datasource will determine what a split is, for example for ORC, the datasource may read the file
        // and create a split for every 200000 rows.
        // The callback will also provide the values read from the split, these values will then be added to the index.
        // The datasource will also return the lastModified date of the split that was read
        try {
            dataSource.readSplits(databaseName, tableName, columns, partitions,
                    (column, values, uri, splitStart, lastModified, currProgress) -> {
                        printVerboseMsg(String.format("split read: column=%s; uri=%s; splitOffset=%s", column, uri, splitStart));

                        if (values == null || values.length == 0) {
                            printVerboseMsg(String.format("values were null or empty, skipping column=%s; uri=%s; splitOffset=%s", column, uri, splitStart));
                            return;
                        }

                        // security check required before using values in a Path
                        if (!column.matches("[\\p{Alnum}_]+")) {
                            printVerboseMsg("Invalid column name " + column);
                            return;
                        }
                        try {
                            checkArgument(SecurePathWhiteList.isSecurePath(tableIndexDirPath.toString()),
                                    "Create index temp directory path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException("Get secure path list error", e);
                        }

                        // table/columns/indexType/
                        Path columnAndIndexTypePath = tableIndexDirPath.resolve(column).resolve(indexType);
                        indexedColumns.add(column);

                        // save the indexes in a.part dir first, it will be moved later
                        URI uriObj = URI.create(uri);
                        String uriIndexDirPath = Paths.get(
                                columnAndIndexTypePath.toString(), uriObj.getPath()).toString();
                        partFiles.add(uriIndexDirPath); // store the path without the part suffix
                        uriIndexDirPath += PART_FILE_SUFFIX; // append the part suffix

                        String indexFileNamePrefix = splitStart + ".";

                        // write the source last modified time
                        String lastModifiedFileName = IndexConstants.LAST_MODIFIED_FILE_PREFIX + lastModified;
                        Path lastModifiedFilePath = Paths.get(uriIndexDirPath, lastModifiedFileName);
                        try {
                            if (!LOCAL_FS_CLIENT.exists(Paths.get(uriIndexDirPath))) {
                                LOCAL_FS_CLIENT.createDirectories(Paths.get(uriIndexDirPath));
                            }
                            try (OutputStream outputStream = LOCAL_FS_CLIENT.newOutputStream(lastModifiedFilePath)) {
                                outputStream.write(0);
                            }
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException("error writing lastModified file: " + lastModifiedFilePath, e);
                        }

                        // write index files
                        // the indexTypesMap contains all the supported index types
                        // the instances in the map are the "base" instances bc they have their properties set
                        // we need to create a new Index instance for each split and copy the properties the base has
                        Index splitIndex;
                        try {
                            Constructor<? extends Index> constructor = indexTypeBaseObj.getClass().getConstructor();
                            splitIndex = constructor.newInstance();
                            splitIndex.setProperties(indexTypeBaseObj.getProperties());
                            splitIndex.setExpectedNumOfEntries(values.length);
                            printVerboseMsg(String.format("creating split index: %s", splitIndex.getId()));
                        }
                        catch (InstantiationException
                                | IllegalAccessException
                                | NoSuchMethodException
                                | InvocationTargetException e) {
                            throw new IllegalStateException("unable to create instance of index: " + indexType, e);
                        }

                        String indexFileName = indexFileNamePrefix + splitIndex.getId().toLowerCase(Locale.ENGLISH);
                        Path indexFilePath = Paths.get(uriIndexDirPath, indexFileName);

                        splitIndex.addValues(ImmutableMap.of(column, values));

                        printVerboseMsg(String.format("writing split index to: %s", indexFilePath));
                        try (OutputStream outputStream = LOCAL_FS_CLIENT.newOutputStream(indexFilePath)) {
                            splitIndex.serialize(outputStream);
                            if (!IndexCommand.verbose) {
                                currProgress *= 0.75 * PROGRESS_BAR_LENGTH;
                                synchronized (progress) {
                                    if (currProgress > progress.get()) {
                                        progress.addAndGet(currProgress - progress.get());
                                        int bars = (int) Math.ceil(currProgress);
                                        System.out.printf("\rProgress: [" + Strings.repeat("|", bars) + Strings.repeat(" ", PROGRESS_BAR_LENGTH - bars) + "] %d%%",
                                                bars * 100 / PROGRESS_BAR_LENGTH);
                                        System.out.flush();
                                    }
                                }
                            }
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException("error writing index file: " + indexFilePath, e);
                        }
                    });

            if (partFiles.isEmpty()) {
                System.out.println();
                String msg = "No index was created. Table may be empty.";
                System.out.println(msg);
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

                if (!LOCAL_FS_CLIENT.exists(partDir)) {
                    continue;
                }

                // Download index from indexstore
                Path originalOnTarget = Paths.get(original.replaceFirst(strTmpPath, ""));
                if (fs.exists(originalOnTarget)) {
                    try {
                        try (Stream<Path> tarsOnRemote = fs.walk(originalOnTarget).filter(p -> p.toString().contains(".tar"))) {
                            for (Path tarFile : (Iterable<Path>) tarsOnRemote::iterator) {
                                printVerboseMsg("Fetching index from target filesystem to local temp: " + tarFile);
                                IndexServiceUtils.unArchive(fs, LOCAL_FS_CLIENT, tarFile, tmpPath);
                                LOCAL_FS_CLIENT.createDirectories(Paths.get(strTmpPath, tarFile.getParent().toString()));
                                Files.createFile(Paths.get(strTmpPath, tarFile.toString().replaceAll("\\.tar", "")));
                            }
                        }
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException("error unarchiving file from remote", e);
                    }
                }

                // 1. no original
                if (!LOCAL_FS_CLIENT.exists(originalDir)) {
                    LOCAL_FS_CLIENT.move(partDir, originalDir);
                }
                else {
                    long previousLastModifiedTime = getLastModifiedTime(originalDir);
                    long newLastModifiedTime = getLastModifiedTime(partDir);

                    // 2. expired original
                    if (previousLastModifiedTime != newLastModifiedTime) {
                        if (LOCAL_FS_CLIENT.exists(originalDir)) {
                            printVerboseMsg(String.format("Removing expired index at %s.", originalDir));
                            LOCAL_FS_CLIENT.deleteRecursively(originalDir);
                        }
                        LOCAL_FS_CLIENT.move(partDir, originalDir);
                    }
                    else {
                        // 3. merge
                        try (Stream<Path> children = LOCAL_FS_CLIENT.list(partDir)) {
                            for (Path child : (Iterable<Path>) children::iterator) {
                                String childName = child.getFileName().toString();
                                Path newPath = originalDir.resolve(childName);
                                printVerboseMsg(String.format("Moving %s to %s.", child, newPath));
                                // file "lastModified=..." with same name may exist
                                LOCAL_FS_CLIENT.deleteIfExists(newPath);
                                LOCAL_FS_CLIENT.move(child, newPath);
                            }

                            // should be empty now
                            LOCAL_FS_CLIENT.deleteRecursively(partDir);
                        }
                    }
                }

                printVerboseMsg(String.format("Created index at %s.", originalDir));

                fs.deleteRecursively(originalOnTarget);
                IndexServiceUtils.archiveTar(LOCAL_FS_CLIENT, fs, originalDir, originalOnTarget);
                if (!IndexCommand.verbose) {
                    int bars = (int) Math.ceil(progress.addAndGet(0.25 * PROGRESS_BAR_LENGTH / partFiles.size()));
                    bars = Math.min(bars, PROGRESS_BAR_LENGTH);
                    System.out.printf("\rProgress: [" + Strings.repeat("|", bars) + Strings.repeat(" ", PROGRESS_BAR_LENGTH - bars) + "] %d%%",
                            bars * 100 / PROGRESS_BAR_LENGTH);
                    System.out.flush();
                }
            }

            for (String indexedColumn : indexedColumns) {
                printVerboseMsg(String.format("Created index for column %s.", indexedColumn));
            }
        }
        finally {
            cleanPartFiles(partFiles);

            if (lock != null) {
                lock.unlock();
            }

            printVerboseMsg("Deleting local tmp folder: " + strTmpPath);
            LOCAL_FS_CLIENT.deleteRecursively(tmpPath);
        }
    }

    private void cleanPartFiles(Collection<String> partFiles)
    {
        if (!isCleanedUp) {
            Set<String> failedDeletes = new HashSet<>(1);

            for (String partFile : partFiles) {
                String partDir = partFile + PART_FILE_SUFFIX;
                try {
                    if (LOCAL_FS_CLIENT.exists(Paths.get(partDir)) && !LOCAL_FS_CLIENT.deleteRecursively(Paths.get(partDir))) {
                        failedDeletes.add(partDir);
                    }
                }
                catch (IOException e) {
                    failedDeletes.add(partDir);
                }
            }

            if (!failedDeletes.isEmpty()) {
                printVerboseMsg("Failed to delete the following files, please delete them manually.");
                failedDeletes.forEach(IndexServiceUtils::printVerboseMsg);
            }

            isCleanedUp = true;
        }
    }

    private long getLastModifiedTime(Path path)
            throws IOException
    {
        try (Stream<Path> children = LOCAL_FS_CLIENT.list(path).filter(this::notDirectory)) {
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
                    printVerboseMsg(String.format("File path not valid: %s", child));
                    return 0;
                }
            }
        }

        return 0;
    }

    private boolean notDirectory(Path path)
    {
        return !LOCAL_FS_CLIENT.isDirectory(path);
    }
}
