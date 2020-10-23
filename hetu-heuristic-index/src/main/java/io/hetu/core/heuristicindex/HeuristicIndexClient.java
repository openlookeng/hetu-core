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
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.hetu.core.heuristicindex.util.IndexConstants;
import io.prestosql.spi.filesystem.FileBasedLock;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.hetu.core.heuristicindex.IndexRecord.COLUMN_DELIMITER;
import static java.util.Objects.requireNonNull;

/**
 * Class for reading and deleting the indices
 *
 * @since 2019-10-15
 */
public class HeuristicIndexClient
        implements IndexClient
{
    private static final HetuFileSystemClient LOCAL_FS_CLIENT = new HetuLocalFileSystemClient(
            new LocalConfig(new Properties()), Paths.get("/"));
    private static final Logger LOG = LoggerFactory.getLogger(HeuristicIndexClient.class);

    private HetuFileSystemClient fs;
    private Path root;
    private IndexRecordManager indexRecordManager;
    private Map<String, Index> indexTypesMap;

    public HeuristicIndexClient(Set<Index> indexTypes, HetuFileSystemClient fs, Path root)
    {
        this.fs = fs;
        this.root = root;
        this.indexRecordManager = new IndexRecordManager(fs, root);
        indexTypesMap = indexTypes.stream().collect(Collectors.toMap(
                type -> type.getId().toLowerCase(Locale.ENGLISH),
                Function.identity()));
    }

    @Override
    public List<IndexMetadata> readSplitIndex(String path)
            throws IOException
    {
        requireNonNull(path, "no path specified");

        List<IndexMetadata> indexes = new LinkedList<>();

        Path indexKeyPath = Paths.get(path);
        try {
            if (indexRecordManager.lookUpIndexRecord(indexKeyPath.subpath(0, 1).toString(),
                    new String[] {indexKeyPath.subpath(1, 2).toString()}, indexKeyPath.subpath(2, 3).toString()) == null) {
                // Use index record file to pre-screen. If record does not contain the index, skip loading
                return null;
            }
        }
        catch (Exception e) {
            // On exception, log and continue reading from disk
            LOG.debug("Error reading index records: " + path);
        }

        for (Map.Entry<String, Index> entry : readIndexMap(path).entrySet()) {
            String absolutePath = entry.getKey();
            Path remainder = Paths.get(absolutePath.replaceFirst(root.toString(), ""));
            Path table = remainder.subpath(0, 1);
            remainder = Paths.get(remainder.toString().replaceFirst(table.toString(), ""));
            Path column = remainder.subpath(0, 1);
            remainder = Paths.get(remainder.toString().replaceFirst(column.toString(), ""));
            Path indexType = remainder.subpath(0, 1);
            remainder = Paths.get(remainder.toString().replaceFirst(indexType.toString(), ""));

            Path filenamePath = remainder.getFileName();
            if (filenamePath == null) {
                throw new IllegalArgumentException("Split path cannot be resolved: " + path);
            }
            remainder = remainder.getParent();
            table = table.getFileName();
            column = column.getFileName();
            indexType = indexType.getFileName();
            if (remainder == null || table == null || column == null || indexType == null) {
                throw new IllegalArgumentException("Split path cannot be resolved: " + path);
            }

            String filename = filenamePath.toString();
            long splitStart = Long.parseLong(filename.substring(0, filename.lastIndexOf('.')));
            String timeDir = Paths.get(table.toString(), column.toString(), indexType.toString(), remainder.toString()).toString();
            long lastUpdated = getLastModified(timeDir);

            IndexMetadata index = new IndexMetadata(
                    entry.getValue(),
                    table.toString(),
                    new String[] {column.toString()},
                    root.toString(),
                    remainder.toString(),
                    splitStart,
                    lastUpdated);

            indexes.add(index);
        }

        return indexes;
    }

    @Override
    public long getLastModified(String path)
            throws IOException
    {
        // get the absolute path to the file being read
        Path absolutePath = Paths.get(root.toString(), path);

        try (Stream<Path> children = fs.list(absolutePath)) {
            for (Path child : (Iterable<Path>) children::iterator) {
                Path filenamePath = child.getFileName();
                if (filenamePath != null) {
                    String filename = filenamePath.toString();
                    if (filename.startsWith(IndexConstants.LAST_MODIFIED_FILE_PREFIX)) {
                        String timeStr = filename.replaceAll("\\D", "");
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

    @Override
    public void deleteIndex(String table, String[] columns, String indexType)
            throws IOException
    {
        Path toDelete = root.resolve(table).resolve(String.join(COLUMN_DELIMITER, columns)).resolve(indexType);

        if (!fs.exists(toDelete)) {
            return;
        }

        Lock lock = new FileBasedLock(fs, toDelete.getParent());
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                lock.unlock();
                try {
                    fs.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Error closing FileSystem Client: " + fs.getClass().getName(), e);
                }
            }));
            lock.lock();

            fs.deleteRecursively(toDelete);
        }
        finally {
            lock.unlock();
        }

        return;
    }

    private boolean notDirectory(Path path)
    {
        return !LOCAL_FS_CLIENT.isDirectory(path);
    }

    /**
     * Reads all files at the specified path recursively.
     * <br>
     * If the file extension matches a supported index type id, the index is loaded.
     * For example, if a file name is filename.bloom, then the file will be loaded
     * as a BloomIndex.
     *
     * @param path relative path to the index file or dir, if dir, it will be searched recursively (relative to the
     * root uri, if one was set)
     * @return an immutable mapping from all index files read to the corresponding index that was loaded
     * @throws IOException
     */
    private Map<String, Index> readIndexMap(String path)
            throws IOException
    {
        ImmutableMap.Builder<String, Index> result = ImmutableMap.builder();

        // get the absolute path to the file being read
        Path absolutePath = Paths.get(root.toString(), path);

        if (!fs.exists(absolutePath)) {
            return result.build();
        }

        try (Stream<Path> tarsOnRemote = fs.walk(absolutePath).filter(p -> p.toString().contains(".tar"))) {
            for (Path tarFile : (Iterable<Path>) tarsOnRemote::iterator) {
                try (TarArchiveInputStream i = new TarArchiveInputStream(fs.newInputStream(tarFile))) {
                    ArchiveEntry entry;
                    while ((entry = i.getNextEntry()) != null) {
                        if (!i.canReadEntryData(entry)) {
                            throw new FileSystemException("Unable to read archive entry: " + entry.toString());
                        }

                        String filename = entry.getName();

                        if (!filename.contains(".")) {
                            continue;
                        }

                        String indexType = filename.substring(filename.lastIndexOf('.') + 1).toLowerCase(Locale.ENGLISH);

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

                            index.deserialize(new CloseShieldInputStream(i));

                            LOG.debug("Loaded {} index from {}.", index.getId(), tarFile.toAbsolutePath());

                            result.put(tarFile.getParent().resolve(filename).toString(), index);
                        }
                    }
                }
            }
        }

        Map<String, Index> resultMap = result.build();

        return resultMap;
    }
}
