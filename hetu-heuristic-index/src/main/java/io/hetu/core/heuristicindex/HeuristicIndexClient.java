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
import io.hetu.core.heuristicindex.util.IndexServiceUtils;
import io.prestosql.spi.filesystem.FileBasedLock;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
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

import static java.util.Objects.requireNonNull;

/**
 * Class for reading and deleting the indices
 *
 * @since 2019-10-15
 */
public class HeuristicIndexClient
        implements IndexClient
{
    private static final Logger LOG = LoggerFactory.getLogger(HeuristicIndexClient.class);
    private static final HetuFileSystemClient LOCAL_FS_CLIENT = new HetuLocalFileSystemClient(
            new LocalConfig(new Properties()), Paths.get("/"));

    private HetuFileSystemClient fs;
    private Path root;
    private Map<String, Index> indexTypesMap;

    public HeuristicIndexClient(Set<Index> indexTypes, HetuFileSystemClient fs, Path root)
    {
        this.fs = fs;
        this.root = root;
        indexTypesMap = indexTypes.stream().collect(Collectors.toMap(
                type -> type.getId().toLowerCase(Locale.ENGLISH),
                Function.identity()));
    }

    @Override
    public List<IndexMetadata> readSplitIndex(String path, String... filterIndexTypes)
            throws IOException
    {
        requireNonNull(path, "no path specified");

        List<IndexMetadata> indexes = new LinkedList<>();

        for (Map.Entry<String, Index> entry : readIndexMap(path, filterIndexTypes).entrySet()) {
            String absolutePath = entry.getKey();
            Path remainder = Paths.get(absolutePath.replaceFirst(root.toString(), ""));
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
            long splitStart = Long.parseLong(filename.substring(0, filename.lastIndexOf('.')));
            String timeDir = Paths.get(table.toString(), column.toString(), remainder.toString()).toString();
            long lastUpdated = getLastModified(timeDir);

            IndexMetadata index = new IndexMetadata(
                    entry.getValue(),
                    table.toString(),
                    column.toString(),
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
    public void deleteIndex(String table, String[] columns)
            throws IOException
    {
        // get the parts just to validate the table name
        IndexServiceUtils.getTableParts(table);

        Path tablePath = root.resolve(table);
        Lock lock = new FileBasedLock(fs, tablePath);
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

            if (columns == null) {
                LOG.info("Deleted index for table {}", table);
                fs.deleteRecursively(tablePath);
            }
            else {
                for (String column : columns) {
                    Path indexFilePath = tablePath.resolve(column);
                    if (!fs.exists(indexFilePath)) {
                        LOG.warn("No index found for column {}", column);
                    }
                    else {
                        fs.deleteRecursively(indexFilePath);
                        LOG.info("Deleted index for column {}", column);
                    }
                }
            }
        }
        finally {
            lock.unlock();
        }
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
     * @param filterIndexTypes only load index types matching these types, if empty or null, all types will be loaded
     * @return an immutable mapping from all index files read to the corresponding index that was loaded
     * @throws IOException
     */
    private Map<String, Index> readIndexMap(String path, String... filterIndexTypes)
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
                Path localTmpDir = Files.createTempDirectory("tmp-index-dump-").toAbsolutePath();
                LOG.debug("Fetching index from remote filesystem to local: " + tarFile);
                IndexServiceUtils.unArchive(fs, LOCAL_FS_CLIENT, tarFile, localTmpDir);
                Path tarReadPath = Paths.get(localTmpDir.toString(), absolutePath.toString());

                try (Stream<Path> children = LOCAL_FS_CLIENT.walk(tarReadPath).filter(this::notDirectory)) {
                    for (Path child : (Iterable<Path>) children::iterator) {
                        LOG.debug("Processing file {}.", child);

                        Path childFilePath = child.getFileName();
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
                            try (InputStream is = LOCAL_FS_CLIENT.newInputStream(child)) {
                                index.load(is);
                            }
                            LOG.debug("Loaded {} index from {}.", index.getId(), child);
                            if (localTmpDir != null) {
                                // remove the temp folder path at the beginning
                                result.put(child.toString().replaceAll(localTmpDir.toString(), ""), index);
                            }
                            else {
                                result.put(child.toString(), index);
                            }
                        }
                    }
                }
                finally {
                    LOCAL_FS_CLIENT.deleteRecursively(localTmpDir);
                }
            }
        }

        Map<String, Index> resultMap = result.build();

        return resultMap;
    }
}
