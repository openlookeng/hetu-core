/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.snapshot;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import io.airlift.log.Logger;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.spi.filesystem.HetuFileSystemClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * SnapshotStoreFileBased is an implementation of SnapshotStoreClient.
 * It uses HetuFileSystemClient to connect to File System.
 */
public class SnapshotFileBasedClient
        implements SnapshotStoreClient
{
    private static final Logger LOG = Logger.get(SnapshotFileBasedClient.class);

    private final HetuFileSystemClient fsClient;
    private final FileSystemClientManager fileSystemClientManager;
    private final Path rootPath;
    private final boolean useKryo;
    private final String spillProfile;
    private final boolean spillToHdfs;

    public SnapshotFileBasedClient(HetuFileSystemClient fsClient, Path rootPath, FileSystemClientManager fileSystemClientManager, String spillProfile, boolean spillToHdfs, boolean useKryo)
    {
        this.fsClient = fsClient;
        this.rootPath = rootPath;
        this.fileSystemClientManager = fileSystemClientManager;
        this.spillProfile = spillProfile;
        this.spillToHdfs = spillToHdfs;
        this.useKryo = useKryo;
    }

    @Override
    public void storeState(SnapshotStateId snapshotStateId, Object state, SnapshotDataCollector dataCollector)
            throws IOException
    {
        Stopwatch timer = Stopwatch.createStarted();
        Path file = SnapshotUtils.createStatePath(rootPath, snapshotStateId.getHierarchy());

        fsClient.createDirectories(file.getParent());

        try (OutputStream outputStream = fsClient.newOutputStream(file)) {
            SnapshotUtils.serializeState(state, outputStream, useKryo);
        }
        timer.stop();
        if (dataCollector != null) {
            long snapshotId = snapshotStateId.getSnapshotId();
            Long size = (Long) fsClient.getAttribute(file, "size");
            if (size != null) {
                dataCollector.updateSnapshotCaptureSize(snapshotId, size.longValue());
            }
            dataCollector.updateSnapshotCaptureCpuTime(snapshotId, timer.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public Optional<Object> loadState(SnapshotStateId snapshotStateId, SnapshotDataCollector dataCollector)
            throws IOException, ClassNotFoundException
    {
        Optional<Object> result;
        Stopwatch timer = Stopwatch.createStarted();
        Path file = SnapshotUtils.createStatePath(rootPath, snapshotStateId.getHierarchy());
        if (!fsClient.exists(file)) {
            return Optional.empty();
        }

        try (InputStream inputStream = fsClient.newInputStream(file)) {
            result = Optional.of(SnapshotUtils.deserializeState(inputStream, useKryo));
        }
        timer.stop();
        if (dataCollector != null) {
            Long size = (Long) fsClient.getAttribute(file, "size");
            dataCollector.updateSnapshotRestoreSize(size.longValue());
            dataCollector.updateSnapshotRestoreCpuTime(timer.elapsed(TimeUnit.MILLISECONDS));
        }
        return result;
    }

    @Override
    public void storeFile(SnapshotStateId snapshotStateId, Path sourceFile, SnapshotDataCollector dataCollector)
            throws IOException
    {
        Stopwatch timer = Stopwatch.createStarted();
        List<String> hierarchy = new ArrayList<>(snapshotStateId.getHierarchy());
        hierarchy.add(sourceFile.getFileName().toString());
        Path file = SnapshotUtils.createStatePath(rootPath, hierarchy);

        fsClient.createDirectories(file.getParent());

        try (OutputStream outputStream = fsClient.newOutputStream(file);
                HetuFileSystemClient spillFs = getSpillerFileSystemClient(sourceFile);
                InputStream inputStream = spillFs.newInputStream(sourceFile)) {
            ByteStreams.copy(inputStream, outputStream);
        }
        timer.stop();
        if (dataCollector != null) {
            long snapshotId = snapshotStateId.getSnapshotId();
            Long size = (Long) fsClient.getAttribute(file, "size");
            if (size != null) {
                dataCollector.updateSnapshotCaptureSize(snapshotId, size.longValue());
            }
            dataCollector.updateSnapshotCaptureCpuTime(snapshotId, timer.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public boolean loadFile(SnapshotStateId snapshotStateId, Path targetPath, SnapshotDataCollector dataCollector)
            throws IOException
    {
        Stopwatch timer = Stopwatch.createStarted();
        List<String> hierarchy = new ArrayList<>(snapshotStateId.getHierarchy());
        String fileName = targetPath.getFileName().toString();
        hierarchy.add(fileName);
        Path file = SnapshotUtils.createStatePath(rootPath, hierarchy);

        if (!fsClient.exists(file)) {
            LOG.warn("File: %s does not exist under %s", targetPath.getFileName().toString(), snapshotStateId);
            return false;
        }

        targetPath.getParent().toFile().mkdirs();

        try (InputStream inputStream = fsClient.newInputStream(file);
                HetuFileSystemClient spillFs = getSpillerFileSystemClient(targetPath);
                OutputStream outputStream = spillFs.newOutputStream(targetPath)) {
            ByteStreams.copy(inputStream, outputStream);
        }
        timer.stop();
        if (dataCollector != null) {
            Long size = (Long) fsClient.getAttribute(file, "size");
            dataCollector.updateSnapshotRestoreSize(size.longValue());
            dataCollector.updateSnapshotRestoreCpuTime(timer.elapsed(TimeUnit.MILLISECONDS));
        }
        return true;
    }

    @Override
    public void deleteAll(String queryId)
            throws IOException
    {
        Path file = SnapshotUtils.createStatePath(rootPath, queryId);
        fsClient.deleteRecursively(file);
    }

    @Override
    public void storeSnapshotResult(String queryId, Map<Long, SnapshotInfo> result)
            throws IOException
    {
        Path file = SnapshotUtils.createStatePath(rootPath, queryId, "result");

        fsClient.createDirectories(file.getParent());

        try (ObjectOutputStream oos = new ObjectOutputStream(fsClient.newOutputStream(file))) {
            oos.writeObject(result);
        }
    }

    @Override
    public Map<Long, SnapshotInfo> loadSnapshotResult(String queryId)
            throws IOException, ClassNotFoundException
    {
        Path file = SnapshotUtils.createStatePath(rootPath, queryId, "result");

        if (!fsClient.exists(file)) {
            return new LinkedHashMap<>();
        }

        try (ObjectInputStream ois = new ObjectInputStream(fsClient.newInputStream(file))) {
            return (LinkedHashMap<Long, SnapshotInfo>) ois.readObject();
        }
    }

    @Override
    public Set<String> loadConsolidatedFiles(String queryId)
            throws IOException, ClassNotFoundException
    {
        Path file = SnapshotUtils.createStatePath(rootPath, queryId, "ConsolidatedFileList");

        if (!fsClient.exists(file)) {
            return null;
        }

        try (ObjectInputStream ois = new ObjectInputStream(fsClient.newInputStream(file))) {
            return (Set<String>) ois.readObject();
        }
    }

    @Override
    public void storeConsolidatedFileList(String queryId, Set<String> path)
            throws IOException
    {
        Path file = SnapshotUtils.createStatePath(rootPath, queryId, "ConsolidatedFileList");

        fsClient.createDirectories(file.getParent());

        try (ObjectOutputStream oos = new ObjectOutputStream(fsClient.newOutputStream(file))) {
            oos.writeObject(path);
        }
    }

    private HetuFileSystemClient getSpillerFileSystemClient(Path filePath) throws IOException
    {
        return !spillToHdfs && spillProfile == null ? fileSystemClientManager.getFileSystemClient(filePath) : fileSystemClientManager.getFileSystemClient(spillProfile, filePath);
    }
}
