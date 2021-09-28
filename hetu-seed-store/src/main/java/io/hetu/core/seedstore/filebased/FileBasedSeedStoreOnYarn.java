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

package io.hetu.core.seedstore.filebased;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.hetu.core.common.util.SecurePathWhiteList;
import io.prestosql.spi.filesystem.FileBasedLock;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.seedstore.Seed;
import io.prestosql.spi.seedstore.SeedStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.nio.file.StandardOpenOption.CREATE_NEW;

/**
 * FileBasedOnYarnSeedStore
 *
 * @since 2021-06-23
 */
public class FileBasedSeedStoreOnYarn
        implements SeedStore
{
    private static final Logger LOG = Logger.get(FileBasedSeedStoreOnYarn.class);
    private static final JsonCodec<List<FileBasedSeed>> LIST_FILE_BASED_SEED_CODEC = listJsonCodec(FileBasedSeed.class);
    private HetuFileSystemClient fs;
    private Map<String, String> config;
    private String name;          // seed store name represents the sub-folder where a seed file is located
    private Path seedFileFolder;  // seed file folder
    private Path seedFilePath;    // seed file full path, = <seedFileFolder>/<name>/seeds.json

    /**
     * Constructor for file based seed store - on YARN
     *
     * @param name seed store name
     * @param config seed store config
     */
    public FileBasedSeedStoreOnYarn(String name, HetuFileSystemClient fs, Map<String, String> config)
    {
        this.name = name;
        this.fs = fs;
        this.config = config;

        try {
            seedFileFolder = Paths.get(config.getOrDefault(FileBasedSeedConstants.SEED_STORE_FILESYSTEM_DIR, FileBasedSeedConstants.SEED_STORE_FILESYSTEM_DIR_DEFAULT_VALUE).trim());

            checkArgument(!seedFileFolder.toString().contains("../"),
                    "SeedStore directory path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(seedFileFolder.toString()),
                    "SeedStore directory path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());

            seedFilePath = seedFileFolder.resolve(name).resolve(FileBasedSeedConstants.ON_YARN_SEED_FILE_NAME);
            checkArgument(!seedFilePath.toString().contains("../"),
                    "Seed file path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(seedFilePath.toString()),
                    "Seed file path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to get secure path list.", e);
        }
    }

    @Override
    public Set<Seed> add(Collection<Seed> seeds)
            throws IOException
    {
        LOG.debug("FileBasedOnYarnSeedStore::add() invoked.");
        Path lockPath = seedFileFolder.resolve(name);
        checkArgument(!lockPath.toString().contains("../"),
                "Lock path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        checkArgument(SecurePathWhiteList.isSecurePath(lockPath.toString()),
                "Lock path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());

        Lock lock = new FileBasedLock(fs, lockPath);
        try {
            lock.lock();
            Set<FileBasedSeed> latestSeeds = new HashSet<>();
            // add all new seeds
            latestSeeds.addAll(
                    seeds.stream()
                            .filter(s -> (s instanceof FileBasedSeed))
                            .map(s -> (FileBasedSeed) s)
                            .collect(Collectors.toList()));
            // load existing seeds and filter out repeated seed compared to new seeds
            if (fs.exists(seedFilePath)) {
                String json = loadFromFile(seedFilePath);
                List<FileBasedSeed> existingSeeds = LIST_FILE_BASED_SEED_CODEC.fromJson(json);
                latestSeeds.addAll(
                        existingSeeds.stream()
                                .filter(s -> !latestSeeds.contains(s))
                                .collect(Collectors.toList()));
            }
            String output = LIST_FILE_BASED_SEED_CODEC.toJson(ImmutableList.copyOf(latestSeeds));
            writeToFile(seedFilePath, output, true);
            return new HashSet<>(latestSeeds);
        }
        catch (UncheckedIOException e) {
            throw new IOException(e);
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public Set<Seed> get()
            throws IOException
    {
        LOG.debug("FileBasedOnYarnSeedStore::get() invoked.");
        try {
            Set<Seed> outputs = new HashSet<>();
            if (fs.exists(seedFilePath)) {
                String json = loadFromFile(seedFilePath);
                outputs.addAll(LIST_FILE_BASED_SEED_CODEC.fromJson(json));
            }
            return outputs;
        }
        catch (UncheckedIOException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Set<Seed> remove(Collection<Seed> seeds)
            throws IOException
    {
        LOG.debug("FileBasedOnYarnSeedStore::remove() invoked.");
        Path lockPath = seedFileFolder.resolve(name);
        checkArgument(!lockPath.toString().contains("../"),
                "Lock path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        checkArgument(SecurePathWhiteList.isSecurePath(lockPath.toString()),
                "Lock path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        Lock lock = new FileBasedLock(fs, lockPath);
        try {
            lock.lock();
            Set<Seed> outputs = new HashSet<>();
            if (fs.exists(seedFilePath)) {
                String json = loadFromFile(seedFilePath);
                List<FileBasedSeed> existingSeeds = LIST_FILE_BASED_SEED_CODEC.fromJson(json);
                Set<FileBasedSeed> latestSeeds = new HashSet<>(existingSeeds);
                latestSeeds.removeAll(
                        seeds.stream()
                                .filter(s -> (s instanceof FileBasedSeed))
                                .map(s -> (FileBasedSeed) s)
                                .collect(Collectors.toList()));
                String output = LIST_FILE_BASED_SEED_CODEC.toJson(ImmutableList.copyOf(latestSeeds));
                writeToFile(seedFilePath, output, true);
                outputs.addAll(latestSeeds);
            }
            return outputs;
        }
        catch (UncheckedIOException e) {
            throw new IOException(e);
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public Seed create(Map<String, String> properties)
    {
        String location = properties.get(Seed.LOCATION_PROPERTY_NAME);
        String timestamp = properties.get(Seed.TIMESTAMP_PROPERTY_NAME);
        // add more properties if interfaces change
        if (location == null || location.isEmpty()) {
            throw new NullPointerException("Cannot create file-based seed since location is null or empty");
        }

        return new FileBasedSeed(location, Long.parseLong(timestamp));
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void setName(String name)
    {
        checkArgument(name.matches("[\\p{Alnum}_\\-]+"), "Invalid cluster name");
        this.name = name;
        this.seedFilePath = seedFileFolder.resolve(name).resolve(FileBasedSeedConstants.SEED_FILE_NAME);
    }

    private void writeToFile(Path file, String content, boolean overwrite)
            throws IOException
    {
        try (OutputStream os = (overwrite) ? fs.newOutputStream(file) : fs.newOutputStream(file, CREATE_NEW)) {
            os.write(content.getBytes());
        }
    }

    private String loadFromFile(Path file)
            throws IOException
    {
        StringBuilder content = new StringBuilder(0);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.newInputStream(file)))) {
            br.lines().forEach(content::append);
        }
        return content.toString();
    }
}
