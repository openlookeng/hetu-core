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

package io.prestosql.catalog;

import com.google.common.io.ByteStreams;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.hetu.core.common.util.SecurePathWhiteList;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.filesystem.FileBasedLock;
import io.prestosql.spi.filesystem.HetuFileSystemClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.fromProperties;
import static io.prestosql.catalog.CatalogFilePath.getCatalogBasePath;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public abstract class AbstractCatalogStore
        implements CatalogStore
{
    private static final Logger log = Logger.get(AbstractCatalogStore.class);
    private static final JsonCodec<List<String>> LIST_CODEC = JsonCodec.listJsonCodec(String.class);
    private static final String CATALOG_NAME_PROPERTY = "connector.name";

    protected final String baseDirectory;
    protected final HetuFileSystemClient fileSystemClient;
    private final int maxFileSizeInBytes;

    public AbstractCatalogStore(String baseDirectory, HetuFileSystemClient fileSystemClient, int maxFileSizeInBytes)
    {
        this.baseDirectory = requireNonNull(baseDirectory, "baseDirectory is null");
        try {
            checkArgument(!baseDirectory.contains("../"),
                    "Catalog directory path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(baseDirectory),
                    "Catalog file directory path must at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Catalog file path not secure", e);
        }

        this.maxFileSizeInBytes = requireNonNull(maxFileSizeInBytes, "maxFileSizeInBytes is null");
        this.fileSystemClient = requireNonNull(fileSystemClient, "fileSystemClient is null");
        if (!fileSystemClient.exists(getCatalogBasePath(baseDirectory))) {
            try {
                fileSystemClient.createDirectories(getCatalogBasePath(baseDirectory));
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to prepare base directory.", e);
            }
        }
    }

    /**
     * Rewrite the file path properties.
     *
     * @param catalogName catalog name.
     * @param properties catalog properties.
     * @param catalogFileNames catalog file names.
     * @param globalFileNames global file name.
     * @return properties after rewrite.
     */
    abstract Map<String, String> rewriteFilePathProperties(String catalogName,
            Map<String, String> properties,
            List<String> catalogFileNames,
            List<String> globalFileNames);

    @Override
    public void createCatalog(CatalogInfo catalogInfo, CatalogFileInputStream configFiles)
            throws IOException
    {
        String catalogName = catalogInfo.getCatalogName();
        CatalogFilePath catalogPath = new CatalogFilePath(baseDirectory, catalogName);

        // create catalog directory
        Path globalBasePath = catalogPath.getGlobalDirPath();
        if (!fileSystemClient.exists(globalBasePath)) {
            fileSystemClient.createDirectories(globalBasePath);
        }

        // create catalog directory
        Path catalogBasePath = catalogPath.getCatalogDirPath();
        if (!fileSystemClient.exists(catalogBasePath)) {
            fileSystemClient.createDirectories(catalogBasePath);
        }

        try (OutputStream propertiesOutputStream = fileSystemClient.newOutputStream(catalogPath.getPropertiesPath());
                OutputStream metadataOutputStream = fileSystemClient.newOutputStream(catalogPath.getMetadataPath())) {
            // store the related file to local disk.
            Map<String, CatalogFileInputStream.InputStreamWithType> inputStreams = configFiles.getInputStreams();
            for (Map.Entry<String, CatalogFileInputStream.InputStreamWithType> entry : inputStreams.entrySet()) {
                String fileName = entry.getKey();
                CatalogFileInputStream.InputStreamWithType inputStream = entry.getValue();

                Path filePath;
                if (inputStream.getFileType() == CatalogFileInputStream.CatalogFileType.CATALOG_FILE) {
                    filePath = Paths.get(catalogPath.getCatalogDirPath().toString(), fileName);
                }
                else {
                    filePath = Paths.get(catalogPath.getGlobalDirPath().toString(), fileName);
                }
                try (OutputStream outputStream = fileSystemClient.newOutputStream(filePath)) {
                    ByteStreams.copy(inputStream.getInputStream(), outputStream);
                }
                log.info("Store file of catalog [%s] to %s", catalogName, filePath);
            }

            // store the metadata.
            Properties metadata = new Properties();
            metadata.put("createdTime", String.valueOf(catalogInfo.getCreatedTime()));
            metadata.put("version", String.valueOf(catalogInfo.getVersion()));
            metadata.put("catalogFiles", LIST_CODEC.toJson(configFiles.getCatalogFileNames()));
            metadata.put("globalFiles", LIST_CODEC.toJson(configFiles.getGlobalFileNames()));
            metadata.store(metadataOutputStream, "The metadata of dynamic catalog");

            // last store the properties file. scanner scan the properties file, so we store this file lastly.
            Map<String, String> catalogProperties = rewriteFilePathProperties(catalogName,
                    catalogInfo.getProperties(),
                    configFiles.getCatalogFileNames(),
                    configFiles.getGlobalFileNames());
            Properties properties = new Properties();
            properties.putAll(catalogProperties);
            properties.put(CATALOG_NAME_PROPERTY, catalogInfo.getConnectorName());
            properties.store(propertiesOutputStream, "The properties of dynamic catalog");
        }
        catch (IOException ex) {
            log.error(ex, "Pull catalog files failed");
            deleteCatalog(catalogName, false);
            throw ex;
        }
    }

    @Override
    public void deleteCatalog(String catalogName, boolean totalDelete)
    {
        CatalogFilePath catalogPath = new CatalogFilePath(baseDirectory, catalogName);

        Set<Path> deleteFiles = new HashSet<>();
        deleteFiles.add(catalogPath.getMetadataPath());
        deleteFiles.add(catalogPath.getPropertiesPath());

        Properties metadata = new Properties();
        try (InputStream metadataInputStream = fileSystemClient.newInputStream(catalogPath.getMetadataPath())) {
            metadata.load(metadataInputStream);
            if (metadata.getProperty("catalogFiles") != null) {
                List<String> catalogFileNames = LIST_CODEC.fromJson(metadata.getProperty("catalogFiles"));
                for (String catalogFileName : catalogFileNames) {
                    deleteFiles.add(Paths.get(catalogPath.getCatalogDirPath().toString(), catalogFileName));
                }
            }
            if (totalDelete && metadata.getProperty("globalFiles") != null) {
                List<String> globalFileNames = LIST_CODEC.fromJson(metadata.getProperty("globalFiles"));
                for (String globalFileName : globalFileNames) {
                    deleteFiles.add(Paths.get(catalogPath.getGlobalDirPath().toString(), globalFileName));
                }
            }
        }
        catch (IOException e) {
            log.error(e, "Metadata file of catalog [%s] is missing", catalogName);
        }

        for (Path deleteFile : deleteFiles) {
            try {
                fileSystemClient.deleteIfExists(deleteFile);
                log.info("Deleted file of catalog [%s] from %s", catalogName, deleteFile);
            }
            catch (IOException e) {
                log.error(e, "Deleted file of catalog [%s] from %s", catalogName, deleteFile);
            }
        }

        try {
            fileSystemClient.deleteRecursively(catalogPath.getCatalogDirPath());
        }
        catch (IOException e) {
            log.error(e, "Delete directory of catalog [%s] failed", catalogName);
        }
    }

    @Override
    public CatalogInfo getCatalogInformation(String catalogName)
            throws IOException
    {
        CatalogFilePath catalogPath = new CatalogFilePath(baseDirectory, catalogName);

        Properties properties = new Properties();
        try (InputStream inputStream = fileSystemClient.newInputStream(catalogPath.getPropertiesPath())) {
            properties.load(inputStream);
        }

        Properties metadata = new Properties();
        try (InputStream metadataInputStream = fileSystemClient.newInputStream(catalogPath.getMetadataPath())) {
            metadata.load(metadataInputStream);
        }
        long createdTime = 0;
        if (metadata.getProperty("createdTime") != null) {
            createdTime = Long.valueOf(metadata.getProperty("createdTime"));
        }
        String version = "";
        if (metadata.getProperty("version") != null) {
            version = String.valueOf(metadata.getProperty("version"));
        }

        Map<String, String> catalogProperties = new HashMap<>(fromProperties(properties));

        String connectorName = catalogProperties.remove(CATALOG_NAME_PROPERTY);
        checkState(connectorName != null, "Catalog configuration does not contain connector.name");
        return new CatalogInfo(catalogName, connectorName, null, createdTime, version, catalogProperties);
    }

    @Override
    public CatalogFileInputStream getCatalogFiles(String catalogName)
            throws IOException
    {
        CatalogFilePath catalogPath = new CatalogFilePath(baseDirectory, catalogName);
        Properties metadata = new Properties();

        try (InputStream metadataInputStream = fileSystemClient.newInputStream(catalogPath.getMetadataPath())) {
            metadata.load(metadataInputStream);
        }

        try (CatalogFileInputStream.Builder builder = new CatalogFileInputStream.Builder(maxFileSizeInBytes)) {
            if (metadata.getProperty("catalogFiles") != null) {
                List<String> catalogFileNames = LIST_CODEC.fromJson(metadata.getProperty("catalogFiles"));
                for (String catalogFileName : catalogFileNames) {
                    Path filePath = Paths.get(catalogPath.getCatalogDirPath().toString(), catalogFileName);
                    try (InputStream inputStream = fileSystemClient.newInputStream(filePath)) {
                        builder.put(catalogFileName, CatalogFileInputStream.CatalogFileType.CATALOG_FILE, inputStream);
                    }
                }
            }

            if (metadata.getProperty("globalFiles") != null) {
                List<String> globalFileNames = LIST_CODEC.fromJson(metadata.getProperty("globalFiles"));
                for (String globalFileName : globalFileNames) {
                    Path filePath = Paths.get(catalogPath.getGlobalDirPath().toString(), globalFileName);
                    try (InputStream inputStream = fileSystemClient.newInputStream(filePath)) {
                        builder.put(globalFileName, CatalogFileInputStream.CatalogFileType.GLOBAL_FILE, inputStream);
                    }
                }
            }
            return builder.build();
        }
        catch (IOException ex) {
            // close inputStreams.
            throw ex;
        }
    }

    @Override
    public Set<String> listCatalogNames()
            throws IOException
    {
        // if the file extension is ".properties", then that is a properties file of catalog,
        // the catalog name is the file name without extension.
        try (Stream<Path> stream = fileSystemClient.list(getCatalogBasePath(baseDirectory))) {
            return stream.map(Path::getFileName)
                    .map(Path::toString)
                    .filter(fileName -> fileName.endsWith(".properties"))
                    .map(fileName -> fileName.substring(0, fileName.lastIndexOf(".properties")))
                    .collect(toCollection(HashSet::new));
        }
    }

    @Override
    public Lock getCatalogLock(String catalogName)
            throws IOException
    {
        Path path = new CatalogFilePath(baseDirectory, catalogName).getLockPath();
        return new FileBasedLock(fileSystemClient, path);
    }

    @Override
    public void releaseCatalogLock(String catalogName)
    {
        CatalogFilePath catalogPath = new CatalogFilePath(baseDirectory, catalogName);
        try {
            fileSystemClient.deleteRecursively(catalogPath.getLockPath());
        }
        catch (IOException e) {
            log.error(e, "Delete lock directory of catalog [%s] failed", catalogName);
        }
    }

    @Override
    public void close()
    {
        try {
            fileSystemClient.close();
        }
        catch (IOException ex) {
            log.error(ex, "Close hetu file system client failed");
        }
    }
}
