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

import com.google.common.collect.ImmutableSet;
import io.hetu.core.common.util.SecurePathWhiteList;
import io.hetu.core.filesystem.HdfsFileSystemClientFactory;
import io.hetu.core.filesystem.LocalFileSystemClientFactory;
import io.hetu.core.heuristicindex.util.IndexConstants;
import io.hetu.core.heuristicindex.util.IndexServiceUtils;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.filesystem.HetuFileSystemClientFactory;
import io.prestosql.spi.heuristicindex.IndexFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class IndexCommandUtils
{
    // Strong coupling with filesystem client
    private static final Set<HetuFileSystemClientFactory> availableFactories = ImmutableSet.of(new LocalFileSystemClientFactory(), new HdfsFileSystemClientFactory());

    private IndexCommandUtils()
    {
    }

    public static IndexFactory getIndexFactory()
    {
        return new HeuristicIndexFactory();
    }

    /**
     * Loads the properties from the corresponding catalog properties file
     *
     * @param fullyQualifiedTableName Fully qualified table name used to search for the catalog file,
     * i.e. catalog.schema.table
     * @param configRootDir The path to the directory that contains the configuration files
     * @return Properties object with the corresponding DataSource properties loaded from the configuration files
     * @throws IOException Thrown by reading the configuration files
     */
    public static Properties loadDataSourceProperties(String fullyQualifiedTableName, String configRootDir)
            throws IOException
    {
        String[] parts = IndexServiceUtils.getTableParts(fullyQualifiedTableName);
        String catalog = parts[0];

        // load the catalog properties in catalog dir
        File catalogPropertiesFile = Paths.get(configRootDir, IndexConstants.CATALOG_CONFIGS_DIR, catalog + ".properties").toFile();

        return IndexServiceUtils.loadProperties(catalogPropertiesFile);
    }

    /**
     * Load the properties from config.properties related to indexstore, including the root dir and filesystem profile
     *
     * @param configRootDir The path to the directory that contains the configuration files
     * @return Properties object with global indexer properties loaded from the configuration files
     * @throws IOException When IOException occurs during reading config files or initializing filesystem client
     */
    public static IndexStore loadIndexStore(String configRootDir)
            throws IOException
    {
        // load all properties from config.properties
        File configPropertiesFile = Paths.get(configRootDir, IndexConstants.CONFIG_FILE).toFile();
        Properties properties = IndexServiceUtils.loadProperties(configPropertiesFile);

        Path root = Paths.get(requireNonNull(properties.getProperty(IndexConstants.INDEXSTORE_URI_KEY),
                IndexConstants.INDEXSTORE_URI_KEY + " is not set in config.properties"));
        try {
            checkArgument(!root.toString().contains("../"), "Index store directory path must be absolute");
            checkArgument(SecurePathWhiteList.isSecurePath(root.toString()),
                    "Index store directory path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to get secure path list.", e);
        }

        String fileSystemProfileName = requireNonNull(properties.getProperty(IndexConstants.INDEXSTORE_FILESYSTEM_PROFILE_KEY),
                IndexConstants.INDEXSTORE_FILESYSTEM_PROFILE_KEY + " is not set in config.properties");

        File fileSystemProfile = new File(String.format("%s/filesystem/%s.properties", configRootDir, fileSystemProfileName));
        if (!fileSystemProfile.exists()) {
            throw new FileNotFoundException(String.format("Filesystem profile '%s' not found", fileSystemProfileName));
        }

        // Strong coupling with filesystem client, change when modifying filesystem client profile
        try (InputStream is = new FileInputStream(fileSystemProfile)) {
            Properties fsConfig = new Properties();
            fsConfig.load(is);

            String fsType = fsConfig.getProperty("fs.client.type");

            for (HetuFileSystemClientFactory factory : availableFactories) {
                if (fsType.equalsIgnoreCase(factory.getName())) {
                    return new IndexStore(factory.getFileSystemClient(fsConfig, root), root);
                }
            }

            throw new IllegalArgumentException(String.format("fs.client.type '{}' has no registered factory", fsType));
        }
    }

    static class IndexStore
    {
        final HetuFileSystemClient fs;
        final Path root;

        public IndexStore(HetuFileSystemClient fs, Path root)
        {
            this.fs = fs;
            this.root = root;
        }

        public HetuFileSystemClient getFs()
        {
            return fs;
        }

        public Path getRoot()
        {
            return root;
        }
    }
}
