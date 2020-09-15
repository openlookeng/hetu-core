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

import io.hetu.core.common.util.SecurePathWhiteList;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class CatalogFilePath
{
    private final Path catalogDirPath;
    private final Path globalDirPath;
    private final Path propertiesPath;
    private final Path metadataPath;
    private final Path lockPath;

    /*
    * baseDirectory
    * ├── global
    * │   └── krb5.conf
    * ├── catalog
    * │   ├── hive.properties
    * │   ├── hive
    * │   │   ├── hive.metadata
    * │   │   ├── hdfs-site.xml
    * │   │   ├── core-site.xml
    * │   │   └── user.keytab
    * │   ├── mysql.properties
    * │   └── mysql
    * │       └── mysql.metadata
    * ├── lock
     * */
    public CatalogFilePath(String baseDirectory, String catalogName)
    {
        requireNonNull(baseDirectory, "base directory is null");
        requireNonNull(catalogName, "catalog name is null");

        try {
            checkArgument(!baseDirectory.contains("../"),
                    "Catalog directory path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(baseDirectory),
                    "Catalog file directory path must at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Catalog file path not secure", e);
        }

        // global files directory
        this.globalDirPath = Paths.get(baseDirectory, "global");
        // catalog files directory
        String catalogBasePath = getCatalogBasePath(baseDirectory).toString();

        this.catalogDirPath = Paths.get(catalogBasePath, catalogName);
        this.propertiesPath = Paths.get(catalogBasePath, catalogName + ".properties");
        this.metadataPath = Paths.get(catalogDirPath.toString(), catalogName + ".metadata");
        // lock files directory
        this.lockPath = Paths.get(catalogBasePath);
    }

    public static Path getCatalogBasePath(String baseDirectory)
    {
        return Paths.get(baseDirectory, "catalog");
    }

    public Path getCatalogDirPath()
    {
        return catalogDirPath;
    }

    public Path getGlobalDirPath()
    {
        return globalDirPath;
    }

    public Path getPropertiesPath()
    {
        return propertiesPath;
    }

    public Path getMetadataPath()
    {
        return metadataPath;
    }

    public Path getLockPath()
    {
        return lockPath;
    }
}
