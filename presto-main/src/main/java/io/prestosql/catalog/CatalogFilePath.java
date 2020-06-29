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

import java.nio.file.Path;
import java.nio.file.Paths;

public final class CatalogFilePath
{
    private final Path catalogDirPath;
    private final Path globalDirPath;
    private final Path propertiesPath;
    private final Path metadataPath;
    private final Path lockPath;

    public CatalogFilePath(String baseDirectory, String catalogName)
    {
        this.catalogDirPath = Paths.get(baseDirectory, "catalog", catalogName);
        this.globalDirPath = Paths.get(baseDirectory, "global");
        this.propertiesPath = Paths.get(baseDirectory, catalogName + ".properties");
        this.metadataPath = Paths.get(baseDirectory, "catalog", catalogName, catalogName + ".metadata");
        this.lockPath = Paths.get(baseDirectory, "catalog", catalogName + ".lock");
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
