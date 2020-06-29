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

import com.google.inject.Inject;
import io.prestosql.filesystem.FileSystemClientManager;

import java.io.IOException;

public class CatalogStoreUtil
{
    private static final String LOCAL_FS_CLIENT_CONFIG_NAME = "local-config-catalog";
    private static final String SHARE_FS_CLIENT_CONFIG_NAME = "hdfs-config-catalog";
    private FileSystemClientManager fileSystemClientManager;
    private String localConfigurationDir;
    private String shareConfigurationDir;
    private int maxCatalogFileSize;

    @Inject
    private CatalogStoreUtil(FileSystemClientManager fileSystemClientManager, DynamicCatalogConfig dynamicCatalogConfig)
    {
        this.fileSystemClientManager = fileSystemClientManager;
        this.localConfigurationDir = dynamicCatalogConfig.getCatalogConfigurationDir();
        this.shareConfigurationDir = dynamicCatalogConfig.getCatalogShareConfigurationDir();
        this.maxCatalogFileSize = (int) dynamicCatalogConfig.getCatalogMaxFileSize().toBytes();
    }

    public CatalogStore getLocalCatalogStore()
            throws IOException
    {
        return new LocalCatalogStore(localConfigurationDir,
                fileSystemClientManager.getFileSystemClient(LOCAL_FS_CLIENT_CONFIG_NAME),
                maxCatalogFileSize);
    }

    public CatalogStore getShareCatalogStore()
            throws IOException
    {
        return new ShareCatalogStore(shareConfigurationDir,
                fileSystemClientManager.getFileSystemClient(SHARE_FS_CLIENT_CONFIG_NAME),
                maxCatalogFileSize);
    }
}
