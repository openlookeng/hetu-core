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

package io.prestosql.catalog.showcatalog;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.prestosql.catalog.CatalogStore;
import io.prestosql.catalog.DynamicCatalogConfig;
import io.prestosql.catalog.LocalCatalogStore;
import io.prestosql.catalog.ShareCatalogStore;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.StaticCatalogStoreConfig;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ShowCatalogStore
{
    private final StaticCatalogStoreConfig staticCatalogConfig;
    private final DynamicCatalogConfig dynamicCatalogConfig;
    private CatalogStore localCatalogStore;
    private CatalogStore shareCatalogStore;

    @Inject
    public ShowCatalogStore(DynamicCatalogConfig dynamicCatalogConfig,
                               StaticCatalogStoreConfig staticCatalogConfig)
    {
        this.dynamicCatalogConfig = dynamicCatalogConfig;
        this.staticCatalogConfig = staticCatalogConfig;
    }

    public void loadCatalogStores(FileSystemClientManager fileSystemClientManager)
            throws IOException
    {
        int maxCatalogFileSize = (int) dynamicCatalogConfig.getCatalogMaxFileSize().toBytes();
        String localConfigurationDir = dynamicCatalogConfig.getCatalogConfigurationDir();
        Properties properties = new Properties();
        properties.put("fs.client.type", "local");
        this.localCatalogStore = new LocalCatalogStore(localConfigurationDir,
                fileSystemClientManager.getFileSystemClient(properties, Paths.get(localConfigurationDir)),
                maxCatalogFileSize);

        String shareConfigurationDir = dynamicCatalogConfig.getCatalogShareConfigurationDir();
        String ShareFileSystemProfile = dynamicCatalogConfig.getShareFileSystemProfile();
        if (!dynamicCatalogConfig.isDynamicCatalogEnabled()) {
            ShareFileSystemProfile = "local-config-default";
        }
        this.shareCatalogStore = new ShareCatalogStore(shareConfigurationDir,
                fileSystemClientManager.getFileSystemClient(ShareFileSystemProfile, Paths.get(shareConfigurationDir)),
                maxCatalogFileSize);
    }

    private CatalogStore getCatalogStore(CatalogStoreType type)
    {
        if (type == CatalogStoreType.LOCAL) {
            return localCatalogStore;
        }
        else {
            return shareCatalogStore;
        }
    }

    public synchronized Set<String> listCatalogNames(CatalogStoreType type)
            throws IOException
    {
        return ImmutableSet.copyOf(getCatalogStore(type).listCatalogNames());
    }

    public Map<String, String> getCatalogProperties(String catalogName)
            throws IOException
    {
        Set<String> localCatalogs = listCatalogNames(CatalogStoreType.LOCAL);
        Set<String> shareCatalogs = listCatalogNames(CatalogStoreType.SHARE);
        if (localCatalogs.contains(catalogName)) {
            return localCatalogStore.getCatalogProperties(catalogName, CatalogConfigType.DYNAMIC, dynamicCatalogConfig.getCatalogConfigurationDir());
        }
        else if (shareCatalogs.contains(catalogName)) {
            return shareCatalogStore.getCatalogProperties(catalogName, CatalogConfigType.DYNAMIC, dynamicCatalogConfig.getCatalogShareConfigurationDir());
        }
        else {
            return localCatalogStore.getCatalogProperties(catalogName, CatalogConfigType.STATIC, staticCatalogConfig.getCatalogConfigurationDir().toString());
        }
    }

    public enum CatalogStoreType {
        LOCAL,
        SHARE
    }

    public enum CatalogConfigType {
        DYNAMIC,
        STATIC
    }
}
