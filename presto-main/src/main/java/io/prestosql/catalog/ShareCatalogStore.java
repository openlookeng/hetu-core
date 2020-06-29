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

import io.prestosql.spi.filesystem.HetuFileSystemClient;

import java.util.List;
import java.util.Map;

public class ShareCatalogStore
        extends AbstractCatalogStore
{
    public ShareCatalogStore(String baseDirectory, HetuFileSystemClient fileSystemClient, int maxFileSizeInBytes)
    {
        super(baseDirectory, fileSystemClient, maxFileSizeInBytes);
    }

    @Override
    public Map<String, String> rewriteFilePathProperties(String catalogName, Map<String, String> properties, List<String> catalogFileNames, List<String> globalFileNames)
    {
        return properties;
    }
}
