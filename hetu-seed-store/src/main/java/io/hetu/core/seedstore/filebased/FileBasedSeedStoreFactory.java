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

import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.seedstore.SeedStore;
import io.prestosql.spi.seedstore.SeedStoreFactory;
import io.prestosql.spi.seedstore.SeedStoreSubType;

import java.util.Map;

/**
 * Factory class that creates file-based seed store
 *
 * @since 2020-03-06
 */
public class FileBasedSeedStoreFactory
        implements SeedStoreFactory
{
    private static final String FACTORY_TYPE = "filebased";

    @Override
    public SeedStore create(String name, SeedStoreSubType subType, HetuFileSystemClient fs, Map<String, String> config)
    {
        if (subType == SeedStoreSubType.ON_YARN) {
            return new FileBasedSeedStoreOnYarn(name, fs, config);
        }
        else {
            return new FileBasedSeedStore(name, fs, config);
        }
    }

    @Override
    public String getName()
    {
        return FACTORY_TYPE;
    }
}
