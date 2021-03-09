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

package io.hetu.core.cube.startree;

import io.hetu.core.cube.startree.io.StarTreeMetaStore;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.spi.cube.CubeProvider;
import io.prestosql.spi.metastore.HetuMetastore;

import java.util.Properties;

import static io.hetu.core.cube.startree.util.Constants.CUBE_NAME;

public class StarTreeProvider
        implements CubeProvider
{
    private static final String DEFAULT_CUBE_CACHE_SIZE = "50";
    private static final String DEFAULT_CUBE_CACHE_TTL = "300000";

    @Override
    public String getName()
    {
        return CUBE_NAME;
    }

    @Override
    public CubeMetaStore getCubeMetaStore(HetuMetastore metastore, Properties properties)
    {
        long cacheTtl = Long.parseLong(properties.getProperty("cache-ttl", DEFAULT_CUBE_CACHE_TTL));
        long cacheSize = Long.parseLong(properties.getProperty("cache-size", DEFAULT_CUBE_CACHE_SIZE));
        return new StarTreeMetaStore(metastore, cacheTtl, cacheSize);
    }
}
