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

package io.hetu.core.cube.startree;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.cube.CubeProvider;

import java.util.Collections;

/**
 *
 * hetu-startree is a cube implementation.
 *
 * This cube implementation can be supported by different Connectors.
 *
 * Hive can support cubes but that may not be applicable for other connectors.
 * How do we have once cube that uses FileSystemMetaStore and another that uses JDBC metastore?
 */
public class StarTreePlugin
        implements Plugin
{
    @Override
    public Iterable<CubeProvider> getCubeProviders()
    {
        return Collections.singleton(new StarTreeProvider());
    }
}
