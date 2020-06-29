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
package io.prestosql.heuristicindex;

import io.airlift.log.Logger;
import io.hetu.core.spi.heuristicindex.SplitIndexMetadata;
import io.prestosql.metadata.Split;

import javax.inject.Inject;

import java.util.List;

public class IndexManager
{
    public static final String BLOOM = "bloom";
    private static final Logger LOG = Logger.get(IndexManager.class);
    IndexCache indexCache;

    @Inject
    private IndexManager(IndexCache indexCache)
    {
        this.indexCache = indexCache;
    }

    public List<SplitIndexMetadata> getIndices(String table, String column, Split split)
    {
        return indexCache.getIndices(table, column, split);
    }
}
