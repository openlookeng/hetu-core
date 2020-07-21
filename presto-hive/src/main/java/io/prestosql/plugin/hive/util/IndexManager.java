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
package io.prestosql.plugin.hive.util;

import com.google.inject.Inject;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.List;

public class IndexManager
{
    IndexCache indexCache;

    @Inject
    public IndexManager(IndexCache indexCache)
    {
        this.indexCache = indexCache;
    }

    public List<IndexMetadata> getIndices(String catalog, String table, HiveSplit hiveSplit,
                                               TupleDomain<HiveColumnHandle> effectivePredicate,
                                               List<HiveColumnHandle> partitions)
    {
        return indexCache.getIndices(catalog, table, hiveSplit, effectivePredicate, partitions);
    }
}
