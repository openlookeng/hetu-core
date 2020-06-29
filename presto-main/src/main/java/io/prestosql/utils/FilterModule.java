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
package io.prestosql.utils;

import com.google.common.cache.CacheLoader;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.prestosql.heuristicindex.IndexCache;
import io.prestosql.heuristicindex.IndexManager;
import io.prestosql.heuristicindex.LocalIndexCache;
import io.prestosql.heuristicindex.LocalIndexCacheLoader;

public class FilterModule
        extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(CacheLoader.class).to(LocalIndexCacheLoader.class).in(Scopes.SINGLETON);
        bind(IndexCache.class).to(LocalIndexCache.class).in(Scopes.SINGLETON);
        bind(IndexManager.class).in(Scopes.SINGLETON);
    }
}
