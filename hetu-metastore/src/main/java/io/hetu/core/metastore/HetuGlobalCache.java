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
package io.hetu.core.metastore;

import io.prestosql.spi.metastore.HetuCache;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;

import java.util.concurrent.Callable;

import static io.prestosql.spi.statestore.StateCollection.Type.MAP;

public class HetuGlobalCache<K, V>
        implements HetuCache<K, V>
{
    private StateMap<K, V> distributedCache;

    public HetuGlobalCache(StateStore stateStore, String dcName)
    {
        this.distributedCache = (StateMap) stateStore.getOrCreateStateCollection(dcName, MAP);
    }

    @Override
    public void invalidate(K key)
    {
        distributedCache.remove(key);
    }

    @Override
    public void invalidateAll()
    {
        distributedCache.clear();
    }

    @Override
    public V getIfAbsent(K key, Callable<? extends V> loader)
    {
        return distributedCache.computeIfAbsent(key, k -> {
            try {
                return loader.call();
            }
            catch (Exception e) {
                throw new RuntimeException("distributedCache get cache exception");
            }
        });
    }

    @Override
    public V getIfPresent(K key)
    {
        return distributedCache.get(key);
    }
}
