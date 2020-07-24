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

package io.prestosql.statestore;

import io.prestosql.spi.statestore.Member;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.listener.EntryAddedListener;
import io.prestosql.spi.statestore.listener.EntryEvent;
import io.prestosql.spi.statestore.listener.MapListener;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MockStateMap<K, V>
        implements StateMap<K, V>
{
    Map<K, V> map;
    String name;
    Set<MapListener> addedListeners = new HashSet<>();

    public MockStateMap(String name, Map<K, V> map)
    {
        this.name = name;
        this.map = map;
    }

    @Override
    public V get(K key)
    {
        return map.get(key);
    }

    @Override
    public Map<K, V> getAll(Set<K> keys)
    {
        return null;
    }

    @Override
    public Map<K, V> getAll()
    {
        return map;
    }

    @Override
    public V put(K key, V value)
    {
        for (MapListener listener : addedListeners) {
            if (listener instanceof EntryAddedListener) {
                ((EntryAddedListener) listener).entryAdded(new EntryEvent(new Member("localhost", 8080), 1, key, value));
            }
        }
        return map.put(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value)
    {
        return map.putIfAbsent(key, value);
    }

    @Override
    public void putAll(Map<K, V> map)
    {
        map.putAll(map);
    }

    @Override
    public V remove(K key)
    {
        return map.remove(key);
    }

    @Override
    public void removeAll(Set<K> keys)
    {
    }

    @Override
    public V replace(K key, V value)
    {
        return map.replace(key, value);
    }

    @Override
    public boolean containsKey(K key)
    {
        return map.containsKey(key);
    }

    @Override
    public Set<K> keySet()
    {
        return map.keySet();
    }

    @Override
    public void addEntryListener(MapListener listener)
    {
        this.addedListeners.add(listener);
    }

    @Override
    public void removeEntryListener(MapListener listener)
    {
        this.addedListeners.remove(listener);
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public Type getType()
    {
        return Type.MAP;
    }

    @Override
    public void clear()
    {
        map.clear();
    }

    @Override
    public int size()
    {
        return map.size();
    }

    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    @Override
    public void destroy()
    {
    }
}
