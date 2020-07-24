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
package io.hetu.core.statestore.hazelcast;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.listener.MapListener;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * HazelcastStateMap Class
 *
 * @param <K> Key for StateMap
 * @param <V> Value for StateMap
 * @version 1.0
 * @since 2019-11-29
 */
public class HazelcastStateMap<K, V>
        implements StateMap<K, V>
{
    private final String name;

    private IMap<K, V> hzMap;
    private SetMultimap<MapListener, UUID> registeredListeners = HashMultimap.create();

    /**
     * Create HazelcastStateMap
     *
     * @param instance {@code HazelcastInstance}
     * @param name Name of the {@code StateMap}
     */
    public HazelcastStateMap(HazelcastInstance instance, String name)
    {
        this.hzMap = instance.getMap(name);
        this.name = name;
    }

    public HazelcastStateMap(HazelcastInstance instance, String name, MapListener... listeners)
    {
        this.hzMap = instance.getMap(name);
        if (listeners != null) {
            Stream.of(listeners)
                    .map(ListenerAdapter::toHazelcastListeners)
                    .flatMap(Collection::stream)
                    .forEach(listener -> this.hzMap.addEntryListener(listener, true));
        }
        this.name = name;
    }

    @Override
    public V get(K key)
    {
        return hzMap.get(key);
    }

    @Override
    public Map<K, V> getAll(Set<K> keys)
    {
        return hzMap.getAll(keys);
    }

    @Override
    public Map<K, V> getAll()
    {
        Builder statesBuilder = ImmutableMap.builder();
        for (Entry<K, V> entry : hzMap.entrySet()) {
            statesBuilder.put(entry.getKey(), entry.getValue());
        }
        return statesBuilder.build();
    }

    @Override
    public V put(K key, V value)
    {
        return hzMap.put(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value)
    {
        return hzMap.putIfAbsent(key, value);
    }

    @Override
    public void putAll(Map<K, V> map)
    {
        Builder statesBuilder = ImmutableMap.builder();
        map.forEach((key, value) -> statesBuilder.put(key, value));

        Map states = statesBuilder.build();
        if (map.size() == states.size()) {
            hzMap.putAll(states);
        }
    }

    @Override
    public V remove(K key)
    {
        return hzMap.remove(key);
    }

    @Override
    public void removeAll(Set<K> keys)
    {
        keys.forEach(key -> hzMap.remove(key));
    }

    @Override
    public V replace(K key, V value)
    {
        if (!hzMap.containsKey(key)) {
            return null;
        }
        return this.put(key, value);
    }

    @Override
    public boolean containsKey(K key)
    {
        return hzMap.containsKey(key);
    }

    @Override
    public Set<K> keySet()
    {
        return hzMap.keySet();
    }

    @Override
    public void clear()
    {
        hzMap.clear();
    }

    @Override
    public int size()
    {
        return hzMap.size();
    }

    @Override
    public boolean isEmpty()
    {
        return hzMap.isEmpty();
    }

    @Override
    public void destroy()
    {
        hzMap.destroy();
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Type getType()
    {
        return Type.MAP;
    }

    @Override
    public void addEntryListener(MapListener listener)
    {
        ListenerAdapter.toHazelcastListeners(listener).forEach(hzListener -> {
            UUID listenerId = hzMap.addEntryListener(hzListener, true);
            registeredListeners.put(listener, listenerId);
        });
    }

    @Override
    public void removeEntryListener(MapListener listener)
    {
        registeredListeners.get(listener).forEach(listenerId -> hzMap.removeEntryListener(listenerId));
    }
}
