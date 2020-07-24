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
package io.hetu.core.statestore;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.statestore.CipherService;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.listener.MapListener;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * EncryptedStateMap is a StateMap but have all the values encrypted using configured encryption algorithms
 * Only values are encrypted, keys are not
 *
 * @param <K> type of keys
 * @param <V> type of values
 * @since 2020-03-20
 */
public class EncryptedStateMap<K, V extends Serializable>
        implements StateMap<K, V>
{
    private final StateMap encryptedValues;
    private final CipherService cipherService;

    /**
     * Create an EncryptedStateMap
     *
     * @param stateMap wrapped StateMap used to store encrypted values
     * @param cipherService CipherService to encrypt and decrypt data
     */
    public EncryptedStateMap(StateMap stateMap, CipherService cipherService)
    {
        this.encryptedValues = requireNonNull(stateMap, "stateMap is null");
        this.cipherService = requireNonNull(cipherService, "cipherService is null");
    }

    @Override
    public V get(K key)
    {
        return (V) cipherService.decrypt((String) encryptedValues.get(key));
    }

    @Override
    public Map<K, V> getAll(Set keys)
    {
        Map<K, V> values = encryptedValues.getAll(keys);
        ImmutableMap.Builder mapBuilder = ImmutableMap.builder();
        for (Map.Entry<K, V> entry : values.entrySet()) {
            mapBuilder.put(entry.getKey(), cipherService.decrypt((String) entry.getValue()));
        }
        return mapBuilder.build();
    }

    @Override
    public Map<K, V> getAll()
    {
        Map<K, V> values = encryptedValues.getAll();
        ImmutableMap.Builder mapBuilder = ImmutableMap.builder();
        for (Map.Entry<K, V> entry : values.entrySet()) {
            mapBuilder.put(entry.getKey(), cipherService.decrypt((String) entry.getValue()));
        }
        return mapBuilder.build();
    }

    @Override
    public V put(K key, V value)
    {
        return (V) encryptedValues.put(key, cipherService.encrypt(value));
    }

    @Override
    public V putIfAbsent(K key, V value)
    {
        return (V) encryptedValues.putIfAbsent(key, cipherService.encrypt(value));
    }

    @Override
    public void putAll(Map<K, V> values)
    {
        ImmutableMap.Builder mapBuilder = ImmutableMap.builder();
        for (Map.Entry<K, V> entry : values.entrySet()) {
            mapBuilder.put(entry.getKey(), cipherService.encrypt(entry.getValue()));
        }
        this.encryptedValues.putAll(mapBuilder.build());
    }

    @Override
    public V remove(Object key)
    {
        return (V) cipherService.decrypt((String) encryptedValues.remove(key));
    }

    @Override
    public void removeAll(Set keys)
    {
        encryptedValues.removeAll(keys);
    }

    @Override
    public V replace(K key, V value)
    {
        V originalValue = (V) encryptedValues.replace(key, cipherService.encrypt(value));
        if (originalValue == null) {
            return null;
        }
        return (V) cipherService.decrypt((String) originalValue);
    }

    @Override
    public boolean containsKey(K key)
    {
        return encryptedValues.containsKey(key);
    }

    @Override
    public Set<K> keySet()
    {
        return encryptedValues.keySet();
    }

    @Override
    public void addEntryListener(MapListener listener)
    {
        encryptedValues.addEntryListener(listener);
    }

    @Override
    public void removeEntryListener(MapListener listener)
    {
        encryptedValues.removeEntryListener(listener);
    }

    @Override
    public String getName()
    {
        return encryptedValues.getName();
    }

    @Override
    public Type getType()
    {
        return encryptedValues.getType();
    }

    @Override
    public void clear()
    {
        encryptedValues.clear();
    }

    @Override
    public int size()
    {
        return encryptedValues.size();
    }

    @Override
    public boolean isEmpty()
    {
        return encryptedValues.isEmpty();
    }

    @Override
    public void destroy()
    {
        encryptedValues.destroy();
    }
}
