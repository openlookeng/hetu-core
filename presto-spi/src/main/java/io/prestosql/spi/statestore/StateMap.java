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
package io.prestosql.spi.statestore;

import java.util.Map;
import java.util.Set;

/**
 * StateMap is a map that stores different states
 *
 * @param <K> type of the map key
 * @param <V> type of the map value
 * @since 2019-11-29
 */
public interface StateMap<K, V>
        extends StateCollection
{
    /**
     * Get state related to the key
     *
     * @param key key of the state
     * @return state related to the key or null
     */
    V get(K key);

    /**
     * Get states related to all the keys
     *
     * @param keys set of keys of the states
     * @return states related to the keys
     */
    Map<K, V> getAll(Set<K> keys);

    /**
     * Get all states in the state store
     *
     * @return all states in the state store
     */
    Map<K, V> getAll();

    /**
     * Put a state in state store with a given key
     *
     * @param key key of the state
     * @param value state value
     * @return previous value associated with key or null
     */
    V put(K key, V value);

    /**
     * Put a state in state store with a given key if the key is not associated with any state
     *
     * @param key key of the state
     * @param value state value
     * @return previous value associated with key
     */
    V putIfAbsent(K key, V value);

    /**
     * Put all key state pairs in state store
     *
     * @param map key value pairs of the states
     */
    void putAll(Map<K, V> map);

    /**
     * Remove state related to the key
     *
     * @param key key of the state
     * @return state related to the key or null if the state doesn't exist
     */
    V remove(K key);

    /**
     * Remove states related to all the keys
     *
     * @param keys set of keys of the states
     */
    void removeAll(Set<K> keys);

    /**
     * Replace state in state store for a given key
     *
     * @param key key of the state
     * @param value state value
     * @return previous value associated with key or null if the state doesn't exist
     */
    V replace(K key, V value);

    /**
     * Returns true if state map contains a mapping for specified key
     *
     * @param key key of the state
     * @return true if state map contains a mapping for the specified key
     */
    boolean containsKey(K key);

    /**
     * Returns a set of keys in the state map
     *
     * @return set contains all the keys
     */
    Set<K> keySet();
}
