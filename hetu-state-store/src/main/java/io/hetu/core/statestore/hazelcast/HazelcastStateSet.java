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

import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import io.prestosql.spi.statestore.StateSet;

import java.util.Set;

/**
 * HazelcastStateMap Class
 *
 * @param <V> Value for StateSet
 * @version 1.0 *
 * @since 2019-11-29
 */
public class HazelcastStateSet<V>
        implements StateSet<V>
{
    private final String name;
    private final Type type;
    private ISet<V> hzSet;

    /**
     * Constructor of HazelcastStateSet
     *
     * @param instance Hazelcast instance
     * @param name state set name
     * @param type type of this state collection
     */
    public HazelcastStateSet(HazelcastInstance instance, String name, Type type)
    {
        this.hzSet = instance.getSet(name);
        this.name = name;
        this.type = type;
    }

    @Override
    public boolean add(V value)
    {
        return hzSet.add(value);
    }

    @Override
    public Set<Object> getAll()
    {
        return (Set) hzSet;
    }

    @Override
    public boolean addAll(Set<V> set)
    {
        return hzSet.addAll(set);
    }

    @Override
    public boolean remove(V value)
    {
        return hzSet.remove(value);
    }

    @Override
    public boolean removeAll(Set<V> values)
    {
        return hzSet.removeAll(values);
    }

    @Override
    public boolean contains(V value)
    {
        return hzSet.contains(value);
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public Type getType()
    {
        return this.type;
    }

    @Override
    public void clear()
    {
        hzSet.clear();
    }

    @Override
    public int size()
    {
        return hzSet.size();
    }

    @Override
    public boolean isEmpty()
    {
        return hzSet.isEmpty();
    }

    @Override
    public void destroy()
    {
        hzSet.destroy();
    }
}
