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

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.statestore.CipherService;
import io.prestosql.spi.statestore.StateSet;

import java.io.Serializable;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * EncryptedStateSet is a StateSet but have all the values encrypted using configured encryption algorithms
 *
 * @param <V> type of elements in the set
 * @since 2020-03-20
 */
public class EncryptedStateSet<V extends Serializable>
        implements StateSet<V>
{
    private final StateSet encryptedValues;
    private final CipherService cipherService;

    /**
     * Create an EncryptedStateSet
     *
     * @param stateSet wrapped StateSet used to store encrypted values
     * @param cipherService CipherService to encrypt and decrypt data
     */
    public EncryptedStateSet(StateSet stateSet, CipherService cipherService)
    {
        this.encryptedValues = requireNonNull(stateSet, "stateSet is null");
        this.cipherService = requireNonNull(cipherService, "cipherService is nul");
    }

    @Override
    public boolean add(V value)
    {
        return encryptedValues.add(cipherService.encrypt(value));
    }

    @Override
    public Set getAll()
    {
        ImmutableSet.Builder decryptedValues = ImmutableSet.builder();
        Set values = this.encryptedValues.getAll();
        for (Object value : values) {
            decryptedValues.add(cipherService.decrypt((String) value));
        }
        return decryptedValues.build();
    }

    @Override
    public boolean addAll(Set<V> values)
    {
        ImmutableSet.Builder valuesToAdd = ImmutableSet.builder();
        for (V value : values) {
            valuesToAdd.add(cipherService.encrypt(value));
        }
        return this.encryptedValues.addAll(valuesToAdd.build());
    }

    @Override
    public boolean remove(V value)
    {
        return encryptedValues.remove(cipherService.encrypt(value));
    }

    @Override
    public boolean removeAll(Set<V> values)
    {
        ImmutableSet.Builder valuesToRemove = ImmutableSet.builder();
        for (V value : values) {
            valuesToRemove.add(cipherService.encrypt(value));
        }
        return this.encryptedValues.removeAll(valuesToRemove.build());
    }

    @Override
    public boolean contains(V value)
    {
        return encryptedValues.contains(cipherService.encrypt(value));
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
