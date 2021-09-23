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
package io.hetu.core.statestore.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import io.airlift.log.Logger;
import io.hetu.core.statestore.Base64CipherService;
import io.hetu.core.statestore.EncryptedStateMap;
import io.hetu.core.statestore.EncryptedStateSet;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.statestore.CipherService;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateCollection.Type;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.statestore.listener.MapListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import static io.prestosql.spi.StandardErrorCode.STATE_STORE_FAILURE;

/**
 * HazelcastStateStore Class
 *
 * @version 1.0
 * @since 2019-11-29
 */
public class HazelcastStateStore
        implements StateStore
{
    private static final Logger log = Logger.get(HazelcastStateStore.class);

    private final String name;
    private final HazelcastInstance hzInstance;
    private final CipherService.Type encryptionType;
    private final Map<String, StateCollection> collections = new ConcurrentHashMap<>(0);

    /**
     * Create HazelcastStateStore
     *
     * @param instance HazelcastInstance
     * @param name StateStore name
     */
    public HazelcastStateStore(HazelcastInstance instance, String name)
    {
        this(instance, name, CipherService.Type.NONE);
    }

    /**
     * Create HazelcastStateStore
     *
     * @param instance HazelcastInstance
     * @param name StateStore name
     * @param encryptionType StateStore encryption type
     */
    public HazelcastStateStore(HazelcastInstance instance, String name, CipherService.Type encryptionType)
    {
        this.hzInstance = instance;
        this.name = name;
        this.encryptionType = encryptionType;
    }

    @Override
    public void init()
    {
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public StateCollection getStateCollection(String collectionName)
    {
        return collections.get(collectionName);
    }

    @Override
    public void removeStateCollection(String collectionName)
    {
        collections.remove(collectionName);
    }

    @Override
    public Map<String, StateCollection> getStateCollections()
    {
        return collections;
    }

    @Override
    public StateCollection createStateCollection(String collectionName, Type type)
    {
        StateCollection collection;
        switch (type) {
            case MAP:
                collection = createStateMap(collectionName);
                break;
            case SET:
                collection = new HazelcastStateSet(hzInstance, collectionName, type);
                if (encryptionType != CipherService.Type.NONE) {
                    collection = new EncryptedStateSet<>(
                            (HazelcastStateSet) collection, createCipherService(encryptionType));
                }
                break;
            default:
                throw new PrestoException(STATE_STORE_FAILURE,
                        "State collection type: " + type.name() + " not supported");
        }
        collections.putIfAbsent(collectionName, collection);
        return collections.get(collectionName);
    }

    @Override
    public StateCollection getOrCreateStateCollection(String collectionName, Type type)
    {
        if (!collections.containsKey(collectionName)) {
            return createStateCollection(collectionName, type);
        }
        return collections.get(collectionName);
    }

    @Override
    public <K, V> StateMap<K, V> createStateMap(String name, MapListener... listeners)
    {
        StateMap<K, V> collection = new HazelcastStateMap<K, V>(hzInstance, name, listeners);
        if (encryptionType != CipherService.Type.NONE) {
            collection = new EncryptedStateMap(collection, createCipherService(encryptionType));
        }
        collections.putIfAbsent(name, collection);
        return (StateMap<K, V>) collections.get(name);
    }

    @Override
    public Lock getLock(String lockKey)
    {
        return hzInstance.getCPSubsystem().getLock(lockKey);
    }

    @Override
    public long generateId()
    {
        return hzInstance.getFlakeIdGenerator("default").newId();
    }

    @Override
    public void registerNodeFailureHandler(Consumer nodeFailureHandler)
    {
        hzInstance.getCluster().addMembershipListener(new HazelcastClusterMembershipListener(nodeFailureHandler));
    }

    @Override
    public void registerClusterFailureHandler(Consumer clusterFailureHandler)
    {
        hzInstance.getLifecycleService().addLifecycleListener(
                new HazelcastClusterLifecycleListener(clusterFailureHandler));
    }

    private static CipherService<java.io.Serializable> createCipherService(CipherService.Type encryptionType)
    {
        switch (encryptionType) {
            case BASE64:
                return new Base64CipherService<>();
            default:
                throw new UnsupportedOperationException("Encryption type not supported: " + encryptionType.name());
        }
    }

    /**
     * Shutdown the hazelcast instance
     */
    public void shutdown()
    {
        hzInstance.shutdown();
    }

    /**
     * terminate the hazelcast instance
     */
    public void terminate()
    {
        hzInstance.getLifecycleService().terminate();
    }
}
