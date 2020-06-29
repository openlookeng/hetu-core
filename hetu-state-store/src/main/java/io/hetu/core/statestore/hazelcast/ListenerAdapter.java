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

import io.prestosql.spi.statestore.Member;
import io.prestosql.spi.statestore.listener.EntryAddedListener;
import io.prestosql.spi.statestore.listener.EntryEvent;
import io.prestosql.spi.statestore.listener.EntryEventType;
import io.prestosql.spi.statestore.listener.EntryRemovedListener;
import io.prestosql.spi.statestore.listener.EntryUpdatedListener;
import io.prestosql.spi.statestore.listener.MapListener;

import java.util.ArrayList;
import java.util.List;

class ListenerAdapter
{
    private ListenerAdapter()
    {
        //private constructor - Utility class
    }

    static <K, V> List<com.hazelcast.map.listener.MapListener> toHazelcastListeners(MapListener listener)
    {
        List<com.hazelcast.map.listener.MapListener> mapListeners = new ArrayList<>();
        if (listener instanceof EntryAddedListener) {
            com.hazelcast.map.listener.EntryAddedListener<K, V> entryAddedListener = hazelCastEvent -> {
                EntryAddedListener<K, V> stateStoreListener = (EntryAddedListener<K, V>) listener;
                EntryEvent<K, V> stateStoreEvent = toStateStoreEvent(hazelCastEvent, EntryEventType.ADDED.getTypeId());
                stateStoreListener.entryAdded(stateStoreEvent);
            };
            mapListeners.add(entryAddedListener);
        }
        if (listener instanceof EntryRemovedListener) {
            com.hazelcast.map.listener.EntryRemovedListener<K, V> entryRemovedListener = hazelCastEvent -> {
                EntryRemovedListener<K, V> stateStoreListener = (EntryRemovedListener<K, V>) listener;
                EntryEvent<K, V> stateStoreEvent = toStateStoreEvent(hazelCastEvent, EntryEventType.REMOVED.getTypeId());
                stateStoreListener.entryRemoved(stateStoreEvent);
            };
            mapListeners.add(entryRemovedListener);
        }
        if (listener instanceof EntryUpdatedListener) {
            com.hazelcast.map.listener.EntryUpdatedListener<K, V> entryUpdatedListener = hazelCastEvent -> {
                EntryUpdatedListener<K, V> stateStoreListener = (EntryUpdatedListener<K, V>) listener;
                EntryEvent<K, V> stateStoreEvent = toStateStoreEvent(hazelCastEvent, EntryEventType.UPDATED.getTypeId());
                stateStoreListener.entryUpdated(stateStoreEvent);
            };
            mapListeners.add(entryUpdatedListener);
        }
        return mapListeners;
    }

    static Member toStateStoreMember(com.hazelcast.cluster.Member member)
    {
        return new Member(member.getAddress().getHost(), member.getAddress().getPort());
    }

    static <K, V> EntryEvent<K, V> toStateStoreEvent(com.hazelcast.core.EntryEvent<K, V> hazelCastEvent, int eventType)
    {
        return new EntryEvent<K, V>(toStateStoreMember(hazelCastEvent.getMember()), eventType, hazelCastEvent.getKey(), hazelCastEvent.getOldValue(), hazelCastEvent.getValue());
    }
}
