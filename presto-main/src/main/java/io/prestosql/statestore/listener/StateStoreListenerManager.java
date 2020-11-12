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
package io.prestosql.statestore.listener;

import io.airlift.log.Logger;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.listener.MapListener;
import io.prestosql.statestore.StateStoreProvider;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * StateStore Listener Manager to add/remove state store listeners for different collections
 */
public class StateStoreListenerManager
{
    private static final Logger LOG = Logger.get(StateStoreListenerManager.class);
    private final StateStoreProvider stateStoreProvider;
    private final Map<String, MapListener> listeners = new ConcurrentHashMap<>();

    @Inject
    public StateStoreListenerManager(StateStoreProvider stateStoreProvider)
    {
        this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStoreProvider is null");
    }

    public synchronized void addStateStoreListener(MapListener listener, String map)
    {
        if (stateStoreProvider.getStateStore() == null || listeners.containsKey(map)) {
            return;
        }

        StateMap<?, ?> stateMap = (StateMap<?, ?>) stateStoreProvider.getStateStore().getOrCreateStateCollection(map, StateCollection.Type.MAP);
        stateMap.addEntryListener(listener);
        listeners.putIfAbsent(map, listener);
        LOG.info("Added state store listener " + listener + " for map " + map);
    }

    @PreDestroy
    public void removeStateStoreListener()
    {
        for (Map.Entry<String, MapListener> entry : listeners.entrySet()) {
            String map = entry.getKey();
            MapListener listener = entry.getValue();
            StateMap<?, ?> stateMap = (StateMap<?, ?>) stateStoreProvider.getStateStore().getOrCreateStateCollection(map, StateCollection.Type.MAP);
            stateMap.removeEntryListener(listener);
            LOG.info("Remove state store listener " + listener + " for map " + map);
        }
    }
}
