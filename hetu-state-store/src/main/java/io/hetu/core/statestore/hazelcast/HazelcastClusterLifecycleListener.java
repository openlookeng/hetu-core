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

import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;

import java.util.function.Consumer;

/**
 * Lifecycle listener for Hazelcast cluster
 *
 * @since 2020-03-23
 */
public class HazelcastClusterLifecycleListener
        implements LifecycleListener
{
    private Consumer<LifecycleEvent> clusterShutdownHandler;

    /**
     * Create HazelcastClusterMembershipListener
     *
     * @param clusterShutdownHandler handler to be used when member exit hazelcast state store
     */
    public HazelcastClusterLifecycleListener(Consumer<LifecycleEvent> clusterShutdownHandler)
    {
        this.clusterShutdownHandler = clusterShutdownHandler;
    }

    @Override
    public void stateChanged(LifecycleEvent lifecycleEvent)
    {
        if (lifecycleEvent.getState().equals(LifecycleEvent.LifecycleState.SHUTDOWN)) {
            clusterShutdownHandler.accept(lifecycleEvent);
        }
    }
}
