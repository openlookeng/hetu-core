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

import com.google.common.collect.ImmutableList;
import io.hetu.core.statestore.hazelcast.HazelcastStateStoreBootstrapper;
import io.hetu.core.statestore.hazelcast.HazelcastStateStoreFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.statestore.StateStoreBootstrapper;
import io.prestosql.spi.statestore.StateStoreFactory;

/**
 * StateStoreManagerPlugin
 *
 * @version 1.0
 * @since 2019-11-29
 */
public class StateStoreManagerPlugin
        implements Plugin
{
    @Override
    public Iterable<StateStoreFactory> getStateStoreFactories()
    {
        return ImmutableList.of(new HazelcastStateStoreFactory());
    }

    @Override
    public Iterable<StateStoreBootstrapper> getStateStoreBootstrappers()
    {
        return ImmutableList.of(new HazelcastStateStoreBootstrapper());
    }
}
