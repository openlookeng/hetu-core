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
package io.prestosql.statestore;

import io.prestosql.spi.statestore.StateStoreBootstrapper;

/**
 * StateStoreLauncher that launches new StateStore instance
 *
 * @since 2020-03-06
 */
public interface StateStoreLauncher
{
    /**
     * Add a StateStoreBootstrapper to StateStoreLauncher
     *
     * @param bootstrapper StateStoreBootstrapper used to launch StateStore instance
     */
    void addStateStoreBootstrapper(StateStoreBootstrapper bootstrapper);

    /**
     * Use added StateStoreBootstrapper to launch StateStore instance
     *
     * @throws Exception exception when failed to launch StateStore
     */
    void launchStateStore()
            throws Exception;
}
