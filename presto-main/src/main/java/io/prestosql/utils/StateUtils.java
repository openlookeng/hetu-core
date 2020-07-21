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
package io.prestosql.utils;

import io.airlift.log.Logger;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;

import java.util.Optional;

/**
 * StateUtils contains util functions for StateStore
 *
 * @since 2019-11-29
 */
public class StateUtils
{
    private StateUtils() {}

    /**
     * Check if multiple coordinator mode is enabled
     *
     * @return true if multiple coordinator mode is enabled, false not enabled
     */
    public static boolean isMultiCoordinatorEnabled()
    {
        if (PropertyService.containsProperty(HetuConstant.MULTI_COORDINATOR_ENABLED)) {
            return PropertyService.getBooleanProperty(HetuConstant.MULTI_COORDINATOR_ENABLED);
        }
        else {
            return false;
        }
    }

    /**
     * Remove state from state collection
     *
     * @param stateCollection StateCollection name
     * @param queryId         id of the query state
     * @param log             logger
     */
    public static void removeState(StateCollection stateCollection, Optional<QueryId> queryId, Logger log)
    {
        if (stateCollection != null) {
            switch (stateCollection.getType()) {
                case MAP:
                    if (queryId.isPresent()) {
                        ((StateMap) stateCollection).remove(queryId.get().getId());
                    }
                    break;
                default:
                    log.error("Unsupported state collection type: %s", stateCollection.getType());
            }
        }
    }
}
