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

package io.hetu.core.plugin.datacenter;

import java.util.Optional;
import java.util.UUID;

/**
 * Global query id generator, each query has a unique id.
 *
 * @since 2020-02-11
 */
public class GlobalQueryIdGenerator
{
    private static final String QUERY_ID_SPLIT = "-";

    private final Optional<String> remoteClusterId;

    /**
     * Constructor of GlobalQueryIdGenerator
     *
     * @param remoteClusterId tenant that connection user belong.
     */
    public GlobalQueryIdGenerator(Optional<String> remoteClusterId)
    {
        this.remoteClusterId = remoteClusterId;
    }

    /**
     * create a query id.
     *
     * @return query id.
     */
    public String createId()
    {
        return remoteClusterId.map(s -> s + QUERY_ID_SPLIT + UUID.randomUUID().toString())
                .orElseGet(() -> UUID.randomUUID().toString());
    }
}
