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
package io.prestosql.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Map;

@Immutable
public class CrossRegionDynamicFilterRequest
{
    private final String queryId;
    private final String clientId;
    private final Map<String, byte[]> bloomFilters;

    @JsonCreator
    public CrossRegionDynamicFilterRequest(@JsonProperty("queryId") String queryId,
            @JsonProperty("clientId") String clientId,
            @JsonProperty("bloomFilters") Map<String, byte[]> bloomFilters)
    {
        this.queryId = queryId;
        this.clientId = clientId;
        this.bloomFilters = bloomFilters;
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
    }

    @JsonProperty
    public Map<String, byte[]> getBloomFilters()
    {
        return bloomFilters;
    }
}
