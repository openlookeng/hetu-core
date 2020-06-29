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
import io.airlift.units.Duration;

import javax.annotation.concurrent.Immutable;

@Immutable
public class DataCenterRequest
{
    private final String queryId;
    private final String clientId;
    private final String query;
    private final Duration maxAnticipatedDelay;
    private final DataCenterResponseType responseType;

    @JsonCreator
    public DataCenterRequest(@JsonProperty("queryId") String queryId,
            @JsonProperty("clientId") String clientId,
            @JsonProperty("query") String query,
            @JsonProperty("maxAnticipatedDelay") Duration maxAnticipatedDelay,
            @JsonProperty("responseType") DataCenterResponseType responseType)
    {
        this.queryId = queryId;
        this.clientId = clientId;
        this.query = query;
        this.maxAnticipatedDelay = maxAnticipatedDelay;
        this.responseType = responseType;
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
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public Duration getMaxAnticipatedDelay()
    {
        return maxAnticipatedDelay;
    }

    @JsonProperty
    public DataCenterResponseType getResponseType()
    {
        return responseType;
    }
}
