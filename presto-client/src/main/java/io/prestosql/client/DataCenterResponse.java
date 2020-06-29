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

public class DataCenterResponse
{
    public enum State
    {
        SUBMITTED, FINISHED_ALREADY
    }

    private final State state;
    private final String slug;
    private final boolean registered;

    @JsonCreator
    public DataCenterResponse(@JsonProperty("state") State state,
            @JsonProperty("slug") String slug,
            @JsonProperty("registered") boolean registered)
    {
        this.state = state;
        this.slug = slug;
        this.registered = registered;
    }

    @JsonProperty
    public State getState()
    {
        return state;
    }

    @JsonProperty
    public String getSlug()
    {
        return slug;
    }

    @JsonProperty
    public boolean isRegistered()
    {
        return registered;
    }
}
