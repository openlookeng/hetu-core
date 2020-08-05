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
package io.prestosql.datacenter;

import io.prestosql.client.DataCenterQueryResults;
import io.prestosql.server.protocol.PageSubscriber;
import io.prestosql.server.protocol.Query;

import javax.ws.rs.container.AsyncResponse;

import static io.prestosql.server.protocol.ExecutingStatementResource.toResponse;
import static java.util.Objects.requireNonNull;

public class SingleHTTPSubscriber
        implements PageSubscriber
{
    private final AsyncResponse asyncResponse;
    private boolean active = true;
    private final long token;

    public SingleHTTPSubscriber(AsyncResponse asyncResponse, long token)
    {
        this.asyncResponse = requireNonNull(asyncResponse, "asyncResponse is null");
        this.token = token;
    }

    @Override
    public String getId()
    {
        return "";
    }

    @Override
    public boolean isActive()
    {
        return true;
    }

    @Override
    public long getToken()
    {
        return this.token;
    }

    @Override
    public void send(Query query, DataCenterQueryResults results)
    {
        this.asyncResponse.resume(toResponse(query, results));
    }
}
