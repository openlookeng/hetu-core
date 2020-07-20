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

import io.prestosql.spi.Page;
import okhttp3.OkHttpClient;

import java.util.List;
import java.util.Map;

public interface DataCenterStatementClient
        extends StatementClient
{
    List<Page> getPages();

    /**
     * Apply dynamic filters to the DataCenterStatementClient
     * so values can be filtered in the remote cluster
     *
     * @param dynamicFilters Column name to serialized Dynamic Filter mapping
     * @return if DynamicFilters have been applied successfully
     */
    boolean applyDynamicFilters(Map<String, byte[]> dynamicFilters);

    /**
     * Create a new HTTP DataCenterStatementClient based on the given parameters.
     *
     * @param httpClient the http client to be used
     * @param session the session
     * @param query SQL query
     * @param queryId globally unique query id
     * @return HTTP Statement Client
     */
    static DataCenterStatementClient newStatementClient(OkHttpClient httpClient, DataCenterClientSession session,
            String query, String queryId)
    {
        return new DataCenterHTTPClientV1(httpClient, session, query, queryId);
    }
}
