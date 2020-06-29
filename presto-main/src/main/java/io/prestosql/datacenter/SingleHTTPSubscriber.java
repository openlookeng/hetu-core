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
import javax.ws.rs.core.Response;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

import static io.prestosql.client.PrestoHeaders.PRESTO_ADDED_PREPARE;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static io.prestosql.client.PrestoHeaders.PRESTO_DEALLOCATED_PREPARE;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_CATALOG;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_PATH;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_ROLE;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_SCHEMA;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_SESSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
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

    private static Response toResponse(Query query, DataCenterQueryResults queryResults)
    {
        Response.ResponseBuilder response = Response.ok(queryResults);

        if (query != null) {
            query.getSetCatalog().ifPresent(catalog -> response.header(PRESTO_SET_CATALOG, catalog));
            query.getSetSchema().ifPresent(schema -> response.header(PRESTO_SET_SCHEMA, schema));
            query.getSetPath().ifPresent(path -> response.header(PRESTO_SET_PATH, path));

            // add set session properties
            query.getSetSessionProperties()
                    .forEach((key, value) -> response.header(PRESTO_SET_SESSION, key + '=' + urlEncode(value)));

            // add clear session properties
            query.getResetSessionProperties()
                    .forEach(name -> response.header(PRESTO_CLEAR_SESSION, name));

            // add set roles
            query.getSetRoles()
                    .forEach((key, value) -> response.header(PRESTO_SET_ROLE, key + '=' + urlEncode(value.toString())));

            // add added prepare statements
            for (Map.Entry<String, String> entry : query.getAddedPreparedStatements().entrySet()) {
                String encodedKey = urlEncode(entry.getKey());
                String encodedValue = urlEncode(entry.getValue());
                response.header(PRESTO_ADDED_PREPARE, encodedKey + '=' + encodedValue);
            }

            // add deallocated prepare statements
            for (String name : query.getDeallocatedPreparedStatements()) {
                response.header(PRESTO_DEALLOCATED_PREPARE, urlEncode(name));
            }

            // add new transaction ID
            query.getStartedTransactionId()
                    .ifPresent(transactionId -> response.header(PRESTO_STARTED_TRANSACTION_ID, transactionId));

            // add clear transaction ID directive
            if (query.isClearTransactionId()) {
                response.header(PRESTO_CLEAR_TRANSACTION_ID, true);
            }
        }

        return response.build();
    }

    private static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }
}
