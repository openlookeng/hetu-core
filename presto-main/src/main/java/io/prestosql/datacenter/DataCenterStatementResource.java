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

import io.prestosql.client.CrossRegionDynamicFilterRequest;
import io.prestosql.client.CrossRegionDynamicFilterResponse;
import io.prestosql.client.DataCenterRequest;
import io.prestosql.client.DataCenterResponse;
import io.prestosql.client.DataCenterResponseType;
import io.prestosql.dispatcher.DispatchExecutor;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.execution.QueryManager;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.server.HttpRequestSessionContext;
import io.prestosql.server.SessionContext;
import io.prestosql.server.protocol.PagePublisherQueryManager;
import io.prestosql.server.protocol.PagePublisherQueryRunner;
import io.prestosql.server.protocol.PageSubscriber;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.utils.HetuConfig;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.util.Map;

import static java.util.Objects.isNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

@Path("/")
public class DataCenterStatementResource
{
    private final PagePublisherQueryManager queryManager;
    private final int splitCount;

    @Inject
    public DataCenterStatementResource(
            QueryManager queryManager,
            HetuConfig hetuConfig,
            DispatchManager dispatchManager,
            BlockEncodingSerde blockEncodingSerde,
            ExchangeClientSupplier exchangeClientSupplier,
            DispatchExecutor dispatchExecutor,
            StateStoreProvider stateStoreProvider)
    {
        this.queryManager = new PagePublisherQueryManager(dispatchManager,
                queryManager,
                exchangeClientSupplier,
                blockEncodingSerde,
                dispatchExecutor,
                stateStoreProvider,
                hetuConfig.getDataCenterConsumerTimeout());
        int noOfSplits = hetuConfig.getDataCenterSplits();
        // If the config value is out of range, use 5 as the default count
        this.splitCount = noOfSplits > 0 && noOfSplits <= 100 ? noOfSplits : 5;
    }

    @PreDestroy
    public void stop()
    {
        this.queryManager.close();
    }

    @GET
    @Path("/v1/dc/split/{globalQueryId}")
    @Produces(APPLICATION_JSON)
    public Response getSplits()
    {
        return Response.ok(this.splitCount).build();
    }

    @POST
    @Path("/v1/dc/statement/{globalQueryId}")
    @Produces(APPLICATION_JSON)
    public synchronized Response postStatement(
            @PathParam("globalQueryId") String globalQueryId,
            DataCenterRequest request,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo)
    {
        if (isNull(request)) {
            throw badRequest(BAD_REQUEST, "SQL statement is empty");
        }

        SessionContext context = new HttpRequestSessionContext(servletRequest);
        DataCenterResponse response;
        try {
            PagePublisherQueryRunner queryRunner = this.queryManager.submit(globalQueryId, request.getQuery(), request.getClientId(), request.getMaxAnticipatedDelay(), context);
            if (queryRunner != null) {
                response = new DataCenterResponse(DataCenterResponse.State.SUBMITTED, queryRunner.getSlug(), queryRunner.getConsumer(request.getClientId()) != null);
            }
            else {
                response = new DataCenterResponse(DataCenterResponse.State.FINISHED_ALREADY, null, false);
            }
        }
        catch (IllegalStateException ex) {
            response = new DataCenterResponse(DataCenterResponse.State.FINISHED_ALREADY, null, false);
        }
        return Response.ok(response).build();
    }

    @POST
    @Path("/v1/dc/filter/{globalQueryId}")
    @Produces(APPLICATION_JSON)
    public Response postBloomFilter(
            @PathParam("globalQueryId") String globalQueryId,
            CrossRegionDynamicFilterRequest crossRegionDynamicFilter)
    {
        if (isNull(crossRegionDynamicFilter)) {
            throw badRequest(BAD_REQUEST, "dynamic filter is empty");
        }

        Map<String, byte[]> bloomFilters = crossRegionDynamicFilter.getBloomFilters();
        queryManager.saveDynamicFilter(globalQueryId, bloomFilters);

        return Response.ok(new CrossRegionDynamicFilterResponse(true)).build();
    }

    @GET
    @Path("/v1/dc/statement/{responseType}/{clientId}/{globalQueryId}/{slug}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getQueryResults(
            @PathParam("responseType") DataCenterResponseType responseType,
            @PathParam("clientId") String clientId,
            @PathParam("globalQueryId") String globalQueryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token,
            @Suspended AsyncResponse asyncResponse)
    {
        PageSubscriber subscriber = null;
        if (responseType == DataCenterResponseType.HTTP_PULL) {
            subscriber = new SingleHTTPSubscriber(asyncResponse, token);
        }
        else {
            badRequest(BAD_REQUEST, responseType + " not supported");
        }
        this.queryManager.add(globalQueryId, slug, clientId, subscriber);
    }

    @DELETE
    @Path("/v1/dc/statement/{globalQueryId}/{slug}")
    @Produces(APPLICATION_JSON)
    public Response cancelQuery(
            @PathParam("globalQueryId") String globalQueryId,
            @PathParam("slug") String slug)
    {
        try {
            this.queryManager.cancel(globalQueryId, slug);
        }
        catch (Throwable throwable) {
            badRequest(INTERNAL_SERVER_ERROR, throwable.getMessage());
        }
        return Response.noContent().build();
    }

    private static WebApplicationException badRequest(Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }
}
