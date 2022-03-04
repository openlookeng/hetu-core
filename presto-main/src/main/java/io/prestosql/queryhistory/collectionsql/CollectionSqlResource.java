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
package io.prestosql.queryhistory.collectionsql;

import com.google.inject.Inject;
import io.prestosql.queryhistory.model.FavoriteInfo;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AccessControlUtil;
import io.prestosql.server.HttpRequestSessionContext;
import io.prestosql.spi.favorite.FavoriteResult;
import io.prestosql.spi.security.GroupProvider;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/myCollection")
public class CollectionSqlResource
{
    private final AccessControl accessControl;
    private final CollectionSqlService collectionSqlService;
    private final GroupProvider groupProvider;

    @Inject
    private CollectionSqlResource(
            AccessControl accessControl,
            CollectionSqlService collectionSqlService,
            GroupProvider groupProvider)
    {
        this.accessControl = accessControl;
        this.collectionSqlService = collectionSqlService;
        this.groupProvider = groupProvider;
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("collection")
    public Response insertQueryText(
            @FormParam("queryText") String queryText,
            @FormParam("catalog") String catalog,
            @FormParam("schema") String schema,
            @Context HttpServletRequest servletRequest)
    {
        String user = AccessControlUtil.getUser(accessControl, new HttpRequestSessionContext(servletRequest, groupProvider));
        Boolean res = collectionSqlService.insert(new FavoriteInfo(user, queryText, catalog, schema));
        return Response.ok(res).build();
    }

    @DELETE
    @Path("collection/delete")
    public Response deleteQueryText(
            @FormParam("queryText") String queryText,
            @FormParam("catalog") String catalog,
            @FormParam("schema") String schema,
            @Context HttpServletRequest servletRequest)
    {
        String user = AccessControlUtil.getUser(accessControl, new HttpRequestSessionContext(servletRequest, groupProvider));
        Boolean res = collectionSqlService.delete(new FavoriteInfo(user, queryText, catalog, schema));
        return Response.ok(res).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("collection/query")
    public Response getQueryText(
            @QueryParam("pageSize") int pageSize,
            @QueryParam("pageNum") int pageNum,
            @Context HttpServletRequest servletRequest)
    {
        String user = AccessControlUtil.getUser(accessControl, new HttpRequestSessionContext(servletRequest, groupProvider));
        FavoriteResult res = collectionSqlService.query(pageNum, pageSize, user);
        return Response.ok(res).build();
    }
}
