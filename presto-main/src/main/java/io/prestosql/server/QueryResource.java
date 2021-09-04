/*
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
package io.prestosql.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.dispatcher.DispatchQuery;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryStats;
import io.prestosql.execution.StageId;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AccessControlUtil;
import io.prestosql.server.security.SecurityRequireNonNull;
import io.prestosql.spi.ErrorType;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.security.GroupProvider;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.connector.system.KillQueryProcedure.createKillQueryException;
import static io.prestosql.connector.system.KillQueryProcedure.createPreemptQueryException;
import static java.util.Objects.requireNonNull;

/**
 * Manage queries scheduled on this node
 */
@Path("/v1/query")
public class QueryResource
{
    private static final DataSize ZERO_BYTES = new DataSize(0, DataSize.Unit.BYTE);
    private static final Duration ZERO_MILLIS = new Duration(0, TimeUnit.MILLISECONDS);

    private final HttpServerInfo httpServerInfo;
    // TODO There should be a combined interface for this
    private final DispatchManager dispatchManager;
    private final QueryManager queryManager;

    private final AccessControl accessControl;

    private final ServerConfig serverConfig;
    private final GroupProvider groupProvider;

    public enum SortOrder
    {
        ASCENDING,
        DESCENDING,
    }

    @Inject
    public QueryResource(DispatchManager dispatchManager,
                         QueryManager queryManager,
                         HttpServerInfo httpServerInfo,
                         AccessControl accessControl,
                         ServerConfig serverConfig,
                         GroupProvider groupProvider)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.httpServerInfo = requireNonNull(httpServerInfo, "httpServerInfo is null");
        this.accessControl = requireNonNull(accessControl, "httpServerInfo is null");
        this.serverConfig = requireNonNull(serverConfig, "httpServerInfo is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
    }

    @GET
    public Response getAllQueryInfo(
            @QueryParam("state") String stateFilter,
            @QueryParam("failed") String failedFilter,
            @QueryParam("sort") String sortFilter,
            @QueryParam("sortOrder") String sortOrder,
            @QueryParam("search") String searchFilter,
            @QueryParam("pageNum") Integer pageNum,
            @QueryParam("pageSize") Integer pageSize,
            @Context HttpServletRequest servletRequest)
    {
        // if the user is admin, don't filter results by user.
        Optional<String> filterUser = AccessControlUtil.getUserForFilter(accessControl, serverConfig, servletRequest, groupProvider);

        if (pageNum != null && pageNum <= 0) {
            return Response.status(Status.BAD_REQUEST).build();
        }
        if (pageSize != null && pageSize <= 0) {
            return Response.status(Status.BAD_REQUEST).build();
        }

        String[] states = (stateFilter == null || stateFilter.equals("")) ? new String[0] : stateFilter.split(",");
        String[] failed = (failedFilter == null || failedFilter.equals("")) ? new String[0] : failedFilter.split(",");

        HashMap<QueryState, Boolean> statesMap = new HashMap<>();
        for (String state : states) {
            try {
                QueryState expectedState = QueryState.valueOf(state.toUpperCase(Locale.ENGLISH));
                statesMap.put(expectedState, true);
            }
            catch (Exception ex) {
                return Response.status(Status.BAD_REQUEST).build();
            }
        }

        HashMap<ErrorType, Boolean> failedMap = new HashMap<>();
        for (String failedValue : failed) {
            try {
                ErrorType failedState = ErrorType.valueOf(failedValue.toUpperCase(Locale.ENGLISH));
                failedMap.put(failedState, true);
            }
            catch (Exception ex) {
                return Response.status(Status.BAD_REQUEST).build();
            }
        }

        List<BasicQueryInfo> allQueries = dispatchManager.getQueries();
        List<BasicQueryInfo> filterQueries = new ArrayList<>();

        for (BasicQueryInfo queryInfo : allQueries) {
            if (filterUser(queryInfo, filterUser) && filterState(queryInfo, stateFilter, statesMap, failedFilter, failedMap) &&
                    filterSearch(queryInfo, searchFilter)) {
                filterQueries.add(queryInfo);
            }
        }

        // default sort order ascending
        SortOrder sortOrderValue;
        if (sortOrder == null || sortOrder.equals("")) {
            sortOrderValue = SortOrder.ASCENDING;
        }
        else {
            try {
                sortOrderValue = SortOrder.valueOf(sortOrder.toUpperCase(Locale.ENGLISH));
            }
            catch (Exception e) {
                return Response.status(Status.BAD_REQUEST).build();
            }
        }

        if (sortFilter != null && !sortFilter.equals("")) {
            try {
                QuerySortFilter sort = QuerySortFilter.valueOf(sortFilter.toUpperCase(Locale.ENGLISH));
                if (sortOrderValue == SortOrder.DESCENDING) {
                    filterQueries.sort(Collections.reverseOrder(sort.getCompare()));
                }
                else {
                    filterQueries.sort(sort.getCompare());
                }
            }
            catch (Exception ex) {
                return Response.status(Status.BAD_REQUEST).build();
            }
        }

        if (pageNum == null || pageSize == null) {
            ImmutableList.Builder<BasicQueryInfo> builder = new ImmutableList.Builder<>();
            for (BasicQueryInfo queryInfo : filterQueries) {
                builder.add(queryInfo);
            }
            return Response.ok(builder.build()).build();
        }

        // pagination
        int total = filterQueries.size();
        if (total == 0) {
            return Response.ok(new QueryResponse(total, filterQueries)).build();
        }
        else if (total - pageSize * (pageNum - 1) <= 0) {
            return Response.status(Status.BAD_REQUEST).build();
        }

        QueryResponse res;
        int start = (pageNum - 1) * pageSize;
        int end = Math.min(pageNum * pageSize, total);
        List<BasicQueryInfo> subList = filterQueries.subList(start, end);
        res = new QueryResponse(total, subList);
        return Response.ok(res).build();
    }

    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") QueryId queryId)
    {
        try {
            requireNonNull(queryId, "queryId is null");
        }
        catch (Exception ex) {
            return Response.status(Status.BAD_REQUEST).build();
        }
        BasicQueryInfo basicQueryInfo = getBasicQueryInfo(queryId);

        if (basicQueryInfo == null) {
            return Response.status(Status.GONE).build();
        }

        if (isQueryUriLocal(basicQueryInfo)) {
            // handle local query
            try {
                QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
                return Response.ok(queryInfo).build();
            }
            catch (NoSuchElementException ignored) {
            }
            try {
                DispatchQuery query = dispatchManager.getQuery(queryId);
                if (query.isDone()) {
                    return Response.ok(toFullQueryInfo(query)).build();
                }
            }
            catch (NoSuchElementException ignored) {
            }
            return Response.status(Status.GONE).build();
        }
        else {
            // redirect remote query
            return ClientBuilder.newBuilder().build().target(basicQueryInfo.getSelf()).request().get();
        }
    }

    @DELETE
    @Path("{queryId}")
    public void cancelQuery(@PathParam("queryId") QueryId queryId)
    {
        SecurityRequireNonNull.requireNonNull(queryId, "queryId is null");
        queryManager.cancelQuery(queryId);
    }

    @PUT
    @Path("{queryId}/killed")
    public Response killQuery(@PathParam("queryId") QueryId queryId, String message)
    {
        try {
            return failQuery(queryId, createKillQueryException(message));
        }
        catch (Exception ex) {
            return Response.status(Status.BAD_REQUEST).build();
        }
    }

    @PUT
    @Path("{queryId}/preempted")
    public Response preemptQuery(@PathParam("queryId") QueryId queryId, String message)
    {
        try {
            return failQuery(queryId, createPreemptQueryException(message));
        }
        catch (Exception ex) {
            return Response.status(Status.BAD_REQUEST).build();
        }
    }

    private Response failQuery(QueryId queryId, PrestoException queryException)
    {
        requireNonNull(queryId, "queryId is null");

        try {
            QueryState state = queryManager.getQueryState(queryId);

            // check before killing to provide the proper error code (this is racy)
            if (state.isDone()) {
                return Response.status(Status.CONFLICT).build();
            }

            queryManager.failQuery(queryId, queryException);

            // verify if the query was failed (if not, we lost the race)
            if (!queryException.getErrorCode().equals(queryManager.getQueryInfo(queryId).getErrorCode())) {
                return Response.status(Status.CONFLICT).build();
            }

            return Response.status(Status.OK).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @DELETE
    @Path("stage/{stageId}")
    public void cancelStage(@PathParam("stageId") StageId stageId)
    {
        SecurityRequireNonNull.requireNonNull(stageId, "stageId is null");
        queryManager.cancelStage(stageId);
    }

    private static QueryInfo toFullQueryInfo(DispatchQuery query)
    {
        checkArgument(query.isDone(), "query is not done");
        BasicQueryInfo info = query.getBasicQueryInfo();
        BasicQueryStats stats = info.getQueryStats();

        QueryStats queryStats = new QueryStats(
                query.getCreateTime(),
                query.getExecutionStartTime().orElse(null),
                query.getLastHeartbeat(),
                query.getEndTime().orElse(null),
                stats.getElapsedTime(),
                stats.getQueuedTime(),
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                info.isScheduled(),
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                false,
                ImmutableSet.of(),
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                ImmutableList.of(),
                ImmutableList.of());

        return new QueryInfo(
                info.getQueryId(),
                info.getSession(),
                info.getState(),
                info.getMemoryPool(),
                info.isScheduled(),
                info.getSelf(),
                ImmutableList.of(),
                info.getQuery(),
                info.getPreparedQuery(),
                queryStats,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                null,
                Optional.empty(),
                query.getDispatchInfo().getFailureInfo().orElse(null),
                info.getErrorCode(),
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                true,
                info.getResourceGroupId(), false);
    }

    private BasicQueryInfo getBasicQueryInfo(QueryId queryId)
    {
        return dispatchManager.getQueries()
                .stream()
                .filter(info -> info.getQueryId().equals(queryId))
                .findAny()
                .orElse(null);
    }

    private boolean isQueryUriLocal(BasicQueryInfo basicQueryInfo)
    {
        String localhost = httpServerInfo.getHttpUri() != null ? httpServerInfo.getHttpUri().getHost() : httpServerInfo.getHttpsUri().getHost();
        return basicQueryInfo.getSelf().getHost().equals(localhost);
    }

    private boolean filterUser(BasicQueryInfo queryInfo, Optional<String> user)
    {
        String sessionUser = queryInfo.getSession().getUser();
        if (sessionUser == null) {
            return false;
        }

        if (user.isPresent()) {
            return sessionUser.equals(user.get());
        }

        return true;
    }

    private boolean filterState(BasicQueryInfo queryInfo,
                                String stateFilter,
                                HashMap<QueryState, Boolean> statesMap,
                                String failedFilter,
                                HashMap<ErrorType, Boolean> failedMap)
    {
        if (stateFilter == null) {
            return true;
        }

        QueryState state = queryInfo.getState();
        if (state == QueryState.FAILED) {
            if (failedFilter == null) {
                return statesMap.containsKey(state);
            }
            return statesMap.containsKey(state) && failedMap.containsKey(queryInfo.getErrorType());
        }

        return statesMap.containsKey(state);
    }

    private boolean filterSearch(BasicQueryInfo queryInfo, String searchFilter)
    {
        if (searchFilter == null || searchFilter.equals("")) {
            return true;
        }
        String queryId = queryInfo.getQueryId().toString().toLowerCase(Locale.ENGLISH);
        String query = queryInfo.getQuery().toLowerCase(Locale.ENGLISH);
        String user = queryInfo.getSession().getUser().toLowerCase(Locale.ENGLISH);
        String source = queryInfo.getSession().getSource().toString().toLowerCase(Locale.ENGLISH);
        String resource = queryInfo.getResourceGroupId().toString().toLowerCase(Locale.ENGLISH);
        if (queryId.contains(searchFilter) ||
                query.contains(searchFilter) ||
                user.contains(searchFilter) ||
                source.contains(searchFilter) ||
                resource.contains(searchFilter)) {
            return true;
        }
        return false;
    }
}
