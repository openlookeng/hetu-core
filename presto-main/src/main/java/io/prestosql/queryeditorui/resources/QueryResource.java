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
package io.prestosql.queryeditorui.resources;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.prestosql.queryeditorui.protocol.Job;
import io.prestosql.queryeditorui.protocol.Table;
import io.prestosql.queryeditorui.protocol.queries.CreateSavedQueryBuilder;
import io.prestosql.queryeditorui.protocol.queries.SavedQuery;
import io.prestosql.queryeditorui.protocol.queries.UserSavedQuery;
import io.prestosql.queryeditorui.security.UiAuthenticator;
import io.prestosql.queryeditorui.store.history.JobHistoryStore;
import io.prestosql.queryeditorui.store.jobs.jobs.ActiveJobsStore;
import io.prestosql.queryeditorui.store.queries.QueryStore;
import org.joda.time.DateTime;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Path("/api/query")
public class QueryResource
{
    private final JobHistoryStore jobHistoryStore;
    private final ActiveJobsStore activeJobsStore;
    private final QueryStore queryStore;

    @Inject
    public QueryResource(JobHistoryStore jobHistoryStore,
            QueryStore queryStore,
            ActiveJobsStore activeJobsStore)
    {
        this.jobHistoryStore = jobHistoryStore;
        this.queryStore = queryStore;
        this.activeJobsStore = activeJobsStore;
    }

    public static final Function<Job, DateTime> JOB_ORDERING = (input) ->
    {
        if (input == null) {
            return null;
        }
        return input.getQueryFinished();
    };

    @GET
    @Path("history")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getHistory(
            @QueryParam("table") List<Table> tables,
            @Context HttpServletRequest servletRequest)
    {
        Set<Job> recentlyRun;
        String user = UiAuthenticator.getUser(servletRequest);

        if (tables.size() < 1) {
            recentlyRun = new HashSet<>(jobHistoryStore.getRecentlyRun(200));
            Set<Job> activeJobs = activeJobsStore.getJobsForUser(user);
            recentlyRun.addAll(activeJobs);
        }
        else {
            Table[] tablesArray = tables.toArray(new Table[tables.size()]);
            Table[] restTables = Arrays.copyOfRange(tablesArray, 1, tablesArray.length);

            recentlyRun = new HashSet<>(jobHistoryStore.getRecentlyRun(200, tablesArray[0], restTables));
        }

//        ImmutableList.Builder<Job> filtered = ImmutableList.builder();
//        for (Job job : recentlyRun) {
//            if (job.getTablesUsed().isEmpty() && (job.getState() == JobState.FAILED)) {
//                filtered.add(job);
//                continue;
//            }
//        }

        List<Job> sortedResult = Ordering
                .natural()
                .nullsLast()
                .onResultOf(JOB_ORDERING)
                .immutableSortedCopy(recentlyRun);
        return Response.ok(sortedResult).build();
    }

    @GET
    @Path("saved")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSaved(@Context HttpServletRequest servletRequest)
    {
        String user = UiAuthenticator.getUser(servletRequest);
        return Response.ok(queryStore.getSavedQueries(user)).build();
    }

    @POST
    @Path("saved")
    @Produces(MediaType.APPLICATION_JSON)
    public Response saveQuery(
            @FormParam("description") String description,
            @FormParam("name") String name,
            @FormParam("query") String query,
            @Context HttpServletRequest servletRequest) throws IOException
    {
        //TODO: Get the user from the session
        String user = UiAuthenticator.getUser(servletRequest);
        CreateSavedQueryBuilder createFeaturedQueryRequest = CreateSavedQueryBuilder.notFeatured()
                .description(description)
                .name(name)
                .query(query);
        if (user != null) {
            SavedQuery savedQuery = createFeaturedQueryRequest.user(user)
                    .build();

            if (queryStore.saveQuery((UserSavedQuery) savedQuery)) {
                return Response.ok(savedQuery.getUuid()).build();
            }
            else {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
        }

        return Response.status(Response.Status.UNAUTHORIZED).build();
    }
}
