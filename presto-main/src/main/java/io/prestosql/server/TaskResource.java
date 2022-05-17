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
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.stats.TimeStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.Session;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskManager;
import io.prestosql.execution.TaskState;
import io.prestosql.execution.TaskStatus;
import io.prestosql.execution.buffer.BufferResult;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.operator.TaskStats;
import io.prestosql.server.security.SecurityRequireNonNull;
import io.prestosql.spi.Page;
import org.joda.time.DateTime;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.CompletionCallback;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.prestosql.PrestoMediaTypes.PRESTO_PAGES;
import static io.prestosql.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static io.prestosql.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static io.prestosql.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static io.prestosql.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static io.prestosql.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static io.prestosql.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static io.prestosql.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static io.prestosql.execution.TaskStatus.initialTaskStatus;
import static io.prestosql.protocol.SmileHeader.APPLICATION_JACKSON_SMILE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Manages tasks on this worker node
 */
@Path("/v1/task")
public class TaskResource
{
    private static final Duration ADDITIONAL_WAIT_TIME = new Duration(5, SECONDS);
    private static final Duration DEFAULT_MAX_WAIT_TIME = new Duration(2, SECONDS);

    private final TaskManager taskManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final TimeStat readFromOutputBufferTime = new TimeStat();
    private final TimeStat resultsRequestTime = new TimeStat();

    @Inject
    public TaskResource(
            TaskManager taskManager,
            SessionPropertyManager sessionPropertyManager,
            @ForAsyncHttp BoundedExecutor responseExecutor,
            @ForAsyncHttp ScheduledExecutorService timeoutExecutor)
    {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public List<TaskInfo> getAllTaskInfo(@Context UriInfo uriInfo)
    {
        List<TaskInfo> allTaskInfo = taskManager.getAllTaskInfo();
        if (shouldSummarize(uriInfo)) {
            allTaskInfo = ImmutableList.copyOf(transform(allTaskInfo, TaskInfo::summarize));
        }
        return allTaskInfo;
    }

    @POST
    @Path("{taskId}")
    @Produces({MediaType.APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    @Consumes({MediaType.APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public Response createOrUpdateTask(
            @PathParam("taskId") TaskId taskId,
            TaskUpdateRequest taskUpdateRequest,
            @Context UriInfo uriInfo)
    {
        try {
            requireNonNull(taskUpdateRequest, "taskUpdateRequest is null");
        }
        catch (Exception ex) {
            return Response.status(Status.BAD_REQUEST).build();
        }
        Session session = taskUpdateRequest.getSession().toSession(sessionPropertyManager, taskUpdateRequest.getExtraCredentials());
        TaskInfo taskInfo = taskManager.updateTask(session,
                taskId,
                taskUpdateRequest.getFragment(),
                taskUpdateRequest.getSources(),
                taskUpdateRequest.getOutputIds(),
                taskUpdateRequest.getTotalPartitions(),
                taskUpdateRequest.getConsumerId(),
                taskUpdateRequest.getTaskInstanceId());
        if (taskInfo == null) {
            return Response.ok().entity(createAbortedTaskInfo(taskId, uriInfo.getAbsolutePath())).build();
        }

        if (shouldSummarize(uriInfo)) {
            taskInfo = taskInfo.summarize();
        }

        return Response.ok().entity(taskInfo).build();
    }

    @GET
    @Path("{taskId}")
    @Produces({MediaType.APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public void getTaskInfo(
            @PathParam("taskId") final TaskId taskId,
            @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
            @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
            @HeaderParam(PRESTO_TASK_INSTANCE_ID) String taskInstanceId,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        SecurityRequireNonNull.requireNonNull(taskId, "taskId is null");

        if (currentState == null || maxWait == null) {
            TaskInfo taskInfo = tryGetTaskInfo(uriInfo, taskId, taskInstanceId);
            if (shouldSummarize(uriInfo)) {
                taskInfo = taskInfo.summarize();
            }
            asyncResponse.resume(taskInfo);
            return;
        }

        ListenableFuture<TaskInfo> futureTaskInfo = taskManager.getTaskInfo(taskId, currentState, taskInstanceId);
        if (futureTaskInfo == null) {
            asyncResponse.resume(createAbortedTaskInfo(taskId, uriInfo.getAbsolutePath()));
            return;
        }
        Duration waitTime = randomizeWaitTime(maxWait);
        futureTaskInfo = addTimeout(
                futureTaskInfo,
                () -> tryGetTaskInfo(uriInfo, taskId, null),
                waitTime,
                timeoutExecutor);

        if (shouldSummarize(uriInfo)) {
            futureTaskInfo = Futures.transform(futureTaskInfo, TaskInfo::summarize, directExecutor());
        }

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, futureTaskInfo, responseExecutor)
                .withTimeout(timeout);
    }

    private TaskInfo tryGetTaskInfo(UriInfo uriInfo, TaskId taskId, String taskInstanceId)
    {
        TaskInfo taskInfo = taskManager.getTaskInfo(taskId, taskInstanceId);
        if (taskInfo == null) {
            taskInfo = createAbortedTaskInfo(taskId, uriInfo.getAbsolutePath());
        }
        return taskInfo;
    }

    @GET
    @Path("{taskId}/status")
    @Produces({MediaType.APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public void getTaskStatus(
            @PathParam("taskId") TaskId taskId,
            @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
            @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
            @HeaderParam(PRESTO_TASK_INSTANCE_ID) String taskInstanceId,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        SecurityRequireNonNull.requireNonNull(taskId, "taskId is null");

        if (currentState == null || maxWait == null) {
            asyncResponse.resume(tryGetTaskStatus(uriInfo, taskId, taskInstanceId));
            return;
        }

        ListenableFuture<TaskStatus> futureTaskStatus = taskManager.getTaskStatus(taskId, currentState, taskInstanceId);
        if (futureTaskStatus == null) {
            asyncResponse.resume(createAbortedTaskStatus(taskId, uriInfo.getAbsolutePath()));
            return;
        }
        Duration waitTime = randomizeWaitTime(maxWait);
        // TODO: With current implementation, a newly completed driver group won't trigger immediate HTTP response,
        // leading to a slight delay of approx 1 second, which is not a major issue for any query that are heavy weight enough
        // to justify group-by-group execution. In order to fix this, REST endpoint /v1/{task}/status will need change.
        futureTaskStatus = addTimeout(
                futureTaskStatus,
                () -> tryGetTaskStatus(uriInfo, taskId, taskInstanceId),
                waitTime,
                timeoutExecutor);

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, futureTaskStatus, responseExecutor)
                .withTimeout(timeout);
    }

    private TaskStatus tryGetTaskStatus(UriInfo uriInfo, TaskId taskId, String taskInstanceId)
    {
        TaskStatus taskStatus = taskManager.getTaskStatus(taskId, taskInstanceId);
        if (taskStatus == null) {
            taskStatus = createAbortedTaskStatus(taskId, uriInfo.getAbsolutePath());
        }
        return taskStatus;
    }

    @DELETE
    @Path("{taskId}")
    @Produces({MediaType.APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public TaskInfo deleteTask(
            @PathParam("taskId") TaskId taskId,
            @HeaderParam(PRESTO_TASK_INSTANCE_ID) String taskInstanceId,
            @QueryParam("targetState") String targetStateStr,
            @Context UriInfo uriInfo)
    {
        SecurityRequireNonNull.requireNonNull(taskId, "taskId is null");

        TaskState targetState = targetStateStr == null ? TaskState.ABORTED : TaskState.valueOf(targetStateStr);
        TaskInfo taskInfo = taskManager.cancelTask(taskId, targetState, taskInstanceId);

        if (taskInfo == null) {
            taskInfo = createAbortedTaskInfo(taskId, uriInfo.getAbsolutePath());
        }

        if (shouldSummarize(uriInfo)) {
            taskInfo = taskInfo.summarize();
        }
        return taskInfo;
    }

    @GET
    @Path("{taskId}/results/{bufferId}/{token}")
    @Produces(PRESTO_PAGES)
    public void getResults(
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") OutputBufferId bufferId,
            @PathParam("token") final long token,
            @HeaderParam(PRESTO_MAX_SIZE) DataSize maxSize,
            @HeaderParam(PRESTO_TASK_INSTANCE_ID) String taskInstanceId,
            @Suspended AsyncResponse asyncResponse)
    {
        SecurityRequireNonNull.requireNonNull(taskId, "taskId is null");
        SecurityRequireNonNull.requireNonNull(bufferId, "bufferId is null");

        long start = System.nanoTime();
        ListenableFuture<BufferResult> bufferResultFuture = taskManager.getTaskResults(taskId, bufferId, token, maxSize, taskInstanceId);
        if (bufferResultFuture == null) {
            // Request came from task has been cancelled.
            asyncResponse.resume(Response
                    .status(Status.NO_CONTENT)
                    .header(PRESTO_PAGE_TOKEN, token)
                    .header(PRESTO_PAGE_NEXT_TOKEN, token)
                    .header(PRESTO_BUFFER_COMPLETE, false) // keep requesting task running, until they are cancelled (to resume)
                    .build());
            return;
        }

        Duration waitTime = randomizeWaitTime(DEFAULT_MAX_WAIT_TIME);
        bufferResultFuture = addTimeout(
                bufferResultFuture,
                () -> BufferResult.emptyResults(token, false),
                waitTime,
                timeoutExecutor);

        ListenableFuture<Response> responseFuture = Futures.transform(bufferResultFuture, result -> {
            List<SerializedPage> serializedPages = result.getSerializedPages();

            GenericEntity<?> entity = null;
            Status status;
            if (serializedPages.isEmpty()) {
                status = Status.NO_CONTENT;
            }
            else {
                entity = new GenericEntity<>(serializedPages, new TypeToken<List<Page>>() {}.getType());
                status = Status.OK;
            }

            return Response.status(status)
                    .entity(entity)
                    .header(PRESTO_PAGE_TOKEN, result.getToken())
                    .header(PRESTO_PAGE_NEXT_TOKEN, result.getNextToken())
                    .header(PRESTO_BUFFER_COMPLETE, result.isBufferComplete())
                    .build();
        }, directExecutor());

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, responseFuture, responseExecutor)
                .withTimeout(timeout,
                        Response.status(Status.NO_CONTENT)
                                .header(PRESTO_PAGE_TOKEN, token)
                                .header(PRESTO_PAGE_NEXT_TOKEN, token)
                                .header(PRESTO_BUFFER_COMPLETE, false)
                                .build());

        responseFuture.addListener(() -> readFromOutputBufferTime.add(Duration.nanosSince(start)), directExecutor());
        asyncResponse.register((CompletionCallback) throwable -> resultsRequestTime.add(Duration.nanosSince(start)));
    }

    @GET
    @Path("{taskId}/results/{bufferId}/{token}/acknowledge")
    public void acknowledgeResults(
            @HeaderParam(PRESTO_TASK_INSTANCE_ID) String taskInstanceId,
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") OutputBufferId bufferId,
            @PathParam("token") final long token)
    {
        SecurityRequireNonNull.requireNonNull(taskId, "taskId is null");
        SecurityRequireNonNull.requireNonNull(bufferId, "bufferId is null");

        taskManager.acknowledgeTaskResults(taskId, bufferId, token, taskInstanceId);
    }

    @DELETE
    @Path("{taskId}/results/{bufferId}")
    @Produces(MediaType.APPLICATION_JSON)
    public void abortResults(
            @HeaderParam(PRESTO_TASK_INSTANCE_ID) String taskInstanceId,
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") OutputBufferId bufferId,
            @Context UriInfo uriInfo)
    {
        SecurityRequireNonNull.requireNonNull(taskId, "taskId is null");
        SecurityRequireNonNull.requireNonNull(bufferId, "bufferId is null");

        taskManager.abortTaskResults(taskId, bufferId, taskInstanceId);
    }

    @Managed
    @Nested
    public TimeStat getReadFromOutputBufferTime()
    {
        return readFromOutputBufferTime;
    }

    @Managed
    @Nested
    public TimeStat getResultsRequestTime()
    {
        return resultsRequestTime;
    }

    private static boolean shouldSummarize(UriInfo uriInfo)
    {
        return uriInfo.getQueryParameters().containsKey("summarize");
    }

    private static Duration randomizeWaitTime(Duration waitTime)
    {
        // Randomize in [T/2, T], so wait is not near zero and the client-supplied max wait time is respected
        long halfWaitMillis = waitTime.toMillis() / 2;
        return new Duration(halfWaitMillis + ThreadLocalRandom.current().nextLong(halfWaitMillis), MILLISECONDS);
    }

    // Snapshot: Request includes an invalid task instance id. Return "aborted" result to indicate the task doesn't exist (anymore).
    private TaskStatus createAbortedTaskStatus(TaskId taskId, URI uri)
    {
        return TaskStatus.failWith(initialTaskStatus(taskId, uri, ""), TaskState.ABORTED, ImmutableList.of());
    }

    private TaskInfo createAbortedTaskInfo(TaskId taskId, URI uri)
    {
        return TaskInfo.createInitialTask(taskId, uri, "", ImmutableList.of(), new TaskStats(DateTime.now(), null))
                .withTaskStatus(createAbortedTaskStatus(taskId, uri));
    }
}
