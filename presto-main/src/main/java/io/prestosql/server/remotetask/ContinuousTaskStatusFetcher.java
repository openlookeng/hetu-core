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
package io.prestosql.server.remotetask;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskStatus;
import io.prestosql.protocol.BaseResponse;
import io.prestosql.protocol.Codec;
import io.prestosql.snapshot.QuerySnapshotManager;
import io.prestosql.snapshot.RestoreResult;
import io.prestosql.snapshot.SnapshotInfo;
import io.prestosql.spi.PrestoException;

import javax.annotation.concurrent.GuardedBy;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static io.prestosql.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static io.prestosql.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static io.prestosql.protocol.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static io.prestosql.protocol.FullSmileResponseHandler.createFullSmileResponseHandler;
import static io.prestosql.protocol.JsonCodecWrapper.unwrapJsonCodec;
import static io.prestosql.protocol.RequestHelpers.setContentTypeHeaders;
import static io.prestosql.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static io.prestosql.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class ContinuousTaskStatusFetcher
        implements SimpleHttpResponseCallback<TaskStatus>
{
    private static final Logger log = Logger.get(ContinuousTaskStatusFetcher.class);

    private final TaskId taskId;
    private final String instanceId;
    private final Consumer<Throwable> onFail;
    private final StateMachine<TaskStatus> taskStatus;
    private final Codec<TaskStatus> taskStatusCodec;

    private final Duration refreshMaxWait;
    private final Executor executor;
    private final HttpClient httpClient;
    private final RequestErrorTracker errorTracker;
    private final RemoteTaskStats stats;
    private final boolean isBinaryEncoding;

    private final AtomicLong currentRequestStartNanos = new AtomicLong();

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private ListenableFuture<BaseResponse<TaskStatus>> future;

    private String expectedConfirmationInstanceId;

    private final QuerySnapshotManager snapshotManager;

    public ContinuousTaskStatusFetcher(
            Consumer<Throwable> onFail,
            TaskStatus initialTaskStatus,
            String instanceId,
            Duration refreshMaxWait,
            Codec<TaskStatus> taskStatusCodec,
            Executor executor,
            HttpClient httpClient,
            Duration maxErrorDuration,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats,
            boolean isBinaryEncoding,
            QuerySnapshotManager snapshotManager)
    {
        requireNonNull(initialTaskStatus, "initialTaskStatus is null");

        this.taskId = initialTaskStatus.getTaskId();
        this.instanceId = requireNonNull(instanceId, "instanceId is null");
        this.onFail = requireNonNull(onFail, "onFail is null");
        this.taskStatus = new StateMachine<>("task-" + taskId, executor, initialTaskStatus);

        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        this.taskStatusCodec = requireNonNull(taskStatusCodec, "taskStatusCodec is null");

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        this.errorTracker = new RequestErrorTracker(taskId, initialTaskStatus.getSelf(), maxErrorDuration, errorScheduledExecutor, "getting task status");
        this.stats = requireNonNull(stats, "stats is null");
        this.isBinaryEncoding = isBinaryEncoding;

        this.snapshotManager = requireNonNull(snapshotManager, "snapshotManager is null");
    }

    public synchronized void start()
    {
        if (running) {
            // already running
            return;
        }
        running = true;
        scheduleNextRequest();
    }

    public synchronized void stop()
    {
        running = false;
        if (future != null) {
            future.cancel(true);
            future = null;
        }
    }

    private synchronized void scheduleNextRequest()
    {
        // stopped or done?
        TaskStatus tmpTaskStatus = getTaskStatus();
        if (!running || tmpTaskStatus.getState().isDone()) {
            return;
        }

        // outstanding request?
        if (future != null && !future.isDone()) {
            // this should never happen
            log.error("Can not reschedule update because an update is already running");
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::scheduleNextRequest, executor);
            return;
        }

        Request request = addInstanceIdHeader(setContentTypeHeaders(isBinaryEncoding, prepareGet()))
                .setUri(uriBuilderFrom(tmpTaskStatus.getSelf()).appendPath("status").build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(PRESTO_CURRENT_STATE, tmpTaskStatus.getState().toString())
                .setHeader(PRESTO_MAX_WAIT, refreshMaxWait.toString())
                .build();

        ResponseHandler responseHandler;
        if (isBinaryEncoding) {
            responseHandler = createFullSmileResponseHandler((io.prestosql.protocol.SmileCodec<TaskStatus>) taskStatusCodec);
        }
        else {
            responseHandler = createAdaptingJsonResponseHandler(unwrapJsonCodec(taskStatusCodec));
        }

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, responseHandler);
        currentRequestStartNanos.set(System.nanoTime());
        Futures.addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri(), stats), executor);
    }

    private Request.Builder addInstanceIdHeader(Request.Builder builder)
    {
        // Add task instance id to all task related requests,
        // so receiver can verify if the instance id matches
        return builder.setHeader(PRESTO_TASK_INSTANCE_ID, instanceId);
    }

    TaskStatus getTaskStatus()
    {
        return taskStatus.get();
    }

    @Override
    public void success(TaskStatus value)
    {
        try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", taskId)) {
            if (expectedConfirmationInstanceId == null) {
                expectedConfirmationInstanceId = value.getConfirmationInstanceId();
            }
            else if (!expectedConfirmationInstanceId.equals(value.getConfirmationInstanceId())) {
                onFail.accept(new PrestoException(REMOTE_TASK_MISMATCH, format("%s (%s)", REMOTE_TASK_MISMATCH_ERROR, value.getTaskId())));
            }
            updateStats(currentRequestStartNanos.get());
            try {
                updateTaskStatus(value);
                errorTracker.requestSucceeded();
            }
            finally {
                scheduleNextRequest();
            }
        }
    }

    @Override
    public void failed(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            try {
                // if task not already done, record error
                TaskStatus tmpTaskStatus = getTaskStatus();
                if (!tmpTaskStatus.getState().isDone()) {
                    errorTracker.requestFailed(cause);
                }
            }
            catch (Error e) {
                onFail.accept(e);
                throw e;
            }
            catch (RuntimeException e) {
                onFail.accept(e);
            }
            finally {
                scheduleNextRequest();
            }
        }
    }

    @Override
    public void fatal(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            onFail.accept(cause);
        }
    }

    void updateTaskStatus(TaskStatus newValue)
    {
        // change to new value if old value is not changed and new value has a newer version
        if (taskStatus.setIf(newValue, oldValue -> {
            if (oldValue.getState().isDone()) {
                // never update if the task has reached a terminal state
                return false;
            }
            // don't update to an older version (same version is ok)
            return newValue.getVersion() >= oldValue.getVersion();
        })) {
            updateSnapshots(newValue.getSnapshotCaptureResult(), newValue.getSnapshotRestoreResult());
        }
    }

    public synchronized boolean isRunning()
    {
        return running;
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateMachine.StateChangeListener<TaskStatus> stateChangeListener)
    {
        taskStatus.addStateChangeListener(stateChangeListener);
    }

    private void updateStats(long currentRequestStartNanos)
    {
        stats.statusRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }

    private void updateSnapshots(Map<Long, SnapshotInfo> captureResult, Optional<RestoreResult> restoreResult)
    {
        snapshotManager.updateQueryCapture(taskId, captureResult);
        snapshotManager.updateQueryRestore(taskId, restoreResult);
    }
}
