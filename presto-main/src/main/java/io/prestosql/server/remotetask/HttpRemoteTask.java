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

import com.google.common.base.Ticker;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.execution.FutureStateChange;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.NodeTaskMap.PartitionedSplitCountTracker;
import io.prestosql.execution.RemoteTask;
import io.prestosql.execution.ScheduledSplit;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskSource;
import io.prestosql.execution.TaskState;
import io.prestosql.execution.TaskStatus;
import io.prestosql.execution.buffer.BufferInfo;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.PageBufferInfo;
import io.prestosql.metadata.Split;
import io.prestosql.operator.TaskStats;
import io.prestosql.protocol.BaseResponse;
import io.prestosql.protocol.Codec;
import io.prestosql.protocol.SmileCodec;
import io.prestosql.server.TaskUpdateRequest;
import io.prestosql.snapshot.QuerySnapshotManager;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.sql.planner.PlanFragment;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.prestosql.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static io.prestosql.execution.TaskInfo.createInitialTask;
import static io.prestosql.execution.TaskState.ABORTED;
import static io.prestosql.execution.TaskState.CANCELED_TO_RESUME;
import static io.prestosql.execution.TaskState.FAILED;
import static io.prestosql.execution.TaskState.RESUMABLE_FAILURE;
import static io.prestosql.execution.TaskStatus.failWith;
import static io.prestosql.protocol.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static io.prestosql.protocol.FullSmileResponseHandler.createFullSmileResponseHandler;
import static io.prestosql.protocol.JsonCodecWrapper.unwrapJsonCodec;
import static io.prestosql.protocol.RequestHelpers.setContentTypeHeaders;
import static io.prestosql.server.remotetask.RequestErrorTracker.logError;
import static io.prestosql.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static io.prestosql.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static io.prestosql.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static io.prestosql.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static io.prestosql.util.Failures.WORKER_NODE_ERROR;
import static io.prestosql.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class HttpRemoteTask
        implements RemoteTask
{
    private static final Logger log = Logger.get(HttpRemoteTask.class);

    private final TaskId taskId;
    private final String instanceId;

    private final Session session;
    private final String nodeId;
    private final PlanFragment planFragment;
    private final OptionalInt totalPartitions;

    private final AtomicLong nextSplitId = new AtomicLong();

    private final RemoteTaskStats stats;
    private final TaskInfoFetcher taskInfoFetcher;
    private final ContinuousTaskStatusFetcher taskStatusFetcher;

    @GuardedBy("this")
    private Future<?> currentRequest;
    @GuardedBy("this")
    private long currentRequestStartNanos;

    @GuardedBy("this")
    //LinkedHashMultimap is used to preserve the order of insertion in addSplits.
    //It guarantees that TaskSources and their Splits will be sent in the order they're received.
    private final SetMultimap<PlanNodeId, ScheduledSplit> pendingSplits = LinkedHashMultimap.create();
    @GuardedBy("this")
    private volatile int pendingSourceSplitCount;
    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, Lifespan> pendingNoMoreSplitsForLifespan = HashMultimap.create();
    @GuardedBy("this")
    // The keys of this map represent all plan nodes that have "no more splits".
    // The boolean value of each entry represents whether the "no more splits" notification is pending delivery to workers.
    private final Map<PlanNodeId, Boolean> noMoreSplits = new HashMap<>();
    @GuardedBy("this")
    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();
    private final FutureStateChange<?> whenSplitQueueHasSpace = new FutureStateChange<>();
    @GuardedBy("this")
    private boolean splitQueueHasSpace = true;
    @GuardedBy("this")
    private OptionalInt whenSplitQueueHasSpaceThreshold = OptionalInt.empty();

    private final boolean summarizeTaskInfo;

    private final HttpClient httpClient;
    private final Executor executor;
    private final ScheduledExecutorService errorScheduledExecutor;

    private final Codec<TaskInfo> taskInfoCodec;
    private final Codec<TaskUpdateRequest> taskUpdateRequestCodec;

    private final RequestErrorTracker updateErrorTracker;

    private final AtomicBoolean needsUpdate = new AtomicBoolean(true);
    private final AtomicBoolean sendPlan = new AtomicBoolean(true);

    private final PartitionedSplitCountTracker partitionedSplitCountTracker;

    private final AtomicBoolean aborting = new AtomicBoolean(false);
    private final AtomicBoolean abandoned = new AtomicBoolean(false);
    private final AtomicBoolean cancelledToResume = new AtomicBoolean(false);
    private final boolean isBinaryEncoding;
    private Optional<PlanNodeId> parent;

    public HttpRemoteTask(Session session,
            TaskId taskId,
            String instanceId,
            String nodeId,
            URI location,
            PlanFragment planFragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OptionalInt totalPartitions,
            OutputBuffers outputBuffers,
            HttpClient httpClient,
            Executor executor,
            ScheduledExecutorService updateScheduledExecutor,
            ScheduledExecutorService errorScheduledExecutor,
            Duration maxErrorDuration,
            Duration taskStatusRefreshMaxWait,
            Duration taskInfoUpdateInterval,
            boolean summarizeTaskInfo,
            Codec<TaskStatus> taskStatusCodec,
            Codec<TaskInfo> taskInfoCodec,
            Codec<TaskUpdateRequest> taskUpdateRequestCodec,
            PartitionedSplitCountTracker partitionedSplitCountTracker,
            RemoteTaskStats stats,
            boolean isBinaryEncoding,
            Optional<PlanNodeId> parent,
            QuerySnapshotManager snapshotManager)
    {
        requireNonNull(session, "session is null");
        requireNonNull(taskId, "taskId is null");
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(location, "location is null");
        requireNonNull(planFragment, "planFragment is null");
        requireNonNull(totalPartitions, "totalPartitions is null");
        requireNonNull(outputBuffers, "outputBuffers is null");
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(taskStatusCodec, "taskStatusCodec is null");
        requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        requireNonNull(partitionedSplitCountTracker, "partitionedSplitCountTracker is null");
        requireNonNull(stats, "stats is null");
        requireNonNull(parent, "parent is null");

        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            this.taskId = taskId;
            this.instanceId = instanceId;
            this.session = session;
            this.nodeId = nodeId;
            this.planFragment = planFragment;
            this.totalPartitions = totalPartitions;
            this.outputBuffers.set(outputBuffers);
            this.httpClient = httpClient;
            this.executor = executor;
            this.errorScheduledExecutor = errorScheduledExecutor;
            this.summarizeTaskInfo = summarizeTaskInfo;
            this.taskInfoCodec = taskInfoCodec;
            this.taskUpdateRequestCodec = taskUpdateRequestCodec;
            this.updateErrorTracker = new RequestErrorTracker(taskId, location, maxErrorDuration, errorScheduledExecutor, "updating task");
            this.partitionedSplitCountTracker = requireNonNull(partitionedSplitCountTracker, "partitionedSplitCountTracker is null");
            this.stats = stats;
            this.isBinaryEncoding = isBinaryEncoding;
            this.parent = parent;

            for (Entry<PlanNodeId, Split> entry : requireNonNull(initialSplits, "initialSplits is null").entries()) {
                ScheduledSplit scheduledSplit = new ScheduledSplit(nextSplitId.getAndIncrement(), entry.getKey(), entry.getValue());
                pendingSplits.put(entry.getKey(), scheduledSplit);
            }
            pendingSourceSplitCount = planFragment.getPartitionedSources().stream()
                    .filter(initialSplits::containsKey)
                    .mapToInt(partitionedSource -> initialSplits.get(partitionedSource).size())
                    .sum();

            List<BufferInfo> bufferStates = outputBuffers.getBuffers()
                    .keySet().stream()
                    .map(outputId -> new BufferInfo(outputId, false, 0, 0, PageBufferInfo.empty()))
                    .collect(toImmutableList());

            TaskInfo initialTask = createInitialTask(taskId, location, nodeId, bufferStates, new TaskStats(DateTime.now(), null));

            this.taskStatusFetcher = new ContinuousTaskStatusFetcher(
                    this::failTask,
                    initialTask.getTaskStatus(),
                    instanceId,
                    taskStatusRefreshMaxWait,
                    taskStatusCodec,
                    executor,
                    httpClient,
                    maxErrorDuration,
                    errorScheduledExecutor,
                    stats,
                    isBinaryEncoding,
                    snapshotManager);

            this.taskInfoFetcher = new TaskInfoFetcher(
                    this::failTask,
                    initialTask,
                    instanceId,
                    httpClient,
                    taskInfoUpdateInterval,
                    taskInfoCodec,
                    maxErrorDuration,
                    summarizeTaskInfo,
                    executor,
                    updateScheduledExecutor,
                    errorScheduledExecutor,
                    stats,
                    isBinaryEncoding);

            taskStatusFetcher.addStateChangeListener(newStatus -> {
                TaskState state = newStatus.getState();
                if (state.isDone()) {
                    cleanUpTask(state);
                }
                else {
                    partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
                    updateSplitQueueSpace();
                }
            });

            partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
            updateSplitQueueSpace();
        }
    }

    @Override
    public TaskId getTaskId()
    {
        return taskId;
    }

    @Override
    public String getInstanceId()
    {
        return instanceId;
    }

    @Override
    public String getNodeId()
    {
        return nodeId;
    }

    @Override
    public TaskInfo getTaskInfo()
    {
        return taskInfoFetcher.getTaskInfo();
    }

    @Override
    public TaskStatus getTaskStatus()
    {
        return taskStatusFetcher.getTaskStatus();
    }

    @Override
    public void start()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            // to start we just need to trigger an update
            scheduleUpdate();

            taskStatusFetcher.start();
            taskInfoFetcher.start();
        }
    }

    @Override
    public synchronized void addSplits(Multimap<PlanNodeId, Split> splitsBySource)
    {
        requireNonNull(splitsBySource, "splitsBySource is null");

        // only add pending split if not done
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        boolean tmpNeedsUpdate = false;
        for (Entry<PlanNodeId, Collection<Split>> entry : splitsBySource.asMap().entrySet()) {
            PlanNodeId sourceId = entry.getKey();
            Collection<Split> splits = entry.getValue();

            checkState(!noMoreSplits.containsKey(sourceId), "noMoreSplits has already been set for %s", sourceId);
            int added = 0;
            for (Split split : splits) {
                if (pendingSplits.put(sourceId, new ScheduledSplit(nextSplitId.getAndIncrement(), sourceId, split))) {
                    added++;
                }
            }
            if (planFragment.isPartitionedSources(sourceId)) {
                pendingSourceSplitCount += added;
                partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
            }
            tmpNeedsUpdate = true;
        }
        updateSplitQueueSpace();

        if (tmpNeedsUpdate) {
            this.needsUpdate.set(true);
            scheduleUpdate();
        }
    }

    @Override
    public synchronized void noMoreSplits(PlanNodeId sourceId)
    {
        if (noMoreSplits.containsKey(sourceId)) {
            return;
        }

        noMoreSplits.put(sourceId, true);
        needsUpdate.set(true);
        scheduleUpdate();
    }

    @Override
    public synchronized void noMoreSplits(PlanNodeId sourceId, Lifespan lifespan)
    {
        if (pendingNoMoreSplitsForLifespan.put(sourceId, lifespan)) {
            needsUpdate.set(true);
            scheduleUpdate();
        }
    }

    @Override
    public synchronized void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        if (newOutputBuffers.getVersion() > outputBuffers.get().getVersion()) {
            outputBuffers.set(newOutputBuffers);
            needsUpdate.set(true);
            scheduleUpdate();
        }
    }

    @Override
    public int getPartitionedSplitCount()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return 0;
        }
        return getPendingSourceSplitCount() + taskStatus.getQueuedPartitionedDrivers() + taskStatus.getRunningPartitionedDrivers();
    }

    @Override
    public int getQueuedPartitionedSplitCount()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return 0;
        }
        return getPendingSourceSplitCount() + taskStatus.getQueuedPartitionedDrivers();
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    private int getPendingSourceSplitCount()
    {
        return pendingSourceSplitCount;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            taskStatusFetcher.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener)
    {
        taskInfoFetcher.addFinalTaskInfoListener(stateChangeListener);
    }

    @Override
    public synchronized ListenableFuture<?> whenSplitQueueHasSpace(int threshold)
    {
        if (whenSplitQueueHasSpaceThreshold.isPresent()) {
            checkArgument(threshold == whenSplitQueueHasSpaceThreshold.getAsInt(), "Multiple split queue space notification thresholds not supported");
        }
        else {
            whenSplitQueueHasSpaceThreshold = OptionalInt.of(threshold);
            updateSplitQueueSpace();
        }
        if (splitQueueHasSpace) {
            return immediateFuture(null);
        }
        return whenSplitQueueHasSpace.createNewListener();
    }

    private synchronized void updateSplitQueueSpace()
    {
        if (!whenSplitQueueHasSpaceThreshold.isPresent()) {
            return;
        }
        splitQueueHasSpace = getQueuedPartitionedSplitCount() < whenSplitQueueHasSpaceThreshold.getAsInt();
        if (splitQueueHasSpace) {
            whenSplitQueueHasSpace.complete(null, executor);
        }
    }

    private synchronized void processTaskUpdate(TaskInfo newValue, List<TaskSource> sources)
    {
        updateTaskInfo(newValue);

        // remove acknowledged splits, which frees memory
        for (TaskSource source : sources) {
            PlanNodeId planNodeId = source.getPlanNodeId();
            int removed = 0;
            for (ScheduledSplit split : source.getSplits()) {
                if (pendingSplits.remove(planNodeId, split)) {
                    removed++;
                }
            }
            if (source.isNoMoreSplits()) {
                noMoreSplits.put(planNodeId, false);
            }
            for (Lifespan lifespan : source.getNoMoreSplitsForLifespan()) {
                pendingNoMoreSplitsForLifespan.remove(planNodeId, lifespan);
            }
            if (planFragment.isPartitionedSources(planNodeId)) {
                pendingSourceSplitCount -= removed;
            }
        }
        updateSplitQueueSpace();

        partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
    }

    private void updateTaskInfo(TaskInfo taskInfo)
    {
        taskStatusFetcher.updateTaskStatus(taskInfo.getTaskStatus());
        taskInfoFetcher.updateTaskInfo(taskInfo);
    }

    private void scheduleUpdate()
    {
        executor.execute(this::sendUpdate);
    }

    private synchronized void sendUpdate()
    {
        if (abandoned.get()) {
            // Snapshot: Corresponding task has been canceled to resume. Stop any communication with it.
            return;
        }
        TaskStatus taskStatus = getTaskStatus();
        // don't update if the task hasn't been started yet or if it is already finished
        if (!needsUpdate.get() || taskStatus.getState().isDone()) {
            return;
        }

        // if there is a request already running, wait for it to complete
        if (this.currentRequest != null && !this.currentRequest.isDone()) {
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = updateErrorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::sendUpdate, executor);
            return;
        }

        List<TaskSource> sources = getSources();

        Optional<PlanFragment> fragment = sendPlan.get() ? Optional.of(planFragment) : Optional.empty();
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                // Snapshot: Add task instance id to all task related requests,
                // so receiver can verify if the instance id matches
                instanceId,
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                fragment,
                sources,
                outputBuffers.get(),
                totalPartitions,
                parent);
        byte[] taskUpdateRequestJson = taskUpdateRequestCodec.toBytes(updateRequest);
        if (fragment.isPresent()) {
            stats.updateWithPlanBytes(taskUpdateRequestJson.length);
        }

        HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus);
        Request request = setContentTypeHeaders(isBinaryEncoding, preparePost())
                .setUri(uriBuilder.build())
                .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(taskUpdateRequestJson))
                .build();
        ResponseHandler responseHandler;
        if (isBinaryEncoding) {
            responseHandler = createFullSmileResponseHandler((SmileCodec<TaskInfo>) taskInfoCodec);
        }
        else {
            responseHandler = createAdaptingJsonResponseHandler(unwrapJsonCodec(taskInfoCodec));
        }
        updateErrorTracker.startRequest();

        ListenableFuture<BaseResponse<TaskInfo>> future = httpClient.executeAsync(request, responseHandler);
        currentRequest = future;
        currentRequestStartNanos = System.nanoTime();

        // The needsUpdate flag needs to be set to false BEFORE adding the Future callback since callback might change the flag value
        // and does so without grabbing the instance lock.
        needsUpdate.set(false);

        Futures.addCallback(future, new SimpleHttpResponseHandler<>(new UpdateResponseHandler(sources), request.getUri(), stats), executor);
    }

    private synchronized List<TaskSource> getSources()
    {
        return Stream.concat(planFragment.getPartitionedSourceNodes().stream(), planFragment.getRemoteSourceNodes().stream())
                .filter(Objects::nonNull)
                .map(PlanNode::getId)
                .map(this::getSource)
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    private synchronized TaskSource getSource(PlanNodeId planNodeId)
    {
        Set<ScheduledSplit> splits = ImmutableSet.copyOf(pendingSplits.get(planNodeId));
        boolean pendingNoMoreSplits = Boolean.TRUE.equals(this.noMoreSplits.get(planNodeId));
        boolean tmpNoMoreSplits = this.noMoreSplits.containsKey(planNodeId);
        Set<Lifespan> noMoreSplitsForLifespan = pendingNoMoreSplitsForLifespan.get(planNodeId);

        TaskSource element = null;
        if (!splits.isEmpty() || !noMoreSplitsForLifespan.isEmpty() || pendingNoMoreSplits) {
            element = new TaskSource(planNodeId, splits, noMoreSplitsForLifespan, tmpNoMoreSplits);
        }
        return element;
    }

    @Override
    public synchronized void cancel()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            TaskStatus taskStatus = getTaskStatus();
            if (taskStatus.getState().isDone()) {
                return;
            }

            sendCancelRequest(taskStatus, TaskState.CANCELED, "cancel");
        }
    }

    @Override
    public synchronized void cancelToResume()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            // Cancel-to-resume doesn't need retry and backoff mechanisms
            cancelledToResume.set(true);
            TaskStatus taskStatus = getTaskStatus();
            taskStatusFetcher.stop();
            taskInfoFetcher.stop();
            aborting.set(false); // force send this request even if aborting was true
            sendCancelRequest(taskStatus, TaskState.CANCELED_TO_RESUME, "cancel-to-resume");
        }
        // This is the last time this class can be used. All subsequent requests are ignored.
        abandoned.set(true);
    }

    private void sendCancelRequest(TaskStatus taskStatus, TaskState targetState, String action)
    {
        log.debug("Cancelling task %s, with target state %s", taskStatus.getTaskId(), targetState);

        // send cancel to task and ignore response
        HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus).addParameter("targetState", targetState.toString());
        Request request = setContentTypeHeaders(isBinaryEncoding, prepareDelete())
                .setUri(uriBuilder.build())
                .addHeader(PRESTO_TASK_INSTANCE_ID, instanceId)
                .build();
        scheduleAsyncCleanupRequest(createCleanupBackoff(), request, targetState.toString());
    }

    @Override
    public synchronized void suspend()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            TaskStatus taskStatus = getTaskStatus();
            if (taskStatus.getState().isDone()) {
                return;
            }

            sendSuspendRequest(taskStatus, TaskState.SUSPENDED, "suspend");
        }
    }

    private void sendSuspendRequest(TaskStatus taskStatus, TaskState targetState, String action)
    {
        log.debug("Suspend task %s, with target state %s", taskStatus.getTaskId(), targetState);

        // send cancel to task and ignore response
        HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus).appendPath("suspend").addParameter("targetState", targetState.toString());
        Request request = setContentTypeHeaders(isBinaryEncoding, prepareDelete())
                .setUri(uriBuilder.build())
                .addHeader(PRESTO_TASK_INSTANCE_ID, instanceId)
                .build();
        scheduleAsyncCleanupRequest(createCleanupBackoff(), request, targetState.toString()); //todo(nitin) check if separate response handler needed here?
    }

    @Override
    public synchronized void resume()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            TaskStatus taskStatus = getTaskStatus();
            if (taskStatus.getState().isDone()) {
                return;
            }

            sendResumeRequest(taskStatus, TaskState.RUNNING, "suspendedToRunning");
        }
    }

    private void sendResumeRequest(TaskStatus taskStatus, TaskState targetState, String action)
    {
        log.debug("Resume task %s, with target state %s", taskStatus.getTaskId(), targetState);

        // send cancel to task and ignore response
        HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus).appendPath("resume").addParameter("targetState", targetState.toString());
        Request request = setContentTypeHeaders(isBinaryEncoding, prepareDelete())
                .setUri(uriBuilder.build())
                .addHeader(PRESTO_TASK_INSTANCE_ID, instanceId)
                .build();
        scheduleAsyncCleanupRequest(createCleanupBackoff(), request, targetState.toString()); //todo(nitin) check if separate response handler needed here?
    }

    private synchronized void cleanUpTask(TaskState newState)
    {
        checkState(getTaskStatus().getState().isDone(), "attempt to clean up a task that is not done yet");

        // clear pending splits to free memory
        pendingSplits.clear();
        pendingSourceSplitCount = 0;
        partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
        splitQueueHasSpace = true;
        whenSplitQueueHasSpace.complete(null, executor);

        // cancel pending request
        if (currentRequest != null) {
            currentRequest.cancel(true);
            currentRequest = null;
            currentRequestStartNanos = 0;
        }

        taskStatusFetcher.stop();

        // Resumable-failure and cancel-to-resume will be handled separately
        if (newState != RESUMABLE_FAILURE && newState != CANCELED_TO_RESUME) {
            // The remote task is likely to get a delete from the PageBufferClient first.
            // We send an additional delete anyway to get the final TaskInfo
            sendCancelRequest(getTaskStatus(), ABORTED, "cleanup");
        }
    }

    @Override
    public synchronized void abort()
    {
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        abort(failWith(getTaskStatus(), ABORTED, ImmutableList.of()));
    }

    private synchronized void abort(TaskStatus status)
    {
        checkState(status.getState().isDone(), "cannot abort task with an incomplete status");

        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            taskStatusFetcher.updateTaskStatus(status);

            // send abort to task
            sendCancelRequest(getTaskStatus(), ABORTED, "abort");
        }
    }

    private void scheduleAsyncCleanupRequest(Backoff cleanupBackoff, Request request, String action)
    {
        if (!aborting.compareAndSet(false, true)) {
            // Do not initiate another round of cleanup requests if one had been initiated.
            // Otherwise, we can get into an asynchronous recursion here. For example, when aborting a task after REMOTE_TASK_MISMATCH.
            return;
        }
        doScheduleAsyncCleanupRequest(cleanupBackoff, request, action);
    }

    private void doScheduleAsyncCleanupRequest(Backoff cleanupBackoff, Request request, String action)
    {
        ResponseHandler responseHandler;
        if (isBinaryEncoding) {
            responseHandler = createFullSmileResponseHandler((SmileCodec<TaskInfo>) taskInfoCodec);
        }
        else {
            responseHandler = createAdaptingJsonResponseHandler(unwrapJsonCodec(taskInfoCodec));
        }

        Futures.addCallback(httpClient.executeAsync(request, responseHandler), new FutureCallback<BaseResponse<TaskInfo>>()
        {
            @Override
            public void onSuccess(BaseResponse<TaskInfo> result)
            {
                try {
                    updateTaskInfo(result.getValue());
                }
                finally {
                    if (!getTaskInfo().getTaskStatus().getState().isDone()) {
                        cleanUpLocally();
                    }
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                if (cancelledToResume.get()) {
                    // Remote worker is probably unreachable. Don't make additional attempts.
                    cleanUpLocally();
                    return;
                }

                if (t instanceof RejectedExecutionException && httpClient.isClosed()) {
                    logError(t, "Unable to %s task at %s. HTTP client is closed.", action, request.getUri());
                    cleanUpLocally();
                    return;
                }

                // record failure
                if (cleanupBackoff.failure()) {
                    logError(t, "Unable to %s task at %s. Back off depleted.", action, request.getUri());
                    cleanUpLocally();
                    return;
                }

                // reschedule
                long delayNanos = cleanupBackoff.getBackoffDelayNanos();
                if (delayNanos == 0) {
                    doScheduleAsyncCleanupRequest(cleanupBackoff, request, action);
                }
                else {
                    errorScheduledExecutor.schedule(() -> doScheduleAsyncCleanupRequest(cleanupBackoff, request, action), delayNanos, NANOSECONDS);
                }
            }

            private void cleanUpLocally()
            {
                // Update the taskInfo with the new taskStatus.

                // Generally, we send a cleanup request to the worker, and update the TaskInfo on
                // the coordinator based on what we fetched from the worker. If we somehow cannot
                // get the cleanup request to the worker, the TaskInfo that we fetch for the worker
                // likely will not say the task is done however many times we try. In this case,
                // we have to set the local query info directly so that we stop trying to fetch
                // updated TaskInfo from the worker. This way, the task on the worker eventually
                // expires due to lack of activity.

                // This is required because the query state machine depends on TaskInfo (instead of task status)
                // to transition its own state.
                // TODO: Update the query state machine and stage state machine to depend on TaskStatus instead

                // Since this TaskInfo is updated in the client the "complete" flag will not be set,
                // indicating that the stats may not reflect the final stats on the worker.

                TaskStatus taskStatus = getTaskStatus();
                if (cancelledToResume.get()) {
                    // When the task is cancelled to resume, then make sure it gets the new state, so query can start resuming.
                    // Check for task state is in QueryInfo#areAllStagesDone.
                    taskStatus = TaskStatus.failWith(taskStatus, CANCELED_TO_RESUME, ImmutableList.of());
                }
                updateTaskInfo(getTaskInfo().withTaskStatus(taskStatus));
            }
        }, executor);
    }

    /**
     * Move the task directly to the failed state if there was a failure in this task
     */
    private void failTask(Throwable cause)
    {
        TaskStatus taskStatus = getTaskStatus();
        if (!taskStatus.getState().isDone()) {
            log.debug(cause, "Remote task %s failed with %s", taskStatus.getSelf(), cause);
        }

        ExecutionFailureInfo failureInfo = toFailure(cause);
        if (SystemSessionProperties.isSnapshotEnabled(session)) {
            if (isResumableFailure(failureInfo)) {
                // Determine if the failure can be recovered by resuming query from a previous checkpoing
                taskStatus = failWith(taskStatus, RESUMABLE_FAILURE, ImmutableList.of(failureInfo));
                taskStatusFetcher.updateTaskStatus(taskStatus);
                return;
            }
            log.debug(cause, "Snapshot: remote task %s failed with unresumable error %s", taskStatus.getSelf(), cause);
        }

        abort(failWith(taskStatus, FAILED, ImmutableList.of(failureInfo)));
    }

    private static boolean isResumableFailure(ExecutionFailureInfo failureInfo)
    {
        return failureInfo.getErrorCode().equals(TOO_MANY_REQUESTS_FAILED.toErrorCode())
                || failureInfo.getErrorCode().equals(REMOTE_HOST_GONE.toErrorCode())
                || failureInfo.getErrorCode().equals(REMOTE_TASK_MISMATCH.toErrorCode())
                || failureInfo.getErrorCode().equals(REMOTE_TASK_ERROR.toErrorCode())
                || failureInfo.getMessage() != null && failureInfo.getMessage().contains(WORKER_NODE_ERROR);
    }

    private HttpUriBuilder getHttpUriBuilder(TaskStatus taskStatus)
    {
        HttpUriBuilder uriBuilder = uriBuilderFrom(taskStatus.getSelf());
        if (summarizeTaskInfo) {
            uriBuilder.addParameter("summarize");
        }
        return uriBuilder;
    }

    private static Backoff createCleanupBackoff()
    {
        return new Backoff(10, new Duration(10, TimeUnit.MINUTES), Ticker.systemTicker(), ImmutableList.<Duration>builder()
                .add(new Duration(0, MILLISECONDS))
                .add(new Duration(100, MILLISECONDS))
                .add(new Duration(500, MILLISECONDS))
                .add(new Duration(1, SECONDS))
                .add(new Duration(10, SECONDS))
                .build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(getTaskInfo())
                .toString();
    }

    private class UpdateResponseHandler
            implements SimpleHttpResponseCallback<TaskInfo>
    {
        private final List<TaskSource> sources;

        private UpdateResponseHandler(List<TaskSource> sources)
        {
            this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
        }

        @Override
        public void success(TaskInfo value)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                try {
                    long tmpCurrentRequestStartNanos;
                    synchronized (HttpRemoteTask.this) {
                        currentRequest = null;
                        sendPlan.set(value.isNeedsPlan());
                        tmpCurrentRequestStartNanos = HttpRemoteTask.this.currentRequestStartNanos;
                    }
                    updateStats(tmpCurrentRequestStartNanos);
                    processTaskUpdate(value, sources);
                    updateErrorTracker.requestSucceeded();
                }
                finally {
                    sendUpdate();
                }
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                try {
                    long tmpCurrentRequestStartNanos;
                    synchronized (HttpRemoteTask.this) {
                        currentRequest = null;
                        tmpCurrentRequestStartNanos = HttpRemoteTask.this.currentRequestStartNanos;
                    }
                    updateStats(tmpCurrentRequestStartNanos);

                    // on failure assume we need to update again
                    needsUpdate.set(true);

                    // if task not already done, record error
                    TaskStatus taskStatus = getTaskStatus();
                    if (!taskStatus.getState().isDone()) {
                        updateErrorTracker.requestFailed(cause);
                    }
                }
                catch (Error e) {
                    failTask(e);
                    throw e;
                }
                catch (RuntimeException e) {
                    failTask(e);
                }
                finally {
                    sendUpdate();
                }
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                failTask(cause);
            }
        }

        private void updateStats(long currentRequestStartNanos)
        {
            Duration requestRoundTrip = Duration.nanosSince(currentRequestStartNanos);
            stats.updateRoundTripMillis(requestRoundTrip.toMillis());
        }
    }
}
