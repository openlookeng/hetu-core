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

import com.google.common.collect.Multimap;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.NodeTaskMap.PartitionedSplitCountTracker;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.RemoteTask;
import io.prestosql.execution.RemoteTaskFactory;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.execution.TaskStatus;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Split;
import io.prestosql.operator.ForScheduler;
import io.prestosql.protocol.Codec;
import io.prestosql.protocol.SmileCodec;
import io.prestosql.server.remotetask.HttpRemoteTask;
import io.prestosql.server.remotetask.RemoteTaskStats;
import io.prestosql.snapshot.QuerySnapshotManager;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.sql.planner.PlanFragment;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.protocol.JsonCodecWrapper.wrapJsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class HttpRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final HttpClient httpClient;
    private final LocationFactory locationFactory;
    private final Codec<TaskStatus> taskStatusCodec;
    private final Codec<TaskInfo> taskInfoCodec;
    private final Codec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final Duration maxErrorDuration;
    private final Duration taskStatusRefreshMaxWait;
    private final Duration taskInfoUpdateInterval;
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final ThreadPoolExecutorMBean executorMBean;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final ScheduledExecutorService errorScheduledExecutor;
    private final RemoteTaskStats stats;
    private final boolean isBinaryEncoding;

    @Inject
    public HttpRemoteTaskFactory(QueryManagerConfig config,
            TaskManagerConfig taskConfig,
            @ForScheduler HttpClient httpClient,
            LocationFactory locationFactory,
            JsonCodec<TaskStatus> taskStatusJsonCodec,
            SmileCodec<TaskStatus> taskStatusSmileCodec,
            JsonCodec<TaskInfo> taskInfoJsonCodec,
            SmileCodec<TaskInfo> taskInfoSmileCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestJsonCodec,
            SmileCodec<TaskUpdateRequest> taskUpdateRequestSmileCodec,
            RemoteTaskStats stats, InternalCommunicationConfig internalCommunicationConfig)
    {
        this.httpClient = httpClient;
        this.locationFactory = locationFactory;

        this.maxErrorDuration = config.getRemoteTaskMaxErrorDuration();
        this.taskStatusRefreshMaxWait = taskConfig.getStatusRefreshMaxWait();
        this.taskInfoUpdateInterval = taskConfig.getInfoUpdateInterval();
        this.coreExecutor = newCachedThreadPool(daemonThreadsNamed("remote-task-callback-%s"));
        this.executor = new BoundedExecutor(coreExecutor, config.getRemoteTaskMaxCallbackThreads());
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) coreExecutor);
        this.stats = requireNonNull(stats, "stats is null");
        requireNonNull(internalCommunicationConfig, "internaCommnicationConfig is null");
        this.isBinaryEncoding = internalCommunicationConfig.isBinaryEncoding();

        if (isBinaryEncoding) {
            this.taskStatusCodec = taskStatusSmileCodec;
            this.taskInfoCodec = taskInfoSmileCodec;
            this.taskUpdateRequestCodec = taskUpdateRequestSmileCodec;
        }
        else {
            this.taskStatusCodec = wrapJsonCodec(taskStatusJsonCodec);
            this.taskInfoCodec = wrapJsonCodec(taskInfoJsonCodec);
            this.taskUpdateRequestCodec = wrapJsonCodec(taskUpdateRequestJsonCodec);
        }

        this.updateScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("task-info-update-scheduler-%s"));
        this.errorScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("remote-task-error-delay-%s"));
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @PreDestroy
    public void stop()
    {
        coreExecutor.shutdownNow();
        updateScheduledExecutor.shutdownNow();
        errorScheduledExecutor.shutdownNow();
    }

    @Override
    public RemoteTask createRemoteTask(Session session,
            TaskId taskId,
            String instanceId,
            InternalNode node,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OptionalInt totalPartitions,
            OutputBuffers outputBuffers,
            PartitionedSplitCountTracker partitionedSplitCountTracker,
            boolean summarizeTaskInfo,
            Optional<PlanNodeId> parent,
            QuerySnapshotManager snapshotManager)
    {
        return new HttpRemoteTask(session,
                taskId,
                instanceId,
                node.getNodeIdentifier(),
                locationFactory.createTaskLocation(node, taskId),
                fragment,
                initialSplits,
                totalPartitions,
                outputBuffers,
                httpClient,
                executor,
                updateScheduledExecutor,
                errorScheduledExecutor,
                maxErrorDuration,
                taskStatusRefreshMaxWait,
                taskInfoUpdateInterval,
                summarizeTaskInfo,
                taskStatusCodec,
                taskInfoCodec,
                taskUpdateRequestCodec,
                partitionedSplitCountTracker,
                stats,
                isBinaryEncoding,
                parent,
                snapshotManager);
    }
}
