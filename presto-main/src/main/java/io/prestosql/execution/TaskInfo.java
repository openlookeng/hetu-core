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
package io.prestosql.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.prestosql.execution.buffer.BufferInfo;
import io.prestosql.execution.buffer.OutputBufferInfo;
import io.prestosql.operator.TaskStats;
import io.prestosql.snapshot.RestoreResult;
import io.prestosql.snapshot.SnapshotResult;
import io.prestosql.spi.plan.PlanNodeId;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.execution.TaskStatus.initialTaskStatus;
import static io.prestosql.execution.buffer.BufferState.OPEN;
import static java.util.Objects.requireNonNull;

@Immutable
public class TaskInfo
{
    private final TaskStatus taskStatus;
    private final DateTime lastHeartbeat;
    private final OutputBufferInfo outputBuffers;
    private final Set<PlanNodeId> noMoreSplits;
    private final TaskStats stats;

    private final boolean needsPlan;
    // snapshotCaptureResult and snapshotRestoreResult are used to store result of snapshot capture and restore. They are empty when the following conditions happened:
    // (1) Snapshot is not enabled
    // (2) Snapshot is enabled but no data for capture/restore result
    private final Map<Long, SnapshotResult> snapshotCaptureResult;
    private final Optional<RestoreResult> snapshotRestoreResult;

    public TaskInfo(TaskStatus taskStatus,
                    DateTime lastHeartbeat,
                    OutputBufferInfo outputBuffers,
                    Set<PlanNodeId> noMoreSplits,
                    TaskStats stats,
                    boolean needsPlan)
    {
        this(taskStatus, lastHeartbeat, outputBuffers, noMoreSplits, stats, needsPlan, new TreeMap<>(), Optional.empty());
    }

    @JsonCreator
    public TaskInfo(@JsonProperty("taskStatus") TaskStatus taskStatus,
            @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
            @JsonProperty("outputBuffers") OutputBufferInfo outputBuffers,
            @JsonProperty("noMoreSplits") Set<PlanNodeId> noMoreSplits,
            @JsonProperty("stats") TaskStats stats,
            @JsonProperty("needsPlan") boolean needsPlan,
            @JsonProperty("snapshotCaptureResult") Map<Long, SnapshotResult> snapshotCaptureResult,
            @JsonProperty("snapshotRestoreResult") Optional<RestoreResult> snapshotRestoreResult)
    {
        this.taskStatus = requireNonNull(taskStatus, "taskStatus is null");
        this.lastHeartbeat = requireNonNull(lastHeartbeat, "lastHeartbeat is null");
        this.outputBuffers = requireNonNull(outputBuffers, "outputBuffers is null");
        this.noMoreSplits = requireNonNull(noMoreSplits, "noMoreSplits is null");
        this.stats = requireNonNull(stats, "stats is null");

        this.needsPlan = needsPlan;
        this.snapshotCaptureResult = snapshotCaptureResult;
        this.snapshotRestoreResult = snapshotRestoreResult;
    }

    @JsonProperty
    public TaskStatus getTaskStatus()
    {
        return taskStatus;
    }

    @JsonProperty
    public DateTime getLastHeartbeat()
    {
        return lastHeartbeat;
    }

    @JsonProperty
    public OutputBufferInfo getOutputBuffers()
    {
        return outputBuffers;
    }

    @JsonProperty
    public Set<PlanNodeId> getNoMoreSplits()
    {
        return noMoreSplits;
    }

    @JsonProperty
    public TaskStats getStats()
    {
        return stats;
    }

    @JsonProperty
    public boolean isNeedsPlan()
    {
        return needsPlan;
    }

    @JsonProperty
    public Map<Long, SnapshotResult> getSnapshotCaptureResult()
    {
        return snapshotCaptureResult;
    }

    @JsonProperty
    public Optional<RestoreResult> getSnapshotRestoreResult()
    {
        return snapshotRestoreResult;
    }

    public TaskInfo summarize()
    {
        if (taskStatus.getState().isDone()) {
            return new TaskInfo(taskStatus, lastHeartbeat, outputBuffers.summarize(), noMoreSplits, stats.summarizeFinal(), needsPlan, snapshotCaptureResult, snapshotRestoreResult);
        }
        return new TaskInfo(taskStatus, lastHeartbeat, outputBuffers.summarize(), noMoreSplits, stats.summarize(), needsPlan, snapshotCaptureResult, snapshotRestoreResult);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskStatus.getTaskId())
                .add("state", taskStatus.getState())
                .toString();
    }

    public static TaskInfo createInitialTask(TaskId taskId, URI location, String nodeId, List<BufferInfo> bufferStates, TaskStats taskStats)
    {
        return new TaskInfo(
                initialTaskStatus(taskId, location, nodeId),
                DateTime.now(),
                new OutputBufferInfo("UNINITIALIZED", OPEN, true, true, 0, 0, 0, 0, bufferStates),
                ImmutableSet.of(),
                taskStats,
                true);
    }

    public TaskInfo withTaskStatus(TaskStatus newTaskStatus)
    {
        return new TaskInfo(newTaskStatus, lastHeartbeat, outputBuffers, noMoreSplits, stats, needsPlan, snapshotCaptureResult, snapshotRestoreResult);
    }
}
