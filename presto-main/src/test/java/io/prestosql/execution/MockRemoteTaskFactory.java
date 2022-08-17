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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.execution.NodeTaskMap.PartitionedSplitCountTracker;
import io.prestosql.execution.buffer.LazyOutputBuffer;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.memory.MemoryPool;
import io.prestosql.memory.QueryContext;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Split;
import io.prestosql.operator.TaskContext;
import io.prestosql.operator.TaskStats;
import io.prestosql.snapshot.QuerySnapshotManager;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spiller.SpillSpaceTracker;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.testing.TestingMetadata.TestingColumnHandle;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.StateMachine.StateChangeListener;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.BROADCAST;
import static io.prestosql.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.prestosql.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.prestosql.testing.TestingPagesSerdeFactory.TESTING_SERDE_FACTORY;
import static io.prestosql.testing.TestingRecoveryUtils.NOOP_RECOVERY_UTILS;
import static io.prestosql.util.Failures.toFailures;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class MockRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final Executor executor;
    private final ScheduledExecutorService scheduledExecutor;

    public MockRemoteTaskFactory(Executor executor, ScheduledExecutorService scheduledExecutor)
    {
        this.executor = executor;
        this.scheduledExecutor = scheduledExecutor;
    }

    public MockRemoteTask createTableScanTask(TaskId taskId, InternalNode newNode, List<Split> splits, PartitionedSplitCountTracker partitionedSplitCountTracker)
    {
        // for now, just use a filler instanceId for tests
        String instanceId = "testInstanceId";
        Symbol symbol = new Symbol("column");
        PlanNodeId sourceId = new PlanNodeId("sourceId");
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId("test"),
                TableScanNode.newInstance(
                        sourceId,
                        TEST_TABLE_HANDLE,
                        ImmutableList.of(symbol),
                        ImmutableMap.of(symbol, new TestingColumnHandle("column")),
                        ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                        new UUID(0, 0),
                        0,
                        false),
                ImmutableMap.of(symbol, VARCHAR),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(sourceId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                ungroupedExecution(),
                StatsAndCosts.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
        for (Split sourceSplit : splits) {
            initialSplits.put(sourceId, sourceSplit);
        }
        return createRemoteTask(TEST_SESSION, taskId, instanceId, newNode, testFragment, initialSplits.build(), OptionalInt.empty(), createInitialEmptyOutputBuffers(BROADCAST),
                partitionedSplitCountTracker, true, Optional.empty(), new QuerySnapshotManager(taskId.getQueryId(), NOOP_RECOVERY_UTILS, TEST_SESSION));
    }

    @Override
    public MockRemoteTask createRemoteTask(
            Session session,
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
        return new MockRemoteTask(taskId, instanceId, fragment, node.getNodeIdentifier(), executor, scheduledExecutor, initialSplits, totalPartitions, partitionedSplitCountTracker);
    }

    public static final class MockRemoteTask
            implements RemoteTask
    {
        private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskStatus.STARTING_VERSION);

        private final URI location;
        private final String instanceId;
        private final TaskStateMachine taskStateMachine;
        private final TaskContext taskContext;
        private final OutputBuffer outputBuffer;
        private final String nodeId;

        private final PlanFragment fragment;

        @GuardedBy("this")
        private final Set<PlanNodeId> noMoreSplits = new HashSet<>();

        @GuardedBy("this")
        private final Multimap<PlanNodeId, Split> splits = HashMultimap.create();

        @GuardedBy("this")
        private int runningDrivers;

        @GuardedBy("this")
        private SettableFuture<?> whenSplitQueueHasSpace = SettableFuture.create();

        private final PartitionedSplitCountTracker partitionedSplitCountTracker;

        private final AtomicBoolean taskSpillCalled = new AtomicBoolean();

        public MockRemoteTask(TaskId taskId,
                String instanceId,
                PlanFragment fragment,
                String nodeId,
                Executor executor,
                ScheduledExecutorService scheduledExecutor,
                Multimap<PlanNodeId, Split> initialSplits,
                OptionalInt totalPartitions,
                PartitionedSplitCountTracker partitionedSplitCountTracker)
        {
            this.taskStateMachine = new TaskStateMachine(requireNonNull(taskId, "taskId is null"), requireNonNull(executor, "executor is null"));
            this.instanceId = instanceId;

            MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE));
            SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(new DataSize(1, GIGABYTE));
            QueryContext queryContext = new QueryContext(taskId.getQueryId(),
                    new DataSize(1, MEGABYTE),
                    new DataSize(2, MEGABYTE),
                    memoryPool,
                    new TestingGcMonitor(),
                    executor,
                    scheduledExecutor,
                    new DataSize(1, MEGABYTE),
                    spillSpaceTracker,
                    NOOP_RECOVERY_UTILS);
            this.taskContext = queryContext.addTaskContext(
                    taskStateMachine,
                    TEST_SESSION,
                    true,
                    true,
                    totalPartitions,
                    Optional.empty(),
                    TESTING_SERDE_FACTORY);

            this.location = URI.create("fake://task/" + taskId);

            this.outputBuffer = new LazyOutputBuffer(
                    taskId,
                    executor,
                    new DataSize(1, BYTE),
                    () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"));

            this.fragment = requireNonNull(fragment, "fragment is null");
            this.nodeId = requireNonNull(nodeId, "nodeId is null");
            splits.putAll(initialSplits);
            this.partitionedSplitCountTracker = requireNonNull(partitionedSplitCountTracker, "partitionedSplitCountTracker is null");
            partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
            updateSplitQueueSpace();
        }

        @Override
        public TaskId getTaskId()
        {
            return taskStateMachine.getTaskId();
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
            TaskState state = taskStateMachine.getState();
            List<ExecutionFailureInfo> failures = ImmutableList.of();
            if (state == TaskState.FAILED) {
                failures = toFailures(taskStateMachine.getFailureCauses());
            }

            return new TaskInfo(
                    new TaskStatus(
                            taskStateMachine.getTaskId(),
                            "fakeConfirmationInstanceId",
                            nextTaskInfoVersion.getAndIncrement(),
                            state,
                            location,
                            nodeId,
                            ImmutableSet.of(),
                            failures,
                            0,
                            0,
                            false,
                            new DataSize(0, BYTE),
                            new DataSize(0, BYTE),
                            new DataSize(0, BYTE),
                            new DataSize(0, BYTE),
                            0,
                            new Duration(0, MILLISECONDS),
                            ImmutableMap.of(),
                            Optional.empty()),
                    DateTime.now(),
                    outputBuffer.getInfo(),
                    ImmutableSet.of(),
                    taskContext.getTaskStats(),
                    true);
        }

        @Override
        public TaskStatus getTaskStatus()
        {
            TaskStats stats = taskContext.getTaskStats();
            return new TaskStatus(taskStateMachine.getTaskId(),
                    "fakeConfirmationInstanceId",
                    nextTaskInfoVersion.get(),
                    taskStateMachine.getState(),
                    location,
                    nodeId,
                    ImmutableSet.of(),
                    ImmutableList.of(),
                    stats.getQueuedPartitionedDrivers(),
                    stats.getRunningPartitionedDrivers(),
                    false,
                    stats.getPhysicalWrittenDataSize(),
                    stats.getUserMemoryReservation(),
                    stats.getSystemMemoryReservation(),
                    stats.getRevocableMemoryReservation(),
                    0,
                    new Duration(0, MILLISECONDS),
                    ImmutableMap.of(),
                    Optional.empty());
        }

        private synchronized void updateSplitQueueSpace()
        {
            if (getQueuedPartitionedSplitCount() < 9) {
                if (!whenSplitQueueHasSpace.isDone()) {
                    whenSplitQueueHasSpace.set(null);
                }
            }
            else {
                if (whenSplitQueueHasSpace.isDone()) {
                    whenSplitQueueHasSpace = SettableFuture.create();
                }
            }
        }

        public synchronized void finishSplits(int splits)
        {
            List<Map.Entry<PlanNodeId, Split>> toRemove = new ArrayList<>();
            Iterator<Map.Entry<PlanNodeId, Split>> iterator = this.splits.entries().iterator();
            while (toRemove.size() < splits && iterator.hasNext()) {
                toRemove.add(iterator.next());
            }
            for (Map.Entry<PlanNodeId, Split> entry : toRemove) {
                this.splits.remove(entry.getKey(), entry.getValue());
            }
            updateSplitQueueSpace();
        }

        public synchronized void clearSplits()
        {
            splits.clear();
            partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
            runningDrivers = 0;
            updateSplitQueueSpace();
        }

        public synchronized void startSplits(int maxRunning)
        {
            runningDrivers = splits.size();
            runningDrivers = Math.min(runningDrivers, maxRunning);
            updateSplitQueueSpace();
        }

        @Override
        public void start()
        {
            taskStateMachine.addStateChangeListener(newValue -> {
                if (newValue.isDone()) {
                    clearSplits();
                }
            });
        }

        @Override
        public void addSplits(Multimap<PlanNodeId, Split> splits)
        {
            synchronized (this) {
                this.splits.putAll(splits);
            }
            partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
            updateSplitQueueSpace();
        }

        @Override
        public synchronized void noMoreSplits(PlanNodeId sourceId)
        {
            noMoreSplits.add(sourceId);

            boolean allSourcesComplete = Stream.concat(fragment.getPartitionedSourceNodes().stream(), fragment.getRemoteSourceNodes().stream())
                    .filter(Objects::nonNull)
                    .map(PlanNode::getId)
                    .allMatch(noMoreSplits::contains);

            if (allSourcesComplete) {
                taskStateMachine.finished();
            }
        }

        @Override
        public void noMoreSplits(PlanNodeId sourceId, Lifespan lifespan)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setOutputBuffers(OutputBuffers outputBuffers)
        {
            outputBuffer.setOutputBuffers(outputBuffers);
        }

        @Override
        public void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener)
        {
            taskStateMachine.addStateChangeListener(newValue -> stateChangeListener.stateChanged(getTaskStatus()));
        }

        @Override
        public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener)
        {
            AtomicBoolean done = new AtomicBoolean();
            StateChangeListener<TaskState> fireOnceStateChangeListener = state -> {
                if (state.isDone() && done.compareAndSet(false, true)) {
                    stateChangeListener.stateChanged(getTaskInfo());
                }
            };
            taskStateMachine.addStateChangeListener(fireOnceStateChangeListener);
            fireOnceStateChangeListener.stateChanged(taskStateMachine.getState());
        }

        @Override
        public synchronized ListenableFuture<?> whenSplitQueueHasSpace(int threshold)
        {
            return nonCancellationPropagating(whenSplitQueueHasSpace);
        }

        @Override
        public void cancel()
        {
            taskStateMachine.cancel(TaskState.CANCELED);
        }

        @Override
        public void cancelToResume()
        {
            taskStateMachine.cancel(TaskState.CANCELED_TO_RESUME);
        }

        @Override
        public void suspend()
        {
            taskStateMachine.suspend();
        }

        @Override
        public void resume()
        {
            taskStateMachine.resume();
        }

        @Override
        public void abort()
        {
            taskStateMachine.cancel(TaskState.ABORTED);
            clearSplits();
        }

        @Override
        public void spillRevocableMemory()
        {
            taskSpillCalled.set(true);
        }

        @Override
        public int getPartitionedSplitCount()
        {
            if (taskStateMachine.getState().isDone()) {
                return 0;
            }
            synchronized (this) {
                int count = 0;
                for (PlanNodeId partitionedSource : fragment.getPartitionedSources()) {
                    Collection<Split> partitionedSplits = splits.get(partitionedSource);
                    count += partitionedSplits.size();
                }
                return count;
            }
        }

        @Override
        public synchronized int getQueuedPartitionedSplitCount()
        {
            if (taskStateMachine.getState().isDone()) {
                return 0;
            }
            return getPartitionedSplitCount() - runningDrivers;
        }
    }
}
