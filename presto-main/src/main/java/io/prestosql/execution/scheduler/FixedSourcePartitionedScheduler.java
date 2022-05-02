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
package io.prestosql.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.RemoteTask;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.execution.scheduler.ScheduleResult.BlockedReason;
import io.prestosql.execution.scheduler.group.DynamicLifespanScheduler;
import io.prestosql.execution.scheduler.group.FixedLifespanScheduler;
import io.prestosql.execution.scheduler.group.LifespanScheduler;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Split;
import io.prestosql.operator.StageExecutionDescriptor;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.split.SplitSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.prestosql.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsSourceScheduler;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class FixedSourcePartitionedScheduler
        implements StageScheduler
{
    private static final Logger LOG = Logger.get(FixedSourcePartitionedScheduler.class);

    private final SqlStageExecution stage;
    private final List<InternalNode> nodes;
    private final List<SourceScheduler> sourceSchedulers;
    private final List<ConnectorPartitionHandle> partitionHandles;
    private boolean scheduledTasks;
    private final Optional<LifespanScheduler> groupedLifespanScheduler;

    public FixedSourcePartitionedScheduler(
            SqlStageExecution stage,
            Map<PlanNodeId, SplitSource> splitSources,
            StageExecutionDescriptor stageExecutionDescriptor,
            List<PlanNodeId> schedulingOrder,
            List<InternalNode> nodes,
            BucketNodeMap bucketNodeMap,
            int splitBatchSize,
            OptionalInt concurrentLifespansPerTask,
            NodeSelector nodeSelector,
            List<ConnectorPartitionHandle> partitionHandles,
            Session session,
            HeuristicIndexerManager heuristicIndexerManager)
    {
        requireNonNull(stage, "stage is null");
        requireNonNull(splitSources, "splitSources is null");
        requireNonNull(bucketNodeMap, "bucketNodeMap is null");
        checkArgument(!requireNonNull(nodes, "nodes is null").isEmpty(), "nodes is empty");
        requireNonNull(partitionHandles, "partitionHandles is null");

        this.stage = stage;
        this.nodes = ImmutableList.copyOf(nodes);
        this.partitionHandles = ImmutableList.copyOf(partitionHandles);

        checkArgument(splitSources.keySet().equals(ImmutableSet.copyOf(schedulingOrder)));

        BucketedSplitPlacementPolicy splitPlacementPolicy = new BucketedSplitPlacementPolicy(nodeSelector, nodes, bucketNodeMap, stage::getAllTasks);

        ArrayList<SourceScheduler> sourceSchedulerArrayList = new ArrayList<>();
        checkArgument(
                partitionHandles.equals(ImmutableList.of(NOT_PARTITIONED)) != stageExecutionDescriptor.isStageGroupedExecution(),
                "PartitionHandles should be [NOT_PARTITIONED] if and only if all scan nodes use ungrouped execution strategy");
        int nodeCount = nodes.size();
        int concurrentLifespans;
        if (concurrentLifespansPerTask.isPresent() && concurrentLifespansPerTask.getAsInt() * nodeCount <= partitionHandles.size()) {
            concurrentLifespans = concurrentLifespansPerTask.getAsInt() * nodeCount;
        }
        else {
            concurrentLifespans = partitionHandles.size();
        }

        boolean firstPlanNode = true;
        Optional<LifespanScheduler> groupedLifespanSchedulerOptional = Optional.empty();
        for (PlanNodeId planNodeId : schedulingOrder) {
            SplitSource splitSource = splitSources.get(planNodeId);
            boolean groupedExecutionForScanNode = stageExecutionDescriptor.isScanGroupedExecution(planNodeId);

            SourceScheduler sourceScheduler = newSourcePartitionedSchedulerAsSourceScheduler(
                    stage,
                    planNodeId,
                    splitSource,
                    splitPlacementPolicy,
                    Math.max(splitBatchSize / concurrentLifespans, 1),
                    groupedExecutionForScanNode,
                    session,
                    heuristicIndexerManager);

            if (stageExecutionDescriptor.isStageGroupedExecution() && !groupedExecutionForScanNode) {
                sourceScheduler = new AsGroupedSourceScheduler(sourceScheduler);
            }
            sourceSchedulerArrayList.add(sourceScheduler);

            if (firstPlanNode) {
                firstPlanNode = false;
                if (!stageExecutionDescriptor.isStageGroupedExecution()) {
                    sourceScheduler.startLifespan(Lifespan.taskWide(), NOT_PARTITIONED);
                    sourceScheduler.noMoreLifespans();
                }
                else {
                    LifespanScheduler lifespanScheduler;
                    if (bucketNodeMap.isDynamic()) {
                        // Callee of the constructor guarantees dynamic bucket node map will only be
                        // used when the stage has no remote source.
                        //
                        // When the stage has no remote source, any scan is grouped execution guarantees
                        // all scan is grouped execution.
                        lifespanScheduler = new DynamicLifespanScheduler(bucketNodeMap, nodes, partitionHandles, concurrentLifespansPerTask);
                    }
                    else {
                        lifespanScheduler = new FixedLifespanScheduler(bucketNodeMap, partitionHandles, concurrentLifespansPerTask);
                    }

                    // Schedule the first few lifespans
                    lifespanScheduler.scheduleInitial(sourceScheduler);
                    // Schedule new lifespans for finished ones
                    stage.addCompletedDriverGroupsChangedListener(lifespanScheduler::onLifespanFinished);
                    groupedLifespanSchedulerOptional = Optional.of(lifespanScheduler);
                }
            }
        }
        this.groupedLifespanScheduler = groupedLifespanSchedulerOptional;
        this.sourceSchedulers = sourceSchedulerArrayList;
    }

    private ConnectorPartitionHandle partitionHandleFor(Lifespan lifespan)
    {
        if (lifespan.isTaskWide()) {
            return NOT_PARTITIONED;
        }
        return partitionHandles.get(lifespan.getId());
    }

    @Override
    public ScheduleResult schedule()
    {
        return schedule(1);
    }

    @Override
    public ScheduleResult schedule(int maxSplitGroup)
    {
        // schedule a task on every node in the distribution
        List<RemoteTask> newTasks = ImmutableList.of();
        if (!scheduledTasks) {
            OptionalInt totalPartitions = OptionalInt.of(nodes.size());
            newTasks = Streams.mapWithIndex(
                    nodes.stream(),
                    (node, id) -> stage.scheduleTask(node, toIntExact(id), totalPartitions))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableList());
            scheduledTasks = true;
        }

        boolean allBlocked = true;
        List<ListenableFuture<?>> blocked = new ArrayList<>();
        BlockedReason blockedReason = BlockedReason.NO_ACTIVE_DRIVER_GROUP;

        if (groupedLifespanScheduler.isPresent()) {
            // Start new driver groups on the first scheduler if necessary,
            // i.e. when previous ones have finished execution (not finished scheduling).
            //
            // Invoke schedule method to get a new SettableFuture every time.
            // Reusing previously returned SettableFuture could lead to the ListenableFuture retaining too many listeners.
            blocked.add(groupedLifespanScheduler.get().schedule(sourceSchedulers.get(0)));
        }

        int splitsScheduled = 0;
        Iterator<SourceScheduler> schedulerIterator = sourceSchedulers.iterator();
        List<Lifespan> driverGroupsToStart = ImmutableList.of();
        boolean shouldInvokeNoMoreDriverGroups = false;
        while (schedulerIterator.hasNext()) {
            SourceScheduler sourceScheduler = schedulerIterator.next();

            for (Lifespan lifespan : driverGroupsToStart) {
                sourceScheduler.startLifespan(lifespan, partitionHandleFor(lifespan));
            }
            if (shouldInvokeNoMoreDriverGroups) {
                sourceScheduler.noMoreLifespans();
            }

            ScheduleResult schedule = sourceScheduler.schedule(maxSplitGroup);
            splitsScheduled += schedule.getSplitsScheduled();
            if (schedule.getBlockedReason().isPresent()) {
                blocked.add(schedule.getBlocked());
                blockedReason = blockedReason.combineWith(schedule.getBlockedReason().get());
            }
            else {
                verify(schedule.getBlocked().isDone(), "blockedReason not provided when scheduler is blocked");
                allBlocked = false;
            }

            driverGroupsToStart = sourceScheduler.drainCompletedLifespans();

            if (schedule.isFinished()) {
                stage.schedulingComplete(sourceScheduler.getPlanNodeId());
                schedulerIterator.remove();
                sourceScheduler.close();
                shouldInvokeNoMoreDriverGroups = true;
            }
            else {
                shouldInvokeNoMoreDriverGroups = false;
            }
        }

        if (allBlocked) {
            return new ScheduleResult(sourceSchedulers.isEmpty(), newTasks, whenAnyComplete(blocked), blockedReason, splitsScheduled);
        }
        else {
            return new ScheduleResult(sourceSchedulers.isEmpty(), newTasks, splitsScheduled);
        }
    }

    @Override
    public void close()
    {
        for (SourceScheduler sourceScheduler : sourceSchedulers) {
            try {
                sourceScheduler.close();
            }
            catch (Throwable t) {
                LOG.warn(t, "Error closing split source");
            }
        }
        sourceSchedulers.clear();
    }

    public static class BucketedSplitPlacementPolicy
            implements SplitPlacementPolicy
    {
        private final NodeSelector nodeSelector;
        private final List<InternalNode> allNodes;
        private final BucketNodeMap bucketNodeMap;
        private final Supplier<? extends List<RemoteTask>> remoteTasks;

        public BucketedSplitPlacementPolicy(
                NodeSelector nodeSelector,
                List<InternalNode> allNodes,
                BucketNodeMap bucketNodeMap,
                Supplier<? extends List<RemoteTask>> remoteTasks)
        {
            this.nodeSelector = requireNonNull(nodeSelector, "nodeSelector is null");
            this.allNodes = ImmutableList.copyOf(requireNonNull(allNodes, "allNodes is null"));
            this.bucketNodeMap = requireNonNull(bucketNodeMap, "bucketNodeMap is null");
            this.remoteTasks = requireNonNull(remoteTasks, "remoteTasks is null");
        }

        @Override
        public SplitPlacementResult computeAssignments(Set<Split> splits, SqlStageExecution stage)
        {
            return nodeSelector.computeAssignments(splits, remoteTasks.get(), bucketNodeMap);
        }

        @Override
        public void lockDownNodes()
        {
        }

        @Override
        public List<InternalNode> allNodes()
        {
            return allNodes;
        }

        public InternalNode getNodeForBucket(int bucketId)
        {
            return bucketNodeMap.getAssignedNode(bucketId).get();
        }
    }

    private static class AsGroupedSourceScheduler
            implements SourceScheduler
    {
        private final SourceScheduler sourceScheduler;
        private boolean started;
        private boolean completed;
        private final List<Lifespan> pendingCompleted;

        public AsGroupedSourceScheduler(SourceScheduler sourceScheduler)
        {
            this.sourceScheduler = requireNonNull(sourceScheduler, "sourceScheduler is null");
            pendingCompleted = new ArrayList<>();
        }

        @Override
        public ScheduleResult schedule()
        {
            return sourceScheduler.schedule();
        }

        @Override
        public ScheduleResult schedule(int maxSplitGroupSize)
        {
            return sourceScheduler.schedule(maxSplitGroupSize);
        }

        @Override
        public void close()
        {
            sourceScheduler.close();
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return sourceScheduler.getPlanNodeId();
        }

        @Override
        public void startLifespan(Lifespan lifespan, ConnectorPartitionHandle partitionHandle)
        {
            pendingCompleted.add(lifespan);
            if (started) {
                return;
            }
            started = true;
            sourceScheduler.startLifespan(Lifespan.taskWide(), NOT_PARTITIONED);
            sourceScheduler.noMoreLifespans();
        }

        @Override
        public void noMoreLifespans()
        {
            checkState(started);
        }

        @Override
        public List<Lifespan> drainCompletedLifespans()
        {
            if (!completed) {
                List<Lifespan> lifespans = sourceScheduler.drainCompletedLifespans();
                if (lifespans.isEmpty()) {
                    return ImmutableList.of();
                }
                checkState(ImmutableList.of(Lifespan.taskWide()).equals(lifespans));
                completed = true;
            }
            List<Lifespan> result = ImmutableList.copyOf(pendingCompleted);
            pendingCompleted.clear();
            return result;
        }
    }
}
