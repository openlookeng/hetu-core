/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.resourcemanager;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AtomicDouble;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.StageId;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.StageStats;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.server.ResourceGroupInfo;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.KillPolicy;
import io.prestosql.spi.resourcegroups.ResourceGroup;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.SchedulingPolicy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.resourcemanager.QueryExecutionModifier.KILL_QUERY;
import static io.prestosql.resourcemanager.QueryExecutionModifier.NO_OP;
import static io.prestosql.resourcemanager.QueryExecutionModifier.RESUME_QUERY;
import static io.prestosql.resourcemanager.QueryExecutionModifier.SPILL_REVOCABLE;
import static io.prestosql.resourcemanager.QueryExecutionModifier.SUSPEND_QUERY;
import static io.prestosql.resourcemanager.QueryExecutionModifier.THROTTLE_SPLITS;
import static java.util.function.Function.identity;

public class QueryResourceManager
        implements ResourceManager
{
    private static final Logger logger = Logger.get(QueryResourceManager.class);
    private static final double HARD_LIMIT_FACTOR = 1.5;

    private final QueryId queryId;
    private Optional<DataSize> softMemoryLimit;
    private Optional<Duration> softCpuLimit;
    private Optional<Duration> hardCpuLimit;
    private Optional<DataSize> diskUseLimit = Optional.empty();

    private final Optional<KillPolicy> killPolicy;

    private final QueryResourceManagerService.ResourceUpdateListener resourceUpdateListener;
    private final BasicResourceStats queryResources = new BasicResourceStats();
    private final Map<String, BasicResourceStats> nodeResourceStats = new ConcurrentHashMap<>();
    private Map<StageId, StageInfo> lastStagesInfo = new HashMap<>();

    private final ResourceGroupManager queryResourceGroupMgr;
    private final ResourceGroupId queryResourceGroupId;

    private boolean isSuspendSuggested;

    public QueryResourceManager(QueryId queryId, Session session,
                                ResourceGroupId resourceGroupId,
                                ResourceGroupManager resourceGroupManager,
                                QueryResourceManagerService.ResourceUpdateListener resourceUpdateListener)
    {
        /* Todo: get configs for resource limit from resource group! */

        this.queryId = queryId;
        this.softMemoryLimit = Optional.empty();
        this.softCpuLimit = Optional.empty();
        this.hardCpuLimit = Optional.empty();
        this.killPolicy = Optional.empty();
        this.resourceUpdateListener = resourceUpdateListener;
        this.queryResourceGroupMgr = resourceGroupManager;
        this.queryResourceGroupId = resourceGroupId;
    }

    public QueryResourceManager(QueryId queryId, Optional<DataSize> softMemoryLimit, Optional<Double> softMemoryLimitFraction,
                                Optional<DataSize> softReservedMemory, Optional<Double> softReservedFraction,
                                int maxQueued, Optional<Integer> softConcurrencyLimit, Optional<Integer> hardReservedConcurrency,
                                int hardConcurrencyLimit, Optional<SchedulingPolicy> schedulingPolicy,
                                Optional<Integer> schedulingWeight, ResourceGroup resourceGroup, Optional<Duration> softCpuLimit,
                                Optional<Duration> hardCpuLimit, Optional<KillPolicy> killPolicy, ResourceGroupManager resourceGroupManager,
                                QueryResourceManagerService.ResourceUpdateListener resourceUpdateListener)
    {
        this.queryId = queryId;
        this.softMemoryLimit = softMemoryLimit;
        this.softCpuLimit = softCpuLimit;
        this.hardCpuLimit = hardCpuLimit;
        this.killPolicy = killPolicy;
        this.resourceUpdateListener = resourceUpdateListener;
        this.queryResourceGroupMgr = resourceGroupManager;
        this.queryResourceGroupId = resourceGroup.getId();
    }

    @Override
    public boolean setResourceLimit(DataSize memoryLimit, Duration cpuLimit, DataSize ioLimit)
    {
        long softReservedMemory = queryResourceGroupMgr.getSoftReservedMemory(queryResourceGroupId);
        long softCpuMax = queryResourceGroupMgr.getSoftCpuLimit(queryResourceGroupId);
        long hardCpuMax = queryResourceGroupMgr.getHardCpuLimit(queryResourceGroupId);

        ResourceGroupInfo resourceGroupInfo = queryResourceGroupMgr.getResourceGroupInfo(queryResourceGroupId);
        Optional<Duration> softCpuLimitBackup = softCpuLimit;
        Optional<Duration> hardCpuLimitBackup = hardCpuLimit;

        if (cpuLimit.compareTo(Duration.succinctDuration(1, TimeUnit.SECONDS)) > 0) {
            if (cpuLimit.toMillis() <= softCpuMax) {
                softCpuLimit = Optional.of(cpuLimit);
            }
            else {
                return false;
            }

            Duration hardLimitEval = new Duration(cpuLimit.getValue(TimeUnit.SECONDS) * HARD_LIMIT_FACTOR, TimeUnit.SECONDS);
            if (hardLimitEval.toMillis() <= hardCpuMax) {
                hardCpuLimit = Optional.of(hardLimitEval);
            }
            else {
                softCpuLimit = softCpuLimitBackup;
                return false;
            }
        }

        if (memoryLimit.compareTo(DataSize.succinctBytes(1)) > 0) {
            if (resourceGroupInfo.getSoftMemoryLimit().compareTo(memoryLimit) >= 0 && memoryLimit.toBytes() <= softReservedMemory) {
                softMemoryLimit = Optional.of(memoryLimit);
            }
            else {
                softCpuLimit = softCpuLimitBackup;
                hardCpuLimit = hardCpuLimitBackup;
                return false;
            }
        }

        diskUseLimit = Optional.of(ioLimit);
        return true;
    }

    @Override
    public DataSize getMemoryLimit()
    {
        return softMemoryLimit.orElse(DataSize.succinctBytes(0));
    }

    @Override
    public void updateStats(List<StageStats> stats)
    {
        AtomicDouble cpuTime = new AtomicDouble();
        AtomicDouble userMem = new AtomicDouble();
        AtomicLong io = new AtomicLong();
        stats.forEach(stageStats -> {
            cpuTime.addAndGet(stageStats.getTotalCpuTime().getValue());
            userMem.addAndGet(stageStats.getCumulativeUserMemory());
            io.addAndGet(stageStats.getInternalNetworkInputDataSize().toBytes());
        });

        queryResources.cpuTime = Duration.succinctDuration(cpuTime.longValue(), TimeUnit.NANOSECONDS);
        queryResources.memCurrent = DataSize.succinctBytes(userMem.longValue());
        queryResources.ioCurrent = DataSize.succinctBytes(io.get());

        checkResourceLimits();
    }

    @Override
    public QueryExecutionModifier updateStats(ImmutableList<StageInfo> stats)
    {
        double cpuTime = 0.0;
        long userMem = 0;
        long revocableMem = 0;
        long io = 0;

        StageInfo lastStage = null;
        for (StageInfo stageInfo : stats) {
            lastStage = lastStagesInfo.getOrDefault(stageInfo.getStageId(), null);

            Map<TaskId, TaskInfo> lastTasks = (lastStage == null) ? new HashMap<>() : lastStage.getTasks().stream().collect(toImmutableMap(k -> k.getTaskStatus().getTaskId(), identity()));

            for (TaskInfo taskInfo : stageInfo.getTasks()) {
                /* Consider finished stages also for cumulative result */
                cpuTime += taskInfo.getStats().getTotalCpuTime().getValue(TimeUnit.MICROSECONDS);
                userMem += taskInfo.getStats().getUserMemoryReservation().toBytes();
                revocableMem += taskInfo.getStats().getRevocableMemoryReservation().toBytes();
                io += taskInfo.getStats().getInternalNetworkInputDataSize().toBytes();

                /* If already finished then can ignore the stats for given stage */
                if (lastStage == null || !lastStage.getState().isDone()) {
                    TaskInfo lastTask = lastTasks.getOrDefault(taskInfo.getTaskStatus().getTaskId(), null);
                    double cpuTimeDiff = (lastTask == null) ? taskInfo.getStats().getTotalCpuTime().getValue(TimeUnit.MICROSECONDS) :
                            ((diffDuration(taskInfo.getStats().getTotalCpuTime(), lastTask.getStats().getTotalCpuTime()) > 0) ?
                                diffDuration(taskInfo.getStats().getTotalCpuTime(), lastTask.getStats().getTotalCpuTime()) : 0.0);
                    long userMemDiff = (lastTask == null) ? taskInfo.getStats().getUserMemoryReservation().toBytes() :
                            ((diffDataSize(taskInfo.getStats().getUserMemoryReservation(), lastTask.getStats().getUserMemoryReservation()) > 0) ?
                                diffDataSize(taskInfo.getStats().getUserMemoryReservation(), lastTask.getStats().getUserMemoryReservation()) : 0);
                    long revocableMemDiff = (lastTask == null) ? taskInfo.getStats().getRevocableMemoryReservation().toBytes() :
                            ((diffDataSize(taskInfo.getStats().getRevocableMemoryReservation(), lastTask.getStats().getRevocableMemoryReservation()) > 0) ?
                                diffDataSize(taskInfo.getStats().getRevocableMemoryReservation(), lastTask.getStats().getRevocableMemoryReservation()) : 0);
                    long ioDiff = (lastTask == null) ? taskInfo.getStats().getInternalNetworkInputDataSize().toBytes() :
                            ((diffDataSize(taskInfo.getStats().getInternalNetworkInputDataSize(), lastTask.getStats().getInternalNetworkInputDataSize()) > 0) ?
                                diffDataSize(taskInfo.getStats().getInternalNetworkInputDataSize(), lastTask.getStats().getInternalNetworkInputDataSize()) : 0);

                    /* record worker node level resource as well! */
                    BasicResourceStats stat = nodeResourceStats.computeIfAbsent(taskInfo.getTaskStatus().getNodeId(), v -> new BasicResourceStats());
                    stat.memCurrent = addDataSize(stat.memCurrent, userMemDiff);
                    stat.revocableMem = addDataSize(stat.revocableMem, revocableMemDiff);
                    stat.cpuTime = addDuration(stat.cpuTime, cpuTimeDiff);
                    stat.ioCurrent = addDataSize(stat.ioCurrent, ioDiff);
                }
            }
        }

        queryResources.cpuTime = Duration.succinctDuration(cpuTime, TimeUnit.MICROSECONDS);
        queryResources.memCurrent = DataSize.succinctBytes(userMem);
        queryResources.revocableMem = DataSize.succinctBytes(revocableMem);
        queryResources.ioCurrent = DataSize.succinctBytes(io);

        lastStagesInfo = stats.stream().collect(toImmutableMap(StageInfo::getStageId, identity()));

        return checkResourceLimits();
    }

    @Override
    public boolean canScheduleMore()
    {
        long cachedMemoryUsage = queryResourceGroupMgr.getCachedMemoryUsage(queryResourceGroupId);
        long softReservedMemory = queryResourceGroupMgr.getSoftReservedMemory(queryResourceGroupId);

        if (cachedMemoryUsage < softReservedMemory) {
            return true;
        }
        return false;
    }

    private static Duration addDuration(Duration a, double b)
    {
        return Duration.succinctDuration(a.getValue(TimeUnit.MICROSECONDS) + b, TimeUnit.MICROSECONDS);
    }

    private static DataSize addDataSize(DataSize a, long b)
    {
        return DataSize.succinctBytes(a.toBytes() + b);
    }

    private static double diffDuration(Duration a, Duration b)
    {
        return (a.getValue(TimeUnit.MICROSECONDS) - b.getValue(TimeUnit.MICROSECONDS));
    }

    private static long diffDataSize(DataSize a, DataSize b)
    {
        return (a.toBytes() - b.toBytes());
    }

    @Override
    public void updateStats(Duration totalCpu, DataSize totalMem, DataSize totalIo)
    {
        queryResources.cpuTime = totalCpu;
        queryResources.memCurrent = totalMem;
        queryResources.ioCurrent = totalIo;

        checkResourceLimits();
    }

    private QueryExecutionModifier checkResourceLimits()
    {
        QueryExecutionModifier modifier = NO_OP;
        /* Inform the resource usage update to the ResourceManagerService
        * for broader resource planning. */
        resourceUpdateListener.resourceUpdate(queryId, queryResources, nodeResourceStats);

        if (hardCpuLimit.isPresent() && queryResources.cpuTime.compareTo(hardCpuLimit.get()) > 0) {
            logger.warn("Query [%s] exceeds assigned Hard CPU-Time of %s, current Cpu: %s",
                    queryId.toString(), hardCpuLimit.get().toString(), queryResources.cpuTime.toString());
            return KILL_QUERY;
        }

        if (softCpuLimit.isPresent() && queryResources.cpuTime.compareTo(softCpuLimit.get()) > 0) {
            logger.warn("Query [%s] exceeds assigned CPU-Time of %s, current Cpu: %s",
                    queryId.toString(), softCpuLimit.get().toString(), queryResources.cpuTime.toString());
            isSuspendSuggested = true;
            return SUSPEND_QUERY;
        }
        else if (isSuspendSuggested) {
            return RESUME_QUERY;
        }

        if (softMemoryLimit.isPresent() && queryResources.revocableMem.compareTo(softMemoryLimit.get()) > 0) {
            logger.warn("Query [%s] exceeds assigned Memory limit of %s, current revocable memory used: %s",
                    queryId.toString(), softMemoryLimit.get().toString(), queryResources.revocableMem.toString());
            return SPILL_REVOCABLE;
        }

        if (softMemoryLimit.isPresent() && queryResources.memCurrent.compareTo(softMemoryLimit.get()) > 0) {
            logger.warn("Query [%s] exceeds assigned Memory limit of %s, current memory used: %s",
                    queryId.toString(), softMemoryLimit.get().toString(), queryResources.memCurrent.toString());
            return THROTTLE_SPLITS;
        }

        if (diskUseLimit.isPresent() && queryResources.ioCurrent.compareTo(diskUseLimit.get()) > 0) {
            logger.warn("Query [%s] exceeds assigned disk use limit of %s, current disk used: %s",
                    queryId.toString(), diskUseLimit.get().toString(), queryResources.ioCurrent.toString());
        }

        return modifier;
    }
}
