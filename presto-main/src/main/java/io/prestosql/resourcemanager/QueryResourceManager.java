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

import com.google.common.util.concurrent.AtomicDouble;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.StageStats;
import io.prestosql.metadata.InternalNode;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.KillPolicy;
import io.prestosql.spi.resourcegroups.ResourceGroup;
import io.prestosql.spi.resourcegroups.SchedulingPolicy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class QueryResourceManager
        implements ResourceManager
{
    private static final Logger logger = Logger.get(QueryResourceManager.class);
    private static final double HARD_LIMIT_FACTOR = 1.5;

    private final QueryId queryId;
    private Optional<DataSize> softMemoryLimit;
    private final Optional<Double> softMemoryLimitFraction;
    private final Optional<DataSize> softReservedMemory;
    private final Optional<Double> softReservedFraction;
    private final Optional<Integer> softConcurrencyLimit;
    private final Optional<Integer> hardReservedConcurrency;
    private final int hardConcurrencyLimit;
    private final Optional<SchedulingPolicy> schedulingPolicy;
    private final Optional<Integer> schedulingWeight;
    private final ResourceGroup resourceGroup;
    private Optional<Duration> softCpuLimit;
    private Optional<Duration> hardCpuLimit;
    private Optional<DataSize> diskUseLimit = Optional.empty();

    private final Optional<KillPolicy> killPolicy;

    private final QueryResourceManagerService.ResourceUpdateListener resourceUpdateListener;
    private final BasicResourceStats queryResources = new BasicResourceStats();
    private Map<InternalNode, BasicResourceStats> nodeWiseQueryStats = new HashMap<>();

    public QueryResourceManager(QueryId queryId, Session session, QueryResourceManagerService.ResourceUpdateListener resourceUpdateListener)
    {
        this.queryId = queryId;
        this.softMemoryLimit = Optional.empty();
        this.softMemoryLimitFraction = Optional.empty();
        this.softReservedMemory = Optional.empty();
        this.softReservedFraction = Optional.empty();
        this.softConcurrencyLimit = Optional.empty();
        this.hardReservedConcurrency = Optional.empty();
        this.hardConcurrencyLimit = 10;
        this.schedulingPolicy = Optional.empty();
        this.schedulingWeight = Optional.empty();
        this.resourceGroup = null;
        this.softCpuLimit = Optional.empty();
        this.hardCpuLimit = Optional.empty();
        this.killPolicy = Optional.empty();
        this.resourceUpdateListener = resourceUpdateListener;
    }

    public QueryResourceManager(QueryId queryId, Optional<DataSize> softMemoryLimit, Optional<Double> softMemoryLimitFraction,
                                Optional<DataSize> softReservedMemory, Optional<Double> softReservedFraction,
                                int maxQueued, Optional<Integer> softConcurrencyLimit, Optional<Integer> hardReservedConcurrency,
                                int hardConcurrencyLimit, Optional<SchedulingPolicy> schedulingPolicy,
                                Optional<Integer> schedulingWeight, ResourceGroup resourceGroup, Optional<Duration> softCpuLimit,
                                Optional<Duration> hardCpuLimit, Optional<KillPolicy> killPolicy,
                                QueryResourceManagerService.ResourceUpdateListener resourceUpdateListener)
    {
        this.queryId = queryId;
        this.softMemoryLimit = softMemoryLimit;
        this.softMemoryLimitFraction = softMemoryLimitFraction;
        this.softReservedMemory = softReservedMemory;
        this.softReservedFraction = softReservedFraction;
        this.softConcurrencyLimit = softConcurrencyLimit;
        this.hardReservedConcurrency = hardReservedConcurrency;
        this.hardConcurrencyLimit = hardConcurrencyLimit;
        this.schedulingPolicy = schedulingPolicy;
        this.schedulingWeight = schedulingWeight;
        this.resourceGroup = resourceGroup;
        this.softCpuLimit = softCpuLimit;
        this.hardCpuLimit = hardCpuLimit;
        this.killPolicy = killPolicy;
        this.resourceUpdateListener = resourceUpdateListener;
    }

    @Override
    public void setResourceLimit(DataSize memoryLimit, Duration cpuLimit, DataSize ioLimit)
    {
        /* Todo(Nitin K) check estimates correctness before updating limits! */
        softCpuLimit = Optional.of(cpuLimit);
        hardCpuLimit = Optional.of(new Duration(cpuLimit.getValue(TimeUnit.SECONDS) * HARD_LIMIT_FACTOR, TimeUnit.SECONDS));

        softMemoryLimit = Optional.of(memoryLimit);
        diskUseLimit = Optional.of(ioLimit);
    }

    @Override
    public DataSize getMemoryLimit()
    {
        return softMemoryLimit.orElse(DataSize.succinctBytes(0));
    }

    @Override
    public void updateStats(List<StageStats> stats)
    {
        /*Todo(Nitin K): record worker node level resource as well! */
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
    public void updateStats(Duration totalCpu, DataSize totalMem, DataSize totalIo)
    {
        queryResources.cpuTime = totalCpu;
        queryResources.memCurrent = totalMem;
        queryResources.ioCurrent = totalIo;

        checkResourceLimits();
    }

    private void checkResourceLimits()
    {
        /* Inform the resource usage update to the ResourceManagerService
        * for broader resource planning. */
        resourceUpdateListener.resourceUpdate(queryId, queryResources);

        if (hardCpuLimit.isPresent() && queryResources.cpuTime.compareTo(hardCpuLimit.get()) > 0) {
            logger.warn("Query [%s] exceeds assigned Hard CPU-Time of %s, current Cpu: %s",
                    queryId.toString(), hardCpuLimit.get().toString(), queryResources.cpuTime.toString());
        }

        if (softCpuLimit.isPresent() && queryResources.cpuTime.compareTo(softCpuLimit.get()) > 0) {
            logger.warn("Query [%s] exceeds assigned CPU-Time of %s, current Cpu: %s",
                    queryId.toString(), softCpuLimit.get().toString(), queryResources.cpuTime.toString());
        }

        if (softMemoryLimit.isPresent() && queryResources.memCurrent.compareTo(softMemoryLimit.get()) > 0) {
            logger.warn("Query [%s] exceeds assigned Memory limit of %s, current memory used: %s",
                    queryId.toString(), softMemoryLimit.get().toString(), queryResources.memCurrent.toString());
        }

        if (diskUseLimit.isPresent() && queryResources.ioCurrent.compareTo(diskUseLimit.get()) > 0) {
            logger.warn("Query [%s] exceeds assigned disk use limit of %s, current disk used: %s",
                    queryId.toString(), diskUseLimit.get().toString(), queryResources.ioCurrent.toString());
        }
    }
}
