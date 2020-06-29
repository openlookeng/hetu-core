/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.execution.resourcegroups;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.ManagedQueryExecution;
import io.prestosql.execution.QueryState;
import io.prestosql.server.QueryStateInfo;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.SchedulingPolicy;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.statestore.SharedQueryState;
import io.prestosql.statestore.SharedResourceGroupState;
import io.prestosql.statestore.StateCacheStore;
import io.prestosql.statestore.StateStoreConstants;
import io.prestosql.utils.DistributedResourceGroupUtils;
import org.joda.time.DateTime;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.server.QueryStateInfo.createQueryStateInfo;
import static java.util.Objects.requireNonNull;

/**
 * Resource groups form a tree, and all access to a group is guarded by the root of the tree.
 * Queries are submitted to leaf groups. Never to intermediate groups. Intermediate groups
 * aggregate resource consumption from their children, and may have their own limitations that
 * are enforced.
 * <p>
 * Distributed resource group calculates group resource usage using query states from external
 * state store and check if query can run or can queue using calculated resource usage
 *
 * @since 2019-11-29
 */
@ThreadSafe
public class DistributedResourceGroup
        extends BaseResourceGroup
{
    private static final long MILLISECONDS_PER_SECOND = 1000L;

    private static final Logger LOG = Logger.get(DistributedResourceGroup.class);

    // Live data structures
    // ====================
    @GuardedBy("root")
    private Queue<ManagedQueryExecution> queuedQueries = new LinkedList<>();
    @GuardedBy("root")
    private final Set<ManagedQueryExecution> runningQueries = new HashSet<>();
    @GuardedBy("root")
    private int descendantRunningQueries;
    @GuardedBy("root")
    private int descendantQueuedQueries;
    // Memory usage is cached because it changes very rapidly while queries are running, and would be expensive to track continuously
    @GuardedBy("root")
    private long cachedMemoryUsageBytes;
    @GuardedBy("root")
    private long cpuUsageMillis;
    @GuardedBy("root")
    private long lastStartMillis;
    @GuardedBy("root")
    private final CounterStat timeBetweenStartsSec = new CounterStat();
    // Internal time to track last time refreshing stats from state store
    private DateTime lastUpdateTime = new DateTime();
    // Last query execution time in current or all the children groups
    private Optional<DateTime> lastExecutionTime = Optional.empty();
    // State Store
    private StateStore stateStore;

    protected DistributedResourceGroup(Optional<BaseResourceGroup> parent,
            String name,
            BiConsumer<BaseResourceGroup, Boolean> jmxExportListener,
            Executor executor,
            StateStore stateStore)
    {
        super(parent, name, jmxExportListener, executor);
        this.stateStore = requireNonNull(stateStore, "state store is null");
    }

    @Override
    protected List<QueryStateInfo> getAggregatedRunningQueriesInfo()
    {
        synchronized (root) {
            if (subGroups.isEmpty()) {
                Optional<SharedResourceGroupState> resourceGroupState = getSharedResourceGroupState();
                if (!resourceGroupState.isPresent()) {
                    return ImmutableList.of();
                }
                else {
                    return resourceGroupState.get().getRunningQueries().stream()
                            .map(SharedQueryState::getBasicQueryInfo)
                            .map(queryInfo -> createQueryStateInfo(queryInfo, Optional.of(id)))
                            .collect(toImmutableList());
                }
            }

            return subGroups.values().stream()
                    .map(BaseResourceGroup::getAggregatedRunningQueriesInfo)
                    .flatMap(List::stream)
                    .collect(toImmutableList());
        }
    }

    @Managed
    @Override
    public int getRunningQueries()
    {
        synchronized (root) {
            Optional<SharedResourceGroupState> resourceGroupState = getSharedResourceGroupState();
            int numRunningQueries = resourceGroupState.isPresent() ? resourceGroupState.get().getRunningQueries().size() : 0;
            return numRunningQueries + descendantRunningQueries;
        }
    }

    @Managed
    @Override
    public int getQueuedQueries()
    {
        synchronized (root) {
            Optional<SharedResourceGroupState> resourceGroupState = getSharedResourceGroupState();
            int numQueuedQueries = resourceGroupState.isPresent() ? resourceGroupState.get().getQueuedQueries().size() : 0;
            return numQueuedQueries + descendantQueuedQueries;
        }
    }

    @Override
    public void setSoftMemoryLimit(DataSize limit)
    {
        synchronized (root) {
            this.softMemoryLimitBytes = limit.toBytes();
        }
    }

    @Override
    public void setSoftCpuLimit(Duration limit)
    {
        synchronized (root) {
            if (limit.toMillis() > hardCpuLimitMillis) {
                setHardCpuLimit(limit);
            }
            this.softCpuLimitMillis = limit.toMillis();
        }
    }

    @Override
    public void setHardCpuLimit(Duration limit)
    {
        synchronized (root) {
            if (limit.toMillis() < softCpuLimitMillis) {
                setSoftCpuLimit(limit);
            }
            this.hardCpuLimitMillis = limit.toMillis();
        }
    }

    @Override
    public void setSoftConcurrencyLimit(int softConcurrencyLimit)
    {
        checkArgument(softConcurrencyLimit >= 0, "softConcurrencyLimit is negative");
        synchronized (root) {
            this.softConcurrencyLimit = softConcurrencyLimit;
        }
    }

    @Override
    public void setHardReservedConcurrency(int hardReservedConcurrency)
    {
        checkArgument(hardReservedConcurrency >= 0, "hardReservedConcurrency is negative");
        synchronized (root) {
            this.hardReservedConcurrency = hardReservedConcurrency;
        }
    }

    @Override
    public void setSoftReservedMemory(DataSize softReservedMemory)
    {
        synchronized (root) {
            this.softReservedMemory = softReservedMemory.toBytes();
        }
    }

    @Managed
    @Override
    public void setHardConcurrencyLimit(int hardConcurrencyLimit)
    {
        checkArgument(hardConcurrencyLimit >= 0, "hardConcurrencyLimit is negative");
        synchronized (root) {
            this.hardConcurrencyLimit = hardConcurrencyLimit;
        }
    }

    @Override
    public void setSchedulingWeight(int weight)
    {
        checkArgument(weight > 0, "weight must be positive");
        synchronized (root) {
            this.schedulingWeight = weight;
        }
    }

    @Override
    public void setSchedulingPolicy(SchedulingPolicy policy)
    {
        synchronized (root) {
            if (policy == schedulingPolicy) {
                return;
            }

            // Only Fair scheduling policy is supported for distributed resource group
            switch (policy) {
                case FAIR:
                    break;
                case WEIGHTED:
                case WEIGHTED_FAIR:
                case QUERY_PRIORITY:
                default:
                    throw new UnsupportedOperationException("Unsupported scheduling policy: " + policy);
            }
            schedulingPolicy = policy;
        }
    }

    @Override
    public DistributedResourceGroup getOrCreateSubGroup(String name)
    {
        requireNonNull(name, "name is null");
        synchronized (root) {
            checkArgument(runningQueries.isEmpty() && queuedQueries.isEmpty(), "Cannot add sub group to %s while queries are running", id);
            if (subGroups.containsKey(name)) {
                return (DistributedResourceGroup) subGroups.get(name);
            }
            DistributedResourceGroup subGroup = new DistributedResourceGroup(Optional.of(this), name, jmxExportListener, executor, stateStore);
            subGroups.put(name, subGroup);
            return subGroup;
        }
    }

    public Optional<DateTime> getLastExecutionTime()
    {
        return lastExecutionTime;
    }

    /**
     * Check availability and try to run the query in current resource group
     *
     * @param query Query execution contains query states
     */
    @Override
    public void run(ManagedQueryExecution query)
    {
        synchronized (root) {
            root.internalRefreshStats();
            super.run(query);
        }
    }

    @Override
    protected void enqueueQuery(ManagedQueryExecution query)
    {
        checkState(Thread.holdsLock(root), "Must hold lock to enqueue a query");
        synchronized (root) {
            queuedQueries.add(query);
        }
    }

    @Override
    protected void startInBackground(ManagedQueryExecution query)
    {
        checkState(Thread.holdsLock(root), "Must hold lock to start a query");
        synchronized (root) {
            executor.execute(query::startWaitingForResources);
            while (query.getBasicQueryInfo().getState() == QueryState.QUEUED) {
                // wait for query to be started
            }
            runningQueries.add(query);
        }
    }

    @Override
    protected void queryFinished(ManagedQueryExecution query)
    {
        synchronized (root) {
            if (!runningQueries.contains(query) && !queuedQueries.contains(query)) {
                // Query has already been cleaned up
                return;
            }

            if (runningQueries.contains(query)) {
                runningQueries.remove(query);
            }
            else {
                queuedQueries.remove(query);
            }
        }
    }

    /**
     * Update internal stats for all the children groups
     * Stats are calculated based on cached resource group states from state store
     * Stats include: running queries, queued queries, memory usage, cpu usage
     */
    @Override
    protected void internalRefreshStats()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to refresh stats");
        synchronized (root) {
            if (subGroups.isEmpty()) {
                descendantRunningQueries = 0;
                descendantQueuedQueries = 0;
                cachedMemoryUsageBytes = 0;

                Optional<SharedResourceGroupState> resourceGroupState = getSharedResourceGroupState();
                if (!resourceGroupState.isPresent()) {
                    return;
                }

                resourceGroupState.ifPresent(state -> this.lastExecutionTime = state.getLastExecutionTime());
                Set<SharedQueryState> runningQueries = resourceGroupState.get().getRunningQueries();
                for (SharedQueryState state : runningQueries) {
                    cachedMemoryUsageBytes += state.getUserMemoryReservation().toBytes();
                }

                // get cpuUsageMillis from resourceGroupState
                cpuUsageMillis = resourceGroupState.get().getCpuUsageMillis();
            }
            else {
                int tempDescendantRunningQueries = 0;
                int tempDescendantQueuedQueries = 0;
                long tempCachedMemoryUsageBytes = 0L;
                long tempCpuUsageMillis = 0L;

                for (BaseResourceGroup group : subGroups()) {
                    group.internalRefreshStats();
                    tempCpuUsageMillis += ((DistributedResourceGroup) group).cpuUsageMillis;
                }

                // Sub-groups are created on demand, so need to also check stats in sub-groups created on other coordinators
                for (SharedResourceGroupState state : getSharedSubGroups()) {
                    tempDescendantRunningQueries += state.getRunningQueries().size();
                    tempDescendantQueuedQueries += state.getQueuedQueries().size();
                    tempCachedMemoryUsageBytes += state.getRunningQueries().stream()
                            .mapToLong(query -> query.getUserMemoryReservation().toBytes())
                            .reduce(0, (memoryUsage1, memoryUsage2) -> memoryUsage1 + memoryUsage2);
                }

                descendantRunningQueries = tempDescendantRunningQueries;
                descendantQueuedQueries = tempDescendantQueuedQueries;
                cachedMemoryUsageBytes = tempCachedMemoryUsageBytes;
                cpuUsageMillis = tempCpuUsageMillis;
            }
            lastUpdateTime = new DateTime();
        }
    }

    /**
     * Check if there is any queued query in any eligible sub-group that can be started
     *
     * @return if any query has been started in sub-groups
     */
    protected boolean internalStartNext()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to find next query");
        synchronized (root) {
            if (!canRunMore()) {
                return false;
            }

            // Only start the query if it exists locally
            Optional<SharedResourceGroupState> resourceGroupState = getSharedResourceGroupState();
            PriorityQueue<SharedQueryState> globalQueuedQueries = resourceGroupState.isPresent() ? resourceGroupState.get().getQueuedQueries() : new PriorityQueue<>();
            if (!globalQueuedQueries.isEmpty() && !queuedQueries.isEmpty()) {
                // Get queued query with longest queued time from state cache store.
                // Remove it if local queued queries contains it.
                SharedQueryState nextQuery = globalQueuedQueries.peek();
                for (ManagedQueryExecution localQuery : queuedQueries) {
                    if (nextQuery.getBasicQueryInfo().getQueryId().equals(localQuery.getBasicQueryInfo().getQueryId())) {
                        Lock lock = stateStore.getLock(id.toString());
                        boolean locked = false;
                        try {
                            locked = lock.tryLock(MILLISECONDS_PER_SECOND, TimeUnit.MILLISECONDS);
                            if (locked) {
                                // Get the most recent cached state store status and check canRunMore again
                                // Avoid the race condition that state store is updated by other process
                                // Make sure queued query start is synchronized
                                DistributedResourceGroupUtils.mapCachedStates();
                                if (canRunMore()) {
                                    queuedQueries.remove(localQuery);
                                    startInBackground(localQuery);
                                    return true;
                                }
                            }
                            return false;
                        }
                        catch (InterruptedException | RuntimeException e) {
                            return false;
                        }
                        finally {
                            if (locked) {
                                lock.unlock();
                            }
                        }
                    }
                }
            }

            // Try to find least recently used eligible group
            DistributedResourceGroup chosenGroup = findLeastRecentlyExecutedSubgroup();
            if (chosenGroup == null) {
                return false;
            }

            // Try to start queued query in the group
            boolean started = chosenGroup.internalStartNext();

            long currentTime = System.currentTimeMillis();
            if (lastStartMillis != 0) {
                timeBetweenStartsSec.update(Math.max(0, (currentTime - lastStartMillis) / MILLISECONDS_PER_SECOND));
            }
            lastStartMillis = currentTime;

            return started;
        }
    }

    @Override
    protected boolean canQueueMore()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            return getQueuedQueries() < maxQueuedQueries;
        }
    }

    @Override
    protected boolean canRunMore()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        Optional<SharedResourceGroupState> resourceGroupState = getSharedResourceGroupState();
        int numRunningQueries = resourceGroupState.isPresent() ? resourceGroupState.get().getRunningQueries().size() : 0;
        synchronized (root) {
            if (cpuUsageMillis >= hardCpuLimitMillis) {
                return false;
            }

            int hardConcurrencyLimit = this.hardConcurrencyLimit;
            return hasCapacity(numRunningQueries, adjustHardConcurrency(hardConcurrencyLimit, cpuUsageMillis));
        }
    }

    private boolean hasCapacity(int numRunningQueries, int hardConcurrencyLimit)
    {
        if (numRunningQueries + descendantRunningQueries >= hardConcurrencyLimit ||
                cachedMemoryUsageBytes > softMemoryLimitBytes) {
            return false;
        }
        if (parent.isPresent()) {
            // Check if hardConcurrencyLimit of the parent is reached with reserved concurrency for each peer group
            if (numRunningQueries + descendantRunningQueries >= hardReservedConcurrency) {
                int peerTotalQuerySize = 0;
                for (DistributedResourceGroup group : (Collection<DistributedResourceGroup>) parent.get().subGroups()) {
                    Optional<SharedResourceGroupState> peerGroupState = group.getSharedResourceGroupState();
                    int peerRunningQueries = peerGroupState.isPresent() ? peerGroupState.get().getRunningQueries().size() : 0;
                    peerTotalQuerySize += Math.max(peerRunningQueries + group.descendantQueuedQueries, group.hardReservedConcurrency);
                }
                if (parent.get().hardConcurrencyLimit <= peerTotalQuerySize) {
                    return false;
                }
            }
            // Check if the softMemoryLimit of parent is reached with reserved memory for each peer group
            if (cachedMemoryUsageBytes >= softReservedMemory) {
                long peerGroupTotalUsage = 0L;
                for (DistributedResourceGroup group : (Collection<DistributedResourceGroup>) parent.get().subGroups()) {
                    Optional<SharedResourceGroupState> peerGroupState = group.getSharedResourceGroupState();
                    long peerGroupUsage = peerGroupState.isPresent() ? peerGroupState.get().getRunningQueries().stream()
                            .mapToLong(query -> query.getUserMemoryReservation().toBytes())
                            .reduce(0, (memoryUsage1, memoryUsage2) -> memoryUsage1 + memoryUsage2)
                            : group.cachedMemoryUsageBytes;
                    peerGroupTotalUsage += Math.max(peerGroupUsage, group.softReservedMemory);
                }
                if (parent.get().softMemoryLimitBytes <= peerGroupTotalUsage) {
                    LOG.debug("No capacity to run more queries in the resource group: %s, with following reasons: \n" +
                                    "cachedMemoryUsageBytes:%s >= softReservedMemory:%s and \n" +
                                    "softMemoryLimitBytes:%s <= peerGroupTotalUsage:%s",
                            parent.get().id, cachedMemoryUsageBytes, softReservedMemory, softMemoryLimitBytes, peerGroupTotalUsage);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Return subgroups of current resource group
     *
     * @return collection contains all the subgroups
     */
    @Override
    public Collection<BaseResourceGroup> subGroups()
    {
        synchronized (root) {
            return subGroups.values();
        }
    }

    /**
     * Get cached global resource group state from state store  for current resource group
     *
     * @return SharedResourceGroupState if exists in state store
     */
    private Optional<SharedResourceGroupState> getSharedResourceGroupState()
    {
        Map<ResourceGroupId, SharedResourceGroupState> cachedStates = StateCacheStore.get().getCachedStates(StateStoreConstants.RESOURCE_GROUP_STATE_COLLECTION_NAME);
        if (cachedStates == null || !cachedStates.containsKey(id)) {
            return Optional.empty();
        }
        return Optional.ofNullable(cachedStates.get(id));
    }

    /**
     * Get all sub groups belong to current group based on cached resource group states
     *
     * @return List of SharedResourceGroupState
     */
    private List<SharedResourceGroupState> getSharedSubGroups()
    {
        Map<ResourceGroupId, SharedResourceGroupState> cachedStates = StateCacheStore.get().getCachedStates(StateStoreConstants.RESOURCE_GROUP_STATE_COLLECTION_NAME);
        if (cachedStates == null) {
            return ImmutableList.of();
        }
        return cachedStates.values().stream().filter(state -> id.isAncestorOf(state.getId())).collect(Collectors.toList());
    }

    /**
     * Find the sub group that's least recently used that has queued queries and can run more
     *
     * @return The chosen group or null if no eligible group
     */
    private DistributedResourceGroup findLeastRecentlyExecutedSubgroup()
    {
        List<DistributedResourceGroup> eligibleGroups = subGroups().stream()
                .filter(group -> group.getQueuedQueries() > 0 && group.canRunMore())
                .sorted(Comparator.comparing(group -> group.getId().toString()))
                .map(DistributedResourceGroup.class::cast)
                .collect(Collectors.toList());

        DateTime leastRecentlyExecutionTime = null;
        DistributedResourceGroup chosenGroup = null;
        for (DistributedResourceGroup group : eligibleGroups) {
            // If the group has never executed any query just use it
            if (!group.getLastExecutionTime().isPresent()) {
                return group;
            }

            if (leastRecentlyExecutionTime == null || group.getLastExecutionTime().get().isBefore(leastRecentlyExecutionTime)) {
                leastRecentlyExecutionTime = group.getLastExecutionTime().get();
                chosenGroup = group;
            }
        }

        return chosenGroup;
    }

    /**
     * Periodically check and start queries in queue
     */
    @Override
    public synchronized void processQueuedQueries()
    {
        internalRefreshStats();
        while (internalStartNext()) {
            // start all the queries we can
        }
    }

    @Override
    public void generateCpuQuota(long elapsedSeconds)
    {
        return;
    }
}
