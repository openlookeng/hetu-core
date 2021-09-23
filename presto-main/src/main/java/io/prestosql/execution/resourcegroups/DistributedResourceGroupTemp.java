/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.ManagedQueryExecution;
import io.prestosql.execution.QueryState;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.server.QueryStateInfo;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.resourcegroups.KillPolicy;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.SchedulingPolicy;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
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

import java.util.ArrayList;
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
import static com.google.common.math.LongMath.saturatedAdd;
import static com.google.common.math.LongMath.saturatedMultiply;
import static com.google.common.math.LongMath.saturatedSubtract;
import static io.prestosql.server.QueryStateInfo.createQueryStateInfo;
import static io.prestosql.spi.ErrorType.USER_ERROR;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
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
 * <p>
 * TODO: After this class is tested(with HA aggregated stats), it will be renamed to 'DistributedResourceGroup'
 */

@ThreadSafe
public class DistributedResourceGroupTemp
        extends BaseResourceGroup
{
    private static final long MILLISECONDS_PER_SECOND = 1000L;

    private static final Logger LOG = Logger.get(DistributedResourceGroupTemp.class);

    // Live data structures
    // ====================
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

    private InternalNodeManager internalNodeManager;
    private static final ObjectMapper MAPPER = new ObjectMapperProvider().get();
    private static final String DASH = "-";
    private static final String RESOURCE_AGGR_STATS = "resourceaggrstats";

    // Local variables represent value in the current coordinator
    @GuardedBy("root")
    private Queue<ManagedQueryExecution> localQueuedQueries = new LinkedList<>();
    @GuardedBy("root")
    private final Set<ManagedQueryExecution> localRunningQueries = new HashSet<>();
    @GuardedBy("root")
    private int localDescendantRunningQueries;
    @GuardedBy("root")
    private int localDescendantQueuedQueries;
    // Memory usage is cached because it changes very rapidly while queries are running, and would be expensive to track continuously
    @GuardedBy("root")
    private long localCachedMemoryUsageBytes;
    @GuardedBy("root")
    private long localCpuUsageMillis;

    // Global variables represent aggregated values among all the coordinators
    @GuardedBy("root")
    private int globalTotalQueuedQueries;
    @GuardedBy("root")
    private int globalTotalRunningQueries;
    @GuardedBy("root")
    private int globalDescendantRunningQueries;
    @GuardedBy("root")
    private int globalDescendantQueuedQueries;
    // Memory usage is cached because it changes very rapidly while queries are running, and would be expensive to track continuously
    @GuardedBy("root")
    private long globalCachedMemoryUsageBytes;
    @GuardedBy("root")
    private long globalCpuUsageMillis;

    protected DistributedResourceGroupTemp(Optional<BaseResourceGroup> parent,
            String name,
            BiConsumer<BaseResourceGroup, Boolean> jmxExportListener,
            Executor executor,
            StateStore stateStore,
            InternalNodeManager internalNodeManager)
    {
        super(parent, name, jmxExportListener, executor);
        this.stateStore = requireNonNull(stateStore, "state store is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
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
            return getTotalGlobalRunningQueries() + getGlobalDescendantRunningQueries();
        }
    }

    @Managed
    @Override
    public int getQueuedQueries()
    {
        synchronized (root) {
            refreshGlobalValues();
            return globalTotalQueuedQueries + globalDescendantQueuedQueries;
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
    public void setKillPolicy(KillPolicy killPolicy)
    {
        synchronized (root) {
            this.killPolicy = killPolicy;
        }
    }

    @Override
    public DistributedResourceGroupTemp getOrCreateSubGroup(String name)
    {
        requireNonNull(name, "name is null");
        synchronized (root) {
            checkArgument(localRunningQueries.isEmpty() && localQueuedQueries.isEmpty(), "Cannot add sub group to %s while queries are running", id);
            if (subGroups.containsKey(name)) {
                return (DistributedResourceGroupTemp) subGroups.get(name);
            }
            DistributedResourceGroupTemp subGroup = new DistributedResourceGroupTemp(Optional.of(this), name, jmxExportListener, executor, stateStore, internalNodeManager);
            subGroup.setMemoryMarginPercent(memoryMarginPercent);
            subGroup.setQueryProgressMarginPercent(queryProgressMarginPercent);
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
            localQueuedQueries.add(query);
            updateLocalValuesToStateStore();
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
            localQueuedQueries.remove(query);
            localRunningQueries.add(query);
            updateLocalValuesToStateStore();
        }
    }

    @Override
    protected void queryFinished(ManagedQueryExecution query)
    {
        synchronized (root) {
            if (!localRunningQueries.contains(query) && !localQueuedQueries.contains(query)) {
                // Query has already been cleaned up
                return;
            }

            // Only count the CPU time if the query succeeded, or the failure was the fault of the user
            if (!query.getErrorCode().isPresent() || query.getErrorCode().get().getType() == USER_ERROR) {
                DistributedResourceGroupTemp group = this;
                while (group != null) {
                    group.localCpuUsageMillis = saturatedAdd(group.localCpuUsageMillis, query.getTotalCpuTime().toMillis());
                    group = (DistributedResourceGroupTemp) group.parent.orElse(null);
                }
            }

            if (localRunningQueries.contains(query)) {
                localRunningQueries.remove(query);
            }
            else {
                localQueuedQueries.remove(query);
            }
            updateLocalValuesToStateStore();
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
                localDescendantRunningQueries = 0;
                localDescendantQueuedQueries = 0;
                localCachedMemoryUsageBytes = 0;

                for (ManagedQueryExecution query : localRunningQueries) {
                    localCachedMemoryUsageBytes += query.getTotalMemoryReservation().toBytes();
                }
            }
            else {
                int tempLocalDescendantRunningQueries = 0;
                int tempLocalDescendantQueuedQueries = 0;
                long tempLocalCachedMemoryUsageBytes = 0L;
                long tempLocalCpuUsageMillis = 0L;

                for (BaseResourceGroup group : subGroups()) {
                    group.internalRefreshStats();
                    tempLocalCpuUsageMillis += ((DistributedResourceGroupTemp) group).localCpuUsageMillis;
                    tempLocalDescendantRunningQueries += ((DistributedResourceGroupTemp) group).localDescendantRunningQueries;
                    tempLocalDescendantQueuedQueries += ((DistributedResourceGroupTemp) group).localDescendantQueuedQueries;
                    tempLocalCachedMemoryUsageBytes += ((DistributedResourceGroupTemp) group).localCachedMemoryUsageBytes;
                }

                localDescendantRunningQueries = tempLocalDescendantRunningQueries;
                localDescendantQueuedQueries = tempLocalDescendantQueuedQueries;
                localCachedMemoryUsageBytes = tempLocalCachedMemoryUsageBytes;
                localCpuUsageMillis = tempLocalCpuUsageMillis;
            }
            lastUpdateTime = new DateTime();
            updateLocalValuesToStateStore();
        }
    }

    public void internalCancelQuery()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to check cancel query");
        synchronized (root) {
            if (!subGroups.isEmpty()) {
                for (BaseResourceGroup group : subGroups()) {
                    ((DistributedResourceGroupTemp) group).internalCancelQuery();
                }

                return;
            }

            long globalCachedMemoryUsageBytes = getGlobalCachedMemoryUsageBytes();
            if (globalCachedMemoryUsageBytes <= softMemoryLimitBytes) {
                return;
            }

            Optional<SharedResourceGroupState> resourceGroupState = getSharedResourceGroupState();
            if (!resourceGroupState.isPresent()) {
                return;
            }

            Set<SharedQueryState> globalRunningQueries = resourceGroupState.get().getRunningQueries();
            List<SharedQueryState> sortedQueryList;

            Lock lock = stateStore.getLock(id.toString());
            boolean locked = false;
            try {
                // If lock is not free, then we return immediately, so no need to refresh after taking lock.
                // Before next call of this function, refresh will already happen.
                locked = lock.tryLock();
                if (locked) {
                    switch (killPolicy) {
                        case HIGH_MEMORY_QUERIES:
                            double absMemoryMargin = 1 - (double) memoryMarginPercent / 100;
                            double absQueryProgressMargin = 1 - (double) queryProgressMarginPercent / 100;

                            sortedQueryList = globalRunningQueries.stream().sorted((o1, o2) -> {
                                if (o1.getTotalMemoryReservation().toBytes() < o2.getTotalMemoryReservation().toBytes() * absMemoryMargin
                                        || o2.getTotalMemoryReservation().toBytes() < o1.getTotalMemoryReservation().toBytes() * absMemoryMargin) {
                                    return ((Long) o2.getTotalMemoryReservation().toBytes()).compareTo(o1.getTotalMemoryReservation().toBytes());
                                }

                                // if memory usage within 10%, then sort based on % of query completion.
                                // if query progress difference is within 5%, then order will be decided based on memory itself.
                                if (o1.getQueryProgress().orElse(0) < o2.getQueryProgress().orElse(0) * absQueryProgressMargin
                                        || o2.getQueryProgress().orElse(0) < o1.getQueryProgress().orElse(0) * absQueryProgressMargin) {
                                    return ((Double) o1.getQueryProgress().orElse(0)).compareTo((o2.getQueryProgress().orElse(0)));
                                }

                                return ((Long) o2.getTotalMemoryReservation().toBytes()).compareTo(o1.getTotalMemoryReservation().toBytes());
                            }).collect(Collectors.toList());
                            break;
                        case OLDEST_QUERIES:
                            sortedQueryList = globalRunningQueries.stream().sorted(Comparator.comparing(o -> (o.getExecutionStartTime().get()))).collect(Collectors.toList());
                            break;
                        case RECENT_QUERIES:
                            sortedQueryList = globalRunningQueries.stream().sorted(Comparator.comparing(o -> (o.getExecutionStartTime().get()), Comparator.reverseOrder())).collect(Collectors.toList());
                            break;
                        case FINISH_PERCENTAGE_QUERIES:
                            sortedQueryList = globalRunningQueries.stream().sorted(Comparator.comparing(o -> (o.getQueryProgress().orElse(0)))).collect(Collectors.toList());
                            break;
                        case NO_KILL:
                            //fall through
                        default:
                            sortedQueryList = new ArrayList<>();
                    }

                    long tempGlobalCachedMemoryUsage = globalCachedMemoryUsageBytes;
                    long tempLocalCachedMemoryUsage = localCachedMemoryUsageBytes;

                    // As per the kill policy, top queries are selected across all coordinators but we kill only local queries
                    // till memory reaches with-in required limit.
                    // E.g. Suppose Kill policy is HIGH_MEMORY_QUERIES and queries are ordered as below
                    //      Q1, Q10, Q7, Q20, Q25
                    //              where only Q1 and Q7 are local queries and only combined memory of Q1 and Q10 brings memory within the desired limit.
                    //              So in this case only Q1 will be killed from local coordinator.
                    for (SharedQueryState query : sortedQueryList) {
                        for (ManagedQueryExecution localQuery : localRunningQueries) {
                            if (query.getBasicQueryInfo().getQueryId().equals(localQuery.getBasicQueryInfo().getQueryId())) {
                                LOG.info("Query " + localQuery.getBasicQueryInfo().getQueryId() + " is getting killed for resource group " + this + " query will be killed with policy " + killPolicy);
                                localQuery.fail(new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Memory consumption " + tempLocalCachedMemoryUsage + " exceed the limit " + softMemoryLimitBytes + "for resource group " + this));
                                queryFinished(localQuery);
                                tempLocalCachedMemoryUsage -= query.getTotalMemoryReservation().toBytes();
                                break;
                            }
                        }

                        tempGlobalCachedMemoryUsage -= query.getTotalMemoryReservation().toBytes();
                        if (tempGlobalCachedMemoryUsage <= softMemoryLimitBytes) {
                            break;
                        }
                    }
                }
            }
            catch (RuntimeException e) {
                return;
            }
            finally {
                if (locked) {
                    lock.unlock();
                }
            }
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
            if (!globalQueuedQueries.isEmpty() && !localQueuedQueries.isEmpty()) {
                // Get queued query with longest queued time from state cache store.
                // Remove it if local queued queries contains it.
                SharedQueryState nextQuery = globalQueuedQueries.peek();
                for (ManagedQueryExecution localQuery : localQueuedQueries) {
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
            DistributedResourceGroupTemp chosenGroup = findLeastRecentlyExecutedSubgroup();
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
        synchronized (root) {
            refreshGlobalValues();
            if (globalCpuUsageMillis >= hardCpuLimitMillis) {
                return false;
            }
            int hardConcurrencyLimit = this.hardConcurrencyLimit;
            return hasCapacity(globalTotalRunningQueries, adjustHardConcurrency(hardConcurrencyLimit, globalCpuUsageMillis));
        }
    }

    private boolean hasCapacity(int numRunningQueries, int hardConcurrencyLimit)
    {
        if (numRunningQueries + globalDescendantRunningQueries >= hardConcurrencyLimit ||
                globalCachedMemoryUsageBytes > softMemoryLimitBytes) {
            return false;
        }
        if (parent.isPresent()) {
            // Check if hardConcurrencyLimit of the parent is reached with reserved concurrency for each peer group
            if (numRunningQueries + globalDescendantRunningQueries >= hardReservedConcurrency) {
                int peerTotalQuerySize = 0;
                for (DistributedResourceGroupTemp group : (Collection<DistributedResourceGroupTemp>) parent.get().subGroups()) {
                    group.refreshGlobalValues();
                    peerTotalQuerySize += Math.max(group.globalTotalRunningQueries + group.globalDescendantRunningQueries, group.hardReservedConcurrency);
                }
                if (parent.get().hardConcurrencyLimit <= peerTotalQuerySize) {
                    return false;
                }
            }
            // Check if the softMemoryLimit of parent is reached with reserved memory for each peer group
            if (globalCachedMemoryUsageBytes >= softReservedMemory) {
                long peerGroupTotalUsage = 0L;
                for (DistributedResourceGroupTemp group : (Collection<DistributedResourceGroupTemp>) parent.get().subGroups()) {
                    peerGroupTotalUsage += Math.max(group.globalCachedMemoryUsageBytes, group.softReservedMemory);
                }
                if (parent.get().softMemoryLimitBytes <= peerGroupTotalUsage) {
                    LOG.debug("No capacity to run more queries in the resource group: %s, with following reasons: \n" +
                                    "cachedMemoryUsageBytes:%s >= softReservedMemory:%s and \n" +
                                    "softMemoryLimitBytes:%s <= peerGroupTotalUsage:%s",
                            parent.get().id, globalCachedMemoryUsageBytes, softReservedMemory, softMemoryLimitBytes, peerGroupTotalUsage);
                    return false;
                }
            }
        }
        return true;
    }

    protected void internalGenerateCpuQuota(long elapsedSeconds)
    {
        checkState(Thread.holdsLock(root), "Must hold lock to generate cpu quota");
        synchronized (root) {
            // Bug fix in cases when the CPU usage is initially higher than the cpu quota, but after a while,
            // when the CPU usage is then lower than the CPU quota, queued queries are still not run. This fix was
            long newQuota = saturatedMultiply(elapsedSeconds, cpuQuotaGenerationMillisPerSecond);
            localCpuUsageMillis = saturatedSubtract(localCpuUsageMillis, newQuota);

            if (localCpuUsageMillis < 0 || localCpuUsageMillis == Long.MAX_VALUE) {
                localCpuUsageMillis = 0;
            }
            updateLocalValuesToStateStore();
            for (BaseResourceGroup group : subGroups.values()) {
                ((InternalResourceGroup) group).internalGenerateCpuQuota(elapsedSeconds);
            }
        }
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
     * Find the sub group that's least recently used that has queued queries and can run more
     *
     * @return The chosen group or null if no eligible group
     */
    private DistributedResourceGroupTemp findLeastRecentlyExecutedSubgroup()
    {
        List<DistributedResourceGroupTemp> eligibleGroups = subGroups().stream()
                .filter(group -> group.getQueuedQueries() > 0 && group.canRunMore())
                .sorted(Comparator.comparing(group -> group.getId().toString()))
                .map(DistributedResourceGroupTemp.class::cast)
                .collect(Collectors.toList());

        DateTime leastRecentlyExecutionTime = null;
        DistributedResourceGroupTemp chosenGroup = null;
        for (DistributedResourceGroupTemp group : eligibleGroups) {
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
        internalCancelQuery();
        while (internalStartNext()) {
            // start all the queries we can
        }
    }

    @Override
    public synchronized void generateCpuQuota(long elapsedSeconds)
    {
        if (elapsedSeconds > 0) {
            internalGenerateCpuQuota(elapsedSeconds);
        }
    }

    @Override
    public long getCachedMemoryUsageBytes()
    {
        return getGlobalCachedMemoryUsageBytes();
    }

    public int getGlobalDescendantRunningQueries()
    {
        synchronized (root) {
            refreshGlobalValues();
            return globalDescendantRunningQueries;
        }
    }

    public int getGlobalDescendantQueuedQueries()
    {
        synchronized (root) {
            refreshGlobalValues();
            return globalDescendantQueuedQueries;
        }
    }

    public long getGlobalCachedMemoryUsageBytes()
    {
        synchronized (root) {
            refreshGlobalValues();
            return globalCachedMemoryUsageBytes;
        }
    }

    public long getGlobalCpuUsageMillis()
    {
        synchronized (root) {
            refreshGlobalValues();
            return globalCpuUsageMillis;
        }
    }

    public int getTotalGlobalQueuedQueries()
    {
        synchronized (root) {
            refreshGlobalValues();
            return globalTotalQueuedQueries;
        }
    }

    public int getTotalGlobalRunningQueries()
    {
        synchronized (root) {
            refreshGlobalValues();
            return globalTotalRunningQueries;
        }
    }

    private String createCoordinatorCollectionName(InternalNode coordinator)
    {
        return coordinator.getHostAndPort() + DASH + RESOURCE_AGGR_STATS;
    }

    private void updateLocalValuesToStateStore()
    {
        synchronized (root) {
            try {
                StateMap<String, String> resourceGroupMap = ((StateMap) stateStore.getOrCreateStateCollection(createCoordinatorCollectionName(internalNodeManager.getCurrentNode()), StateCollection.Type.MAP));
                DistributedResourceGroupAggrStats groupAggrStats = new DistributedResourceGroupAggrStats(
                        getId(),
                        localRunningQueries.size(),
                        localQueuedQueries.size(),
                        localDescendantRunningQueries,
                        localDescendantQueuedQueries,
                        localCpuUsageMillis,
                        localCachedMemoryUsageBytes);
                String json = MAPPER.writeValueAsString(groupAggrStats);
                resourceGroupMap.put(getId().toString(), json);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(String.format("Error updating resource group state with group id = %s, caused by ObjectMapper: %s", id, e.getMessage()));
            }
        }
    }

    private void refreshGlobalValues()
    {
        synchronized (root) {
            globalTotalRunningQueries = localRunningQueries.size();
            globalTotalQueuedQueries = localQueuedQueries.size();
            globalDescendantQueuedQueries = localDescendantQueuedQueries;
            globalDescendantRunningQueries = localDescendantRunningQueries;
            globalCachedMemoryUsageBytes = localCachedMemoryUsageBytes;
            globalCpuUsageMillis = localCpuUsageMillis;

            internalNodeManager.refreshNodes();
            try {
                for (InternalNode coordinator : internalNodeManager.getCoordinators()) {
                    if (coordinator.equals(internalNodeManager.getCurrentNode())) {
                        continue;
                    }
                    StateMap<String, String> resourceGroupMap = ((StateMap) stateStore.getOrCreateStateCollection(createCoordinatorCollectionName(coordinator), StateCollection.Type.MAP));
                    DistributedResourceGroupAggrStats groupAggrStats = resourceGroupMap.containsKey(getId().toString()) ? MAPPER.readerFor(DistributedResourceGroupAggrStats.class)
                            .readValue(resourceGroupMap.get(getId().toString())) : null;
                    if (groupAggrStats != null) {
                        globalTotalRunningQueries += groupAggrStats.getRunningQueries();
                        globalTotalQueuedQueries += groupAggrStats.getQueuedQueries();
                        globalDescendantQueuedQueries += groupAggrStats.getDescendantQueuedQueries();
                        globalDescendantRunningQueries += groupAggrStats.getDescendantRunningQueries();
                        globalCachedMemoryUsageBytes += groupAggrStats.getCachedMemoryUsageBytes();
                        globalCpuUsageMillis += groupAggrStats.getCachedMemoryUsageBytes();
                    }
                }
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(String.format("Error fetching resource group state with group id = %s, caused by ObjectMapper: %s", id, e.getMessage()));
            }
        }
    }
}
