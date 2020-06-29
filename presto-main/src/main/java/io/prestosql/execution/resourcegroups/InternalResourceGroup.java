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
package io.prestosql.execution.resourcegroups;

import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.ManagedQueryExecution;
import io.prestosql.execution.resourcegroups.WeightedFairQueue.Usage;
import io.prestosql.server.QueryStateInfo;
import io.prestosql.spi.resourcegroups.SchedulingPolicy;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.math.LongMath.saturatedAdd;
import static com.google.common.math.LongMath.saturatedMultiply;
import static com.google.common.math.LongMath.saturatedSubtract;
import static io.prestosql.SystemSessionProperties.getQueryPriority;
import static io.prestosql.server.QueryStateInfo.createQueryStateInfo;
import static io.prestosql.spi.ErrorType.USER_ERROR;
import static io.prestosql.spi.resourcegroups.SchedulingPolicy.QUERY_PRIORITY;
import static io.prestosql.spi.resourcegroups.SchedulingPolicy.WEIGHTED;
import static io.prestosql.spi.resourcegroups.SchedulingPolicy.WEIGHTED_FAIR;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * Resource groups form a tree, and all access to a group is guarded by the root of the tree.
 * Queries are submitted to leaf groups. Never to intermediate groups. Intermediate groups
 * aggregate resource consumption from their children, and may have their own limitations that
 * are enforced.
 */
@ThreadSafe
public class InternalResourceGroup
        extends BaseResourceGroup
{
    private final Optional<InternalResourceGroup> parentGroup;

    // Live data structures
    // ====================
    // Sub groups whose memory usage may be out of date. Most likely because they have a running query.
    @GuardedBy("root")
    private final Set<InternalResourceGroup> dirtySubGroups = new HashSet<>();
    @GuardedBy("root")
    private UpdateablePriorityQueue<ManagedQueryExecution> queuedQueries = new FifoQueue<>();
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

    public InternalResourceGroup(Optional<BaseResourceGroup> parent, String name, BiConsumer<BaseResourceGroup, Boolean> jmxExportListener, Executor executor)
    {
        super(parent, name, jmxExportListener, executor);
        if (parent.isPresent()) {
            parentGroup = Optional.of((InternalResourceGroup) parent.get());
        }
        else {
            parentGroup = Optional.empty();
        }
    }

    @Override
    protected List<QueryStateInfo> getAggregatedRunningQueriesInfo()
    {
        synchronized (root) {
            if (subGroups.isEmpty()) {
                return runningQueries.stream()
                        .map(ManagedQueryExecution::getBasicQueryInfo)
                        .map(queryInfo -> createQueryStateInfo(queryInfo, Optional.of(id)))
                        .collect(toImmutableList());
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
            return runningQueries.size() + descendantRunningQueries;
        }
    }

    @Managed
    @Override
    public int getQueuedQueries()
    {
        synchronized (root) {
            return queuedQueries.size() + descendantQueuedQueries;
        }
    }

    @Managed
    public int getWaitingQueuedQueries()
    {
        synchronized (root) {
            // For leaf group, when no queries can run, all queued queries are waiting for resources on this resource group.
            if (subGroups.isEmpty()) {
                return queuedQueries.size();
            }

            // For internal groups, when no queries can run, only queries that could run on its subgroups are waiting for resources on this group.
            int waitingQueuedQueries = 0;
            for (BaseResourceGroup subGroup : subGroups.values()) {
                if (subGroup.canRunMore()) {
                    waitingQueuedQueries += min(subGroup.getQueuedQueries(), subGroup.getHardConcurrencyLimit() - subGroup.getRunningQueries());
                }
            }

            return waitingQueuedQueries;
        }
    }

    @Override
    public void setSoftMemoryLimit(DataSize limit)
    {
        synchronized (root) {
            boolean oldCanRun = canRunMore();
            this.softMemoryLimitBytes = limit.toBytes();
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
    }

    @Override
    public void setSoftCpuLimit(Duration limit)
    {
        synchronized (root) {
            if (limit.toMillis() > hardCpuLimitMillis) {
                setHardCpuLimit(limit);
            }
            boolean oldCanRun = canRunMore();
            this.softCpuLimitMillis = limit.toMillis();
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
    }

    @Override
    public void setHardCpuLimit(Duration limit)
    {
        synchronized (root) {
            if (limit.toMillis() < softCpuLimitMillis) {
                setSoftCpuLimit(limit);
            }
            boolean oldCanRun = canRunMore();
            this.hardCpuLimitMillis = limit.toMillis();
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
    }

    @Override
    public void setSoftConcurrencyLimit(int softConcurrencyLimit)
    {
        checkArgument(softConcurrencyLimit >= 0, "softConcurrencyLimit is negative");
        synchronized (root) {
            boolean oldCanRun = canRunMore();
            this.softConcurrencyLimit = softConcurrencyLimit;
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
    }

    // Set softReservedMemory for resource group
    @Override
    public void setSoftReservedMemory(DataSize softReservedMemory)
    {
        synchronized (root) {
            boolean oldCanRun = canRunMore();
            this.softReservedMemory = softReservedMemory.toBytes();
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
    }

    // Set hardReservedConcurrency for resource group
    @Override
    public void setHardReservedConcurrency(int hardReservedConcurrency)
    {
        checkArgument(hardReservedConcurrency >= 0, "hardReservedConcurrency is negative");
        synchronized (root) {
            boolean oldCanRun = canRunMore();
            this.hardReservedConcurrency = hardReservedConcurrency;
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
    }

    @Managed
    @Override
    public void setHardConcurrencyLimit(int hardConcurrencyLimit)
    {
        checkArgument(hardConcurrencyLimit >= 0, "hardConcurrencyLimit is negative");
        synchronized (root) {
            boolean oldCanRun = canRunMore();
            this.hardConcurrencyLimit = hardConcurrencyLimit;
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
    }

    @Override
    public void setSchedulingWeight(int weight)
    {
        checkArgument(weight > 0, "weight must be positive");
        synchronized (root) {
            this.schedulingWeight = weight;
            if (parentGroup.isPresent() && parentGroup.get().schedulingPolicy == WEIGHTED && parentGroup.get().eligibleSubGroups.contains(this)) {
                parentGroup.get().addOrUpdateSubGroup(this);
            }
        }
    }

    @Override
    public void setSchedulingPolicy(SchedulingPolicy policy)
    {
        synchronized (root) {
            if (policy == schedulingPolicy) {
                return;
            }

            if (parent.isPresent() && parent.get().schedulingPolicy == QUERY_PRIORITY) {
                checkArgument(policy == QUERY_PRIORITY, "Parent of %s uses query priority scheduling, so %s must also", id, id);
            }

            // Switch to the appropriate queue implementation to implement the desired policy
            Queue<BaseResourceGroup> queue;
            UpdateablePriorityQueue<ManagedQueryExecution> queryQueue;
            switch (policy) {
                case FAIR:
                    queue = new FifoQueue<>();
                    queryQueue = new FifoQueue<>();
                    break;
                case WEIGHTED:
                    queue = new StochasticPriorityQueue<>();
                    queryQueue = new StochasticPriorityQueue<>();
                    break;
                case WEIGHTED_FAIR:
                    queue = new WeightedFairQueue<>();
                    queryQueue = new IndexedPriorityQueue<>();
                    break;
                case QUERY_PRIORITY:
                    // Sub groups must use query priority to ensure ordering
                    for (BaseResourceGroup group : subGroups.values()) {
                        group.setSchedulingPolicy(QUERY_PRIORITY);
                    }
                    queue = new IndexedPriorityQueue<>();
                    queryQueue = new IndexedPriorityQueue<>();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported scheduling policy: " + policy);
            }
            schedulingPolicy = policy;
            while (!eligibleSubGroups.isEmpty()) {
                BaseResourceGroup group = eligibleSubGroups.poll();
                addOrUpdateSubGroup(queue, group);
            }
            eligibleSubGroups = queue;
            while (!queuedQueries.isEmpty()) {
                ManagedQueryExecution query = queuedQueries.poll();
                queryQueue.addOrUpdate(query, getQueryPriority(query.getSession()));
            }
            queuedQueries = queryQueue;
        }
    }

    @Override
    public InternalResourceGroup getOrCreateSubGroup(String name)
    {
        requireNonNull(name, "name is null");
        synchronized (root) {
            checkArgument(runningQueries.isEmpty() && queuedQueries.isEmpty(), "Cannot add sub group to %s while queries are running", id);
            if (subGroups.containsKey(name)) {
                return (InternalResourceGroup) subGroups.get(name);
            }
            InternalResourceGroup subGroup = new InternalResourceGroup(Optional.of(this), name, jmxExportListener, executor);
            // Sub group must use query priority to ensure ordering
            if (schedulingPolicy == QUERY_PRIORITY) {
                subGroup.setSchedulingPolicy(QUERY_PRIORITY);
            }
            subGroups.put(name, subGroup);
            return subGroup;
        }
    }

    @Override
    protected void enqueueQuery(ManagedQueryExecution query)
    {
        checkState(Thread.holdsLock(root), "Must hold lock to enqueue a query");
        synchronized (root) {
            queuedQueries.addOrUpdate(query, getQueryPriority(query.getSession()));
            InternalResourceGroup group = this;
            while (group.parent.isPresent()) {
                group.parentGroup.get().descendantQueuedQueries++;
                group = group.parentGroup.get();
            }
            updateEligibility();
        }
    }

    // This method must be called whenever the group's eligibility to run more queries may have changed.
    private void updateEligibility()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to update eligibility");
        synchronized (root) {
            if (!parentGroup.isPresent()) {
                return;
            }
            if (isEligibleToStartNext()) {
                parentGroup.get().addOrUpdateSubGroup(this);
            }
            else {
                parentGroup.get().eligibleSubGroups.remove(this);
                lastStartMillis = 0;
            }
            parentGroup.get().updateEligibility();
        }
    }

    @Override
    protected void startInBackground(ManagedQueryExecution query)
    {
        checkState(Thread.holdsLock(root), "Must hold lock to start a query");
        synchronized (root) {
            runningQueries.add(query);
            InternalResourceGroup group = this;
            while (group.parentGroup.isPresent()) {
                group.parentGroup.get().descendantRunningQueries++;
                group.parentGroup.get().dirtySubGroups.add(group);
                group = group.parentGroup.get();
            }
            updateEligibility();
            executor.execute(query::startWaitingForResources);
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
            // Only count the CPU time if the query succeeded, or the failure was the fault of the user
            if (!query.getErrorCode().isPresent() || query.getErrorCode().get().getType() == USER_ERROR) {
                InternalResourceGroup group = this;
                while (group != null) {
                    group.cpuUsageMillis = saturatedAdd(group.cpuUsageMillis, query.getTotalCpuTime().toMillis());
                    group = group.parentGroup.orElse(null);
                }
            }
            if (runningQueries.contains(query)) {
                runningQueries.remove(query);
                InternalResourceGroup group = this;
                while (group.parentGroup.isPresent()) {
                    group.parentGroup.get().descendantRunningQueries--;
                    group = group.parentGroup.get();
                }
            }
            else {
                queuedQueries.remove(query);
                InternalResourceGroup group = this;
                while (group.parentGroup.isPresent()) {
                    group.parentGroup.get().descendantQueuedQueries--;
                    group = group.parentGroup.get();
                }
            }
            updateEligibility();
        }
    }

    // Memory usage stats are expensive to maintain, so this method must be called periodically to update them
    @Override
    protected void internalRefreshStats()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to refresh stats");
        synchronized (root) {
            if (subGroups.isEmpty()) {
                cachedMemoryUsageBytes = 0;
                for (ManagedQueryExecution query : runningQueries) {
                    cachedMemoryUsageBytes += query.getUserMemoryReservation().toBytes();
                }
            }
            else {
                for (Iterator<InternalResourceGroup> iterator = dirtySubGroups.iterator(); iterator.hasNext(); ) {
                    InternalResourceGroup subGroup = iterator.next();
                    long oldMemoryUsageBytes = subGroup.cachedMemoryUsageBytes;
                    cachedMemoryUsageBytes -= oldMemoryUsageBytes;
                    subGroup.internalRefreshStats();
                    cachedMemoryUsageBytes += subGroup.cachedMemoryUsageBytes;
                    if (!subGroup.isDirty()) {
                        iterator.remove();
                    }
                    if (oldMemoryUsageBytes != subGroup.cachedMemoryUsageBytes) {
                        subGroup.updateEligibility();
                    }
                }
            }
        }
    }

    protected void internalGenerateCpuQuota(long elapsedSeconds)
    {
        checkState(Thread.holdsLock(root), "Must hold lock to generate cpu quota");
        synchronized (root) {
            // Bug fix in cases when the CPU usage is initially higher than the cpu quota, but after a while,
            // when the CPU usage is then lower than the CPU quota, queued queries are still not run. This fix was
            long newQuota = saturatedMultiply(elapsedSeconds, cpuQuotaGenerationMillisPerSecond);
            long oldUsageMillis = cpuUsageMillis;
            cpuUsageMillis = saturatedSubtract(cpuUsageMillis, newQuota);

            if (cpuUsageMillis < 0 || cpuUsageMillis == Long.MAX_VALUE) {
                cpuUsageMillis = 0;
            }

            if ((cpuUsageMillis < hardCpuLimitMillis && oldUsageMillis >= hardCpuLimitMillis) ||
                    (cpuUsageMillis < softCpuLimitMillis && oldUsageMillis >= softCpuLimitMillis)) {
                updateEligibility();
            }

            for (BaseResourceGroup group : subGroups.values()) {
                ((InternalResourceGroup) group).internalGenerateCpuQuota(elapsedSeconds);
            }
        }
    }

    protected boolean internalStartNext()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to find next query");
        synchronized (root) {
            if (!canRunMore()) {
                return false;
            }
            ManagedQueryExecution query = queuedQueries.poll();
            if (query != null) {
                startInBackground(query);
                return true;
            }

            // Remove even if the sub group still has queued queries, so that it goes to the back of the queue
            InternalResourceGroup subGroup = (InternalResourceGroup) eligibleSubGroups.poll();
            if (subGroup == null) {
                return false;
            }
            boolean started = subGroup.internalStartNext();
            checkState(started, "Eligible sub group had no queries to run");

            long currentTime = System.currentTimeMillis();
            if (lastStartMillis != 0) {
                timeBetweenStartsSec.update(Math.max(0, (currentTime - lastStartMillis) / 1000));
            }
            lastStartMillis = currentTime;

            descendantQueuedQueries--;
            // Don't call updateEligibility here, as we're in a recursive call, and don't want to repeatedly update our ancestors.
            if (subGroup.isEligibleToStartNext()) {
                addOrUpdateSubGroup(subGroup);
            }
            return true;
        }
    }

    private void addOrUpdateSubGroup(Queue<BaseResourceGroup> queue, BaseResourceGroup group)
    {
        if (schedulingPolicy == WEIGHTED_FAIR) {
            ((WeightedFairQueue<BaseResourceGroup>) queue).addOrUpdate(group, new Usage(group.getSchedulingWeight(), group.getRunningQueries()));
        }
        else {
            ((UpdateablePriorityQueue<BaseResourceGroup>) queue).addOrUpdate(group, getSubGroupSchedulingPriority(schedulingPolicy, (InternalResourceGroup) group));
        }
    }

    private void addOrUpdateSubGroup(InternalResourceGroup group)
    {
        addOrUpdateSubGroup(eligibleSubGroups, group);
    }

    private static long getSubGroupSchedulingPriority(SchedulingPolicy policy, InternalResourceGroup group)
    {
        if (policy == QUERY_PRIORITY) {
            return group.getHighestQueryPriority();
        }
        else {
            return group.computeSchedulingWeight();
        }
    }

    private long computeSchedulingWeight()
    {
        if (runningQueries.size() + descendantRunningQueries >= softConcurrencyLimit) {
            return schedulingWeight;
        }

        return (long) Integer.MAX_VALUE * schedulingWeight;
    }

    private boolean isDirty()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            return runningQueries.size() + descendantRunningQueries > 0;
        }
    }

    private boolean isEligibleToStartNext()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            if (!canRunMore()) {
                return false;
            }
            return !queuedQueries.isEmpty() || !eligibleSubGroups.isEmpty();
        }
    }

    private int getHighestQueryPriority()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            checkState(queuedQueries instanceof IndexedPriorityQueue, "Queued queries not ordered");
            if (queuedQueries.isEmpty()) {
                return 0;
            }
            return getQueryPriority(queuedQueries.peek().getSession());
        }
    }

    @Override
    protected boolean canQueueMore()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            return descendantQueuedQueries + queuedQueries.size() < maxQueuedQueries;
        }
    }

    @Override
    protected boolean canRunMore()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            if (cpuUsageMillis >= hardCpuLimitMillis) {
                return false;
            }

            int hardConcurrencyLimit = this.hardConcurrencyLimit;
            return hasCapacity(adjustHardConcurrency(hardConcurrencyLimit, cpuUsageMillis));
        }
    }

    // Check whether the resource group has capacity to run more queries in terms of the memory limit,
    // concurrency limit, reserved concurrency and reserved memory usage
    // @param hardConcurrencyLimit penalized hardConcurrencyLimit
    private boolean hasCapacity(int hardConcurrencyLimit)
    {
        if (runningQueries.size() + descendantRunningQueries >= hardConcurrencyLimit ||
                cachedMemoryUsageBytes > softMemoryLimitBytes) {
            return false;
        }
        if (parent.isPresent()) {
            // Check if hardConcurrencyLimit of the parent is reached with reserved concurrency for each peer group
            if (runningQueries.size() + descendantRunningQueries >= hardReservedConcurrency) {
                int peerTotalQuerySize = 0;
                for (InternalResourceGroup group : (Collection<InternalResourceGroup>) parent.get().subGroups()) {
                    peerTotalQuerySize += Math.max(group.runningQueries.size() + group.descendantRunningQueries, group.hardReservedConcurrency);
                }
                if (parent.get().hardConcurrencyLimit <= peerTotalQuerySize) {
                    return false;
                }
            }
            // Check if the softMemoryLimit of parent is reached with reserved memory for each peer group
            if (cachedMemoryUsageBytes >= softReservedMemory) {
                long peerGroupTotalUsage = 0;
                for (InternalResourceGroup group : (Collection<InternalResourceGroup>) parent.get().subGroups()) {
                    peerGroupTotalUsage += Math.max(group.cachedMemoryUsageBytes, group.softReservedMemory);
                }
                if (parent.get().softMemoryLimitBytes <= peerGroupTotalUsage) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Collection<BaseResourceGroup> subGroups()
    {
        synchronized (root) {
            return subGroups.values();
        }
    }

    @Override
    public synchronized void processQueuedQueries()
    {
        internalRefreshStats();
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
}
