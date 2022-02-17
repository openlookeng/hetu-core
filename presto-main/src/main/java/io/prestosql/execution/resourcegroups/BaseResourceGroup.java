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

import com.google.common.collect.ImmutableList;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.ManagedQueryExecution;
import io.prestosql.server.QueryStateInfo;
import io.prestosql.server.ResourceGroupInfo;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.resourcegroups.KillPolicy;
import io.prestosql.spi.resourcegroups.ResourceGroup;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.ResourceGroupState;
import io.prestosql.spi.resourcegroups.SchedulingPolicy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.spi.StandardErrorCode.INVALID_RESOURCE_GROUP;
import static io.prestosql.spi.resourcegroups.ResourceGroupState.CAN_QUEUE;
import static io.prestosql.spi.resourcegroups.ResourceGroupState.CAN_RUN;
import static io.prestosql.spi.resourcegroups.ResourceGroupState.FULL;
import static io.prestosql.spi.resourcegroups.SchedulingPolicy.FAIR;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class BaseResourceGroup
        implements ResourceGroup
{
    public static final int DEFAULT_WEIGHT = 1;

    protected final BaseResourceGroup root;
    protected final Optional<BaseResourceGroup> parent;
    protected final ResourceGroupId id;
    protected final BiConsumer<BaseResourceGroup, Boolean> jmxExportListener;
    protected final Executor executor;

    // Configuration
    // =============
    @GuardedBy("root")
    protected long softMemoryLimitBytes = Long.MAX_VALUE;
    @GuardedBy("root")
    protected long softReservedMemory = Long.MAX_VALUE; // default value for softReservedMemory
    @GuardedBy("root")
    protected int hardReservedConcurrency = Integer.MAX_VALUE; // default value for hardReservedConcurrency
    @GuardedBy("root")
    protected int softConcurrencyLimit;
    @GuardedBy("root")
    protected int hardConcurrencyLimit;
    @GuardedBy("root")
    protected int maxQueuedQueries;
    @GuardedBy("root")
    protected long softCpuLimitMillis = Long.MAX_VALUE;
    @GuardedBy("root")
    protected long hardCpuLimitMillis = Long.MAX_VALUE;
    @GuardedBy("root")
    protected long cpuQuotaGenerationMillisPerSecond = Long.MAX_VALUE;
    @GuardedBy("root")
    protected int schedulingWeight = DEFAULT_WEIGHT;
    @GuardedBy("root")
    protected SchedulingPolicy schedulingPolicy = FAIR;
    @GuardedBy("root")
    protected boolean jmxExport;
    @GuardedBy("root")
    protected KillPolicy killPolicy = KillPolicy.NO_KILL;
    @GuardedBy("root")
    protected int memoryMarginPercent = 10;
    @GuardedBy("root")
    protected int queryProgressMarginPercent = 5;

    // Live data structures
    // ====================
    @GuardedBy("root")
    protected final Map<String, BaseResourceGroup> subGroups = new HashMap<>();
    // Sub groups with queued queries, that have capacity to run them
    // That is, they must return true when internalStartNext() is called on them
    @GuardedBy("root")
    protected Queue<BaseResourceGroup> eligibleSubGroups = new FifoQueue<>();
    // Memory usage is cached because it changes very rapidly while queries are running, and would be expensive to track continuously
    @GuardedBy("root")
    protected long cachedMemoryUsageBytes;

    private final CounterStat timeBetweenStartsSec = new CounterStat();

    public BaseResourceGroup(Optional<BaseResourceGroup> parent,
            String name,
            BiConsumer<BaseResourceGroup, Boolean> jmxExportListener,
            Executor executor)
    {
        this.parent = requireNonNull(parent, "parent is null");
        this.jmxExportListener = requireNonNull(jmxExportListener, "jmxExportListener is null");
        this.executor = requireNonNull(executor, "executor is null");
        requireNonNull(name, "name is null");
        if (parent.isPresent()) {
            id = new ResourceGroupId(parent.get().getId(), name);
            root = parent.get().root;
        }
        else {
            id = new ResourceGroupId(name);
            root = this;
        }
    }

    /**
     * Get full resource group info
     *
     * @return Full resource group info
     */
    public ResourceGroupInfo getFullInfo()
    {
        synchronized (root) {
            return new ResourceGroupInfo(
                    id,
                    getState(),
                    schedulingPolicy,
                    schedulingWeight,
                    DataSize.succinctBytes(softMemoryLimitBytes),
                    DataSize.succinctBytes(softReservedMemory),
                    softConcurrencyLimit,
                    hardConcurrencyLimit,
                    hardReservedConcurrency,
                    maxQueuedQueries,
                    killPolicy,
                    DataSize.succinctBytes(cachedMemoryUsageBytes),
                    getQueuedQueries(),
                    getRunningQueries(),
                    eligibleSubGroups.size(),
                    subGroups.values().stream()
                            .filter(group -> group.getRunningQueries() + group.getQueuedQueries() > 0)
                            .map(group -> group.getSummaryInfo())
                            .collect(toImmutableList()),
                    getAggregatedRunningQueriesInfo());
        }
    }

    /**
     * Get detailed information of current resource group
     *
     * @return ResourceGroupInfo object includes detailed resource group information
     */
    public ResourceGroupInfo getInfo()
    {
        synchronized (root) {
            return new ResourceGroupInfo(
                    id,
                    getState(),
                    schedulingPolicy,
                    schedulingWeight,
                    DataSize.succinctBytes(softMemoryLimitBytes),
                    DataSize.succinctBytes(softReservedMemory),
                    softConcurrencyLimit,
                    hardConcurrencyLimit,
                    hardReservedConcurrency,
                    maxQueuedQueries,
                    killPolicy,
                    DataSize.succinctBytes(cachedMemoryUsageBytes),
                    getQueuedQueries(),
                    getRunningQueries(),
                    eligibleSubGroups.size(),
                    subGroups.values().stream()
                            .filter(group -> group.getRunningQueries() + group.getQueuedQueries() > 0)
                            .map(group -> group.getSummaryInfo())
                            .collect(toImmutableList()),
                    null);
        }
    }

    private ResourceGroupInfo getSummaryInfo()
    {
        synchronized (root) {
            return new ResourceGroupInfo(
                    id,
                    getState(),
                    schedulingPolicy,
                    schedulingWeight,
                    DataSize.succinctBytes(softMemoryLimitBytes),
                    DataSize.succinctBytes(softReservedMemory),
                    softConcurrencyLimit,
                    hardConcurrencyLimit,
                    hardReservedConcurrency,
                    maxQueuedQueries,
                    killPolicy,
                    DataSize.succinctBytes(cachedMemoryUsageBytes),
                    getQueuedQueries(),
                    getRunningQueries(),
                    eligibleSubGroups.size(),
                    null,
                    null);
        }
    }

    private ResourceGroupState getState()
    {
        synchronized (root) {
            if (canRunMore()) {
                return CAN_RUN;
            }
            else if (canQueueMore()) {
                return CAN_QUEUE;
            }
            else {
                return FULL;
            }
        }
    }

    /**
     * Get list of resource groups that are between current resource group and root resource group
     *
     * @return list of resource groups
     */
    public List<ResourceGroupInfo> getPathToRoot()
    {
        synchronized (root) {
            ImmutableList.Builder<ResourceGroupInfo> builder = ImmutableList.builder();
            BaseResourceGroup group = this;
            while (group != null) {
                builder.add(group.getInfo());
                group = group.parent.orElse(null);
            }

            return builder.build();
        }
    }

    @Override
    public ResourceGroupId getId()
    {
        return id;
    }

    @Override
    public DataSize getSoftMemoryLimit()
    {
        synchronized (root) {
            return new DataSize(softMemoryLimitBytes, BYTE);
        }
    }

    @Override
    public Duration getSoftCpuLimit()
    {
        synchronized (root) {
            return new Duration(softCpuLimitMillis, MILLISECONDS);
        }
    }

    @Override
    public Duration getHardCpuLimit()
    {
        synchronized (root) {
            return new Duration(hardCpuLimitMillis, MILLISECONDS);
        }
    }

    @Override
    public void setCpuQuotaGenerationMillisPerSecond(long rate)
    {
        checkArgument(rate > 0, "Cpu quota generation must be positive");
        synchronized (root) {
            cpuQuotaGenerationMillisPerSecond = rate;
        }
    }

    @Override
    public long getCpuQuotaGenerationMillisPerSecond()
    {
        synchronized (root) {
            return cpuQuotaGenerationMillisPerSecond;
        }
    }

    @Override
    public int getSoftConcurrencyLimit()
    {
        synchronized (root) {
            return softConcurrencyLimit;
        }
    }

    // Get softReservedMemory in bytes for resource group
    @Override
    public DataSize getSoftReservedMemory()
    {
        synchronized (root) {
            return new DataSize(softReservedMemory, BYTE);
        }
    }

    // Get hardReservedConcurrency for resource group
    @Override
    public int getHardReservedConcurrency()
    {
        {
            synchronized (root) {
                return hardReservedConcurrency;
            }
        }
    }

    @Managed
    @Override
    public int getHardConcurrencyLimit()
    {
        synchronized (root) {
            return hardConcurrencyLimit;
        }
    }

    @Managed
    @Override
    public int getMaxQueuedQueries()
    {
        synchronized (root) {
            return maxQueuedQueries;
        }
    }

    @Managed
    @Override
    public void setMaxQueuedQueries(int maxQueuedQueries)
    {
        checkArgument(maxQueuedQueries >= 0, "maxQueuedQueries is negative");
        synchronized (root) {
            this.maxQueuedQueries = maxQueuedQueries;
        }
    }

    @Managed
    @Nested
    public CounterStat getTimeBetweenStartsSec()
    {
        return timeBetweenStartsSec;
    }

    @Override
    public int getSchedulingWeight()
    {
        synchronized (root) {
            return schedulingWeight;
        }
    }

    @Override
    public SchedulingPolicy getSchedulingPolicy()
    {
        synchronized (root) {
            return schedulingPolicy;
        }
    }

    @Override
    public KillPolicy getKillPolicy()
    {
        synchronized (root) {
            return killPolicy;
        }
    }

    @Override
    public boolean getJmxExport()
    {
        synchronized (root) {
            return jmxExport;
        }
    }

    @Override
    public void setJmxExport(boolean export)
    {
        synchronized (root) {
            jmxExport = export;
        }
        jmxExportListener.accept(this, export);
    }

    public int getMemoryMarginPercent()
    {
        return memoryMarginPercent;
    }

    public void setMemoryMarginPercent(int memoryMarginPercent)
    {
        synchronized (root) {
            this.memoryMarginPercent = memoryMarginPercent;
        }
    }

    public int getQueryProgressMarginPercent()
    {
        return queryProgressMarginPercent;
    }

    public void setQueryProgressMarginPercent(int queryProgressMarginPercent)
    {
        synchronized (root) {
            this.queryProgressMarginPercent = queryProgressMarginPercent;
        }
    }

    /**
     * Try to run a query, if no available resource put the query in queue
     *
     * @param query Query execution contains query states
     */
    public void run(ManagedQueryExecution query)
    {
        synchronized (root) {
            if (!subGroups.isEmpty()) {
                throw new PrestoException(INVALID_RESOURCE_GROUP, format("Cannot add queries to %s. It is not a leaf group.", id));
            }
            // Check all ancestors for capacity
            BaseResourceGroup group = this;
            boolean canQueue = true;
            boolean canRun = true;
            while (true) {
                canQueue = canQueue && group.canQueueMore();
                canRun = canRun && group.canRunMore();
                if (!group.parent.isPresent()) {
                    break;
                }
                group = group.parent.get();
            }
            if (!canQueue && !canRun) {
                query.fail(new QueryQueueFullException(id));
                return;
            }
            if (canRun) {
                startInBackground(query);
            }
            else {
                enqueueQuery(query);
            }
            query.addStateChangeListener(state -> {
                if (state.isDone()) {
                    queryFinished(query);
                }
            });
        }
    }

    /**
     * Adjust hardConcurrencyLimit based on softConcurrencyLimit and current CPU usage
     *
     * @param hardConcurrencyLimit Hard concurrency limit set in resource group
     * @param cpuUsageMillis Current CPU usage in milliseconds
     * @return Adjusted hard concurrency limit
     */
    protected int adjustHardConcurrency(int hardConcurrencyLimit, long cpuUsageMillis)
    {
        int concurrencyLimit = hardConcurrencyLimit;
        // No need to apply penalty if softCpuLimit is bigger than hardCpuLimit
        if (hardCpuLimitMillis <= softCpuLimitMillis) {
            return concurrencyLimit;
        }

        if (cpuUsageMillis >= softCpuLimitMillis) {
            // TODO: Consider whether cpu limit math should be performed on softConcurrency or hardConcurrency
            // Linear penalty between soft and hard limit
            double penalty = (cpuUsageMillis - softCpuLimitMillis) / (double) (hardCpuLimitMillis - softCpuLimitMillis);
            concurrencyLimit = (int) Math.floor(concurrencyLimit * (1 - penalty));
            // Always penalize by at least one
            concurrencyLimit = min(this.hardConcurrencyLimit - 1, concurrencyLimit);
            // Always allow at least one running query
            concurrencyLimit = Math.max(1, concurrencyLimit);
        }
        return concurrencyLimit;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BaseResourceGroup)) {
            return false;
        }
        BaseResourceGroup that = (BaseResourceGroup) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    /**
     * Add query into internal queues
     *
     * @param query Query to be queued
     */
    abstract void enqueueQuery(ManagedQueryExecution query);

    /**
     * Start a new query asynchronously
     *
     * @param query Query to be started
     */
    abstract void startInBackground(ManagedQueryExecution query);

    /**
     * Add event handlers when query finished
     *
     * @param query Query to listen on
     */
    abstract void queryFinished(ManagedQueryExecution query);

    /**
     * Get aggregated query info for current running queries including queries in all the sub groups
     *
     * @return List of aggregated query info
     */
    abstract List<QueryStateInfo> getAggregatedRunningQueriesInfo();

    /**
     * Check if more query can be run in the resource group
     *
     * @return Whether more queries can be run
     */
    abstract boolean canRunMore();

    /**
     * Check if more query can be queued in the resource group
     *
     * @return Whether more queries can be queued
     */
    abstract boolean canQueueMore();

    /**
     * Get or create sub group by name
     *
     * @param name sub group name
     * @return the sub group
     */
    abstract BaseResourceGroup getOrCreateSubGroup(String name);

    /**
     * Refresh all the internal stats of the resource group
     */
    abstract void internalRefreshStats();

    /**
     * Periodically check and start queries in queue
     */
    public abstract void processQueuedQueries();

    /**
     * Generate new CPU quota for the resource groups
     *
     * @param elapsedSeconds Elapsed seconds since last quota generation
     */
    public abstract void generateCpuQuota(long elapsedSeconds);

    public abstract long getCachedMemoryUsageBytes();
}
