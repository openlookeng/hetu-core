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
package io.prestosql.memory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.io.Closer;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.QueryExecution;
import io.prestosql.execution.QueryIdGenerator;
import io.prestosql.memory.LowMemoryKiller.QueryMemoryInfo;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.NodeState;
import io.prestosql.protocol.Codec;
import io.prestosql.protocol.SmileCodec;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.InternalCommunicationConfig;
import io.prestosql.server.ServerConfig;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.memory.ClusterMemoryPoolManager;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spi.memory.MemoryPoolInfo;
import io.prestosql.statestore.StateStoreConstants;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.utils.HetuConfig;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Sets.difference;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.ExceededMemoryLimitException.exceededGlobalTotalLimit;
import static io.prestosql.ExceededMemoryLimitException.exceededGlobalUserLimit;
import static io.prestosql.SystemSessionProperties.RESOURCE_OVERCOMMIT;
import static io.prestosql.SystemSessionProperties.getQueryMaxMemory;
import static io.prestosql.SystemSessionProperties.getQueryMaxTotalMemory;
import static io.prestosql.SystemSessionProperties.resourceOvercommit;
import static io.prestosql.memory.LocalMemoryManager.GENERAL_POOL;
import static io.prestosql.memory.LocalMemoryManager.RESERVED_POOL;
import static io.prestosql.metadata.NodeState.ACTIVE;
import static io.prestosql.metadata.NodeState.ISOLATED;
import static io.prestosql.metadata.NodeState.ISOLATING;
import static io.prestosql.metadata.NodeState.SHUTTING_DOWN;
import static io.prestosql.protocol.JsonCodecWrapper.wrapJsonCodec;
import static io.prestosql.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;

public class ClusterMemoryManager
        implements ClusterMemoryPoolManager
{
    private static final Logger log = Logger.get(ClusterMemoryManager.class);

    private final ExecutorService listenerExecutor = Executors.newSingleThreadExecutor();
    private final ClusterMemoryLeakDetector memoryLeakDetector = new ClusterMemoryLeakDetector();
    private final InternalNodeManager nodeManager;
    private final LocationFactory locationFactory;
    private final HttpClient httpClient;
    private final MBeanExporter exporter;
    private final Codec<MemoryInfo> memoryInfoCodec;
    private final Codec<MemoryPoolAssignmentsRequest> assignmentsRequestCodec;
    private final DataSize maxQueryMemory;
    private final DataSize maxQueryTotalMemory;
    private final boolean enabled;
    private final LowMemoryKiller lowMemoryKiller;
    private final Duration killOnOutOfMemoryDelay;
    private final String coordinatorId;
    private final AtomicLong totalAvailableProcessors = new AtomicLong();
    private final AtomicLong memoryPoolAssignmentsVersion = new AtomicLong();
    private final AtomicLong clusterUserMemoryReservation = new AtomicLong();
    private final AtomicLong clusterTotalMemoryReservation = new AtomicLong();
    private final AtomicLong clusterMemoryBytes = new AtomicLong();
    private final AtomicLong queriesKilledDueToOutOfMemory = new AtomicLong();
    // LocalStateProvider
    private final StateStoreProvider stateStoreProvider;
    private final HetuConfig hetuConfig;
    private final boolean isBinaryEncoding;

    @GuardedBy("this")
    private final Map<String, RemoteNodeMemory> nodes = new LinkedHashMap<>();

    @GuardedBy("this")
    private final Map<String, RemoteNodeMemory> allNodes = new LinkedHashMap<>();

    @GuardedBy("this")
    private final Map<MemoryPoolId, List<Consumer<MemoryPoolInfo>>> changeListeners = new HashMap<>();

    @GuardedBy("this")
    private final Map<MemoryPoolId, ClusterMemoryPool> pools;

    @GuardedBy("this")
    private long lastTimeNotOutOfMemory = System.nanoTime();

    @GuardedBy("this")
    private QueryId lastKilledQuery;

    @Inject
    public ClusterMemoryManager(
            @ForMemoryManager HttpClient httpClient,
            InternalNodeManager nodeManager,
            LocationFactory locationFactory,
            MBeanExporter exporter,
            JsonCodec<MemoryInfo> memoryInfoJsonCodec,
            SmileCodec<MemoryInfo> memoryInfoSmileCodec,
            JsonCodec<MemoryPoolAssignmentsRequest> assignmentsRequestJsonCodec,
            SmileCodec<MemoryPoolAssignmentsRequest> assignmentsRequestSmileCodec,
            QueryIdGenerator queryIdGenerator,
            LowMemoryKiller lowMemoryKiller,
            ServerConfig serverConfig,
            MemoryManagerConfig config,
            NodeMemoryConfig nodeMemoryConfig,
            StateStoreProvider stateStoreProvider,
            HetuConfig hetuConfig,
            InternalCommunicationConfig internalCommunicationConfig)
    {
        requireNonNull(config, "config is null");
        requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null");
        requireNonNull(serverConfig, "serverConfig is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.lowMemoryKiller = requireNonNull(lowMemoryKiller, "lowMemoryKiller is null");
        this.maxQueryMemory = config.getMaxQueryMemory();
        this.maxQueryTotalMemory = config.getMaxQueryTotalMemory();
        this.coordinatorId = queryIdGenerator.getCoordinatorId();
        this.enabled = serverConfig.isCoordinator();
        this.killOnOutOfMemoryDelay = config.getKillOnOutOfMemoryDelay();

        verify(maxQueryMemory.toBytes() <= maxQueryTotalMemory.toBytes(),
                "maxQueryMemory cannot be greater than maxQueryTotalMemory");

        this.isBinaryEncoding = internalCommunicationConfig.isBinaryEncoding();
        if (internalCommunicationConfig.isBinaryEncoding()) {
            this.memoryInfoCodec = requireNonNull(memoryInfoSmileCodec, "memoryInfoSmileCodec is null");
            this.assignmentsRequestCodec = requireNonNull(assignmentsRequestSmileCodec, "AssignmentRequestSmileCodec is null");
        }
        else {
            this.memoryInfoCodec = wrapJsonCodec(requireNonNull(memoryInfoJsonCodec, "memoryInfoJsonCodec is null"));
            this.assignmentsRequestCodec = wrapJsonCodec(requireNonNull(assignmentsRequestJsonCodec, "assignmentsRequestJsonCodec is null"));
        }

        this.pools = createClusterMemoryPools(nodeMemoryConfig.isReservedPoolEnabled());
        // Inject LocalStateProvider
        this.stateStoreProvider = stateStoreProvider;
        this.hetuConfig = requireNonNull(hetuConfig, "hetuConfig is null");
    }

    private Map<MemoryPoolId, ClusterMemoryPool> createClusterMemoryPools(boolean reservedPoolEnabled)
    {
        Set<MemoryPoolId> memoryPools = new HashSet<>();
        memoryPools.add(GENERAL_POOL);
        if (reservedPoolEnabled) {
            memoryPools.add(RESERVED_POOL);
        }

        ImmutableMap.Builder<MemoryPoolId, ClusterMemoryPool> builder = ImmutableMap.builder();
        for (MemoryPoolId poolId : memoryPools) {
            ClusterMemoryPool pool = new ClusterMemoryPool(poolId);
            builder.put(poolId, pool);
            try {
                exporter.exportWithGeneratedName(pool, ClusterMemoryPool.class, poolId.toString());
            }
            catch (JmxException e) {
                log.error(e, "Error exporting memory pool %s", poolId);
            }
        }
        return builder.build();
    }

    @Override
    public synchronized void addChangeListener(MemoryPoolId poolId, Consumer<MemoryPoolInfo> listener)
    {
        verify(memoryPoolExists(poolId), "Memory pool does not exist: %s", poolId);
        changeListeners.computeIfAbsent(poolId, id -> new ArrayList<>()).add(listener);
    }

    public synchronized boolean memoryPoolExists(MemoryPoolId poolId)
    {
        return pools.containsKey(poolId);
    }

    public synchronized void process(Iterable<QueryExecution> runningQueries, Supplier<List<BasicQueryInfo>> allQueryInfoSupplier)
    {
        if (!enabled) {
            return;
        }

        //make sure state store is loaded when multiple coordinator is enabled
        if (!hetuConfig.isMultipleCoordinatorEnabled()
                || (hetuConfig.isMultipleCoordinatorEnabled()
                && stateStoreProvider.getStateStore() != null)) {
            Lock lock = null;
            boolean locked = false;
            try {
                if (hetuConfig.isMultipleCoordinatorEnabled()) {
                    lock = stateStoreProvider.getStateStore().getLock(StateStoreConstants.HANDLE_OOM_QUERY_LOCK_NAME);
                }
                else {
                    lock = new ReentrantLock();
                }
                locked = lock.tryLock(StateStoreConstants.DEFAULT_ACQUIRED_LOCK_TIME_MS, TimeUnit.MILLISECONDS);
                if (locked) {
                    // TODO revocable memory reservations can also leak and may need to be detected in the future
                    // We are only concerned about the leaks in general pool.
                    memoryLeakDetector.checkForMemoryLeaks(allQueryInfoSupplier, pools.get(GENERAL_POOL).getQueryMemoryReservations());

                    boolean outOfMemory = isClusterOutOfMemory();
                    if (!outOfMemory) {
                        lastTimeNotOutOfMemory = System.nanoTime();
                    }

                    boolean queryKilled = false;
                    long totalUserMemoryBytes = 0L;
                    long totalMemoryBytes = 0L;
                    for (QueryExecution query : runningQueries) {
                        boolean resourceOvercommit = resourceOvercommit(query.getSession());
                        long userMemoryReservation = query.getUserMemoryReservation().toBytes();
                        long totalMemoryReservation = query.getTotalMemoryReservation().toBytes();

                        if (resourceOvercommit && outOfMemory) {
                            // If a query has requested resource overcommit, only kill it if the cluster has run out of memory
                            DataSize memory = succinctBytes(getQueryMemoryReservation(query));
                            query.fail(new PrestoException(CLUSTER_OUT_OF_MEMORY,
                                    format("The cluster is out of memory and %s=true, so this query was killed. It was using %s of memory", RESOURCE_OVERCOMMIT, memory)));
                            queryKilled = true;
                        }

                        if (!resourceOvercommit) {
                            long userMemoryLimit = min(maxQueryMemory.toBytes(), getQueryMaxMemory(query.getSession()).toBytes());
                            if (userMemoryReservation > userMemoryLimit) {
                                query.fail(exceededGlobalUserLimit(succinctBytes(userMemoryLimit)));
                                queryKilled = true;
                            }

                            long totalMemoryLimit = min(maxQueryTotalMemory.toBytes(), getQueryMaxTotalMemory(query.getSession()).toBytes());
                            if (totalMemoryReservation > totalMemoryLimit) {
                                query.fail(exceededGlobalTotalLimit(succinctBytes(totalMemoryLimit)));
                                queryKilled = true;
                            }
                        }

                        totalUserMemoryBytes += userMemoryReservation;
                        totalMemoryBytes += totalMemoryReservation;
                    }

                    clusterUserMemoryReservation.set(totalUserMemoryBytes);
                    clusterTotalMemoryReservation.set(totalMemoryBytes);

                    if (!(lowMemoryKiller instanceof NoneLowMemoryKiller) &&
                            outOfMemory &&
                            !queryKilled &&
                            nanosSince(lastTimeNotOutOfMemory).compareTo(killOnOutOfMemoryDelay) > 0) {
                        if (isLastKilledQueryGone()) {
                            callOomKiller(runningQueries);
                        }
                        else {
                            log.debug("Last killed query is still not gone: %s", lastKilledQuery);
                        }
                    }
                }
            }
            catch (Exception e) {
                log.error("Error handleOOMQuery: " + e.getMessage());
            }
            finally {
                if (lock != null && locked) {
                    lock.unlock();
                }
            }
        }

        Map<MemoryPoolId, Integer> countByPool = new HashMap<>();
        for (QueryExecution query : runningQueries) {
            MemoryPoolId id = query.getMemoryPool().getId();
            countByPool.put(id, countByPool.getOrDefault(id, 0) + 1);
        }

        updatePools(countByPool);

        MemoryPoolAssignmentsRequest assignmentsRequest;
        if (pools.containsKey(RESERVED_POOL)) {
            assignmentsRequest = updateAssignments(runningQueries);
        }
        else {
            // If reserved pool is not enabled, we don't create a MemoryPoolAssignmentsRequest that puts all the queries
            // in the general pool (as they already are). In this case we create an effectively NOOP MemoryPoolAssignmentsRequest.
            // Once the reserved pool is removed we should get rid of the logic of putting queries into reserved pool including
            // this piece of code.
            assignmentsRequest = new MemoryPoolAssignmentsRequest(coordinatorId, Long.MIN_VALUE, ImmutableList.of());
        }
        updateNodes(assignmentsRequest);
    }

    private synchronized void callOomKiller(Iterable<QueryExecution> runningQueries)
    {
        List<QueryMemoryInfo> queryMemoryInfoList = Streams.stream(runningQueries)
                .map(this::createQueryMemoryInfo)
                .collect(toImmutableList());
        List<MemoryInfo> nodeMemoryInfos = nodes.values().stream()
                .map(RemoteNodeMemory::getInfo)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
        Optional<QueryId> chosenQueryId = lowMemoryKiller.chooseQueryToKill(queryMemoryInfoList, nodeMemoryInfos);
        if (chosenQueryId.isPresent()) {
            log.debug("Low memory killer chose %s", chosenQueryId.get());
            Optional<QueryExecution> chosenQuery = Streams.stream(runningQueries).filter(query -> chosenQueryId.get().equals(query.getQueryId())).collect(toOptional());
            if (chosenQuery.isPresent()) {
                // See comments in  isLastKilledQueryGone for why chosenQuery might be absent.
                chosenQuery.get().fail(new PrestoException(CLUSTER_OUT_OF_MEMORY, "Query killed because the cluster is out of memory. Please try again in a few minutes."));
                queriesKilledDueToOutOfMemory.incrementAndGet();
                lastKilledQuery = chosenQueryId.get();
                logQueryKill(chosenQueryId.get(), nodeMemoryInfos);
            }
        }
    }

    @GuardedBy("this")
    private boolean isLastKilledQueryGone()
    {
        if (lastKilledQuery == null) {
            return true;
        }

        // If the lastKilledQuery is marked as leaked by the ClusterMemoryLeakDetector we consider the lastKilledQuery as gone,
        // so that the ClusterMemoryManager can continue to make progress even if there are leaks.
        // Even if the weak references to the leaked queries are GCed in the ClusterMemoryLeakDetector, it will mark the same queries
        // as leaked in its next run, and eventually the ClusterMemoryManager will make progress.
        if (memoryLeakDetector.wasQueryPossiblyLeaked(lastKilledQuery)) {
            lastKilledQuery = null;
            return true;
        }

        // pools fields is updated based on nodes field.
        // Therefore, if the query is gone from pools field, it should also be gone from nodes field.
        // However, since nodes can updated asynchronously, it has the potential of coming back after being gone.
        // Therefore, even if the query appears to be gone here, it might be back when one inspects nodes later.
        return !pools.get(GENERAL_POOL)
                .getQueryMemoryReservations()
                .containsKey(lastKilledQuery);
    }

    private void logQueryKill(QueryId killedQueryId, List<MemoryInfo> nodes)
    {
        if (!log.isInfoEnabled()) {
            return;
        }
        StringBuilder nodeDescription = new StringBuilder();
        nodeDescription.append("Query Kill Decision: Killed ").append(killedQueryId).append("\n");
        for (MemoryInfo node : nodes) {
            MemoryPoolInfo memoryPoolInfo = node.getPools().get(GENERAL_POOL);
            if (memoryPoolInfo == null) {
                continue;
            }
            nodeDescription.append("Query Kill Scenario: ");
            nodeDescription.append("MaxBytes ").append(memoryPoolInfo.getMaxBytes()).append(' ');
            nodeDescription.append("FreeBytes ").append(memoryPoolInfo.getFreeBytes() + memoryPoolInfo.getReservedRevocableBytes()).append(' ');
            nodeDescription.append("Queries ");
            Joiner.on(",").withKeyValueSeparator("=").appendTo(nodeDescription, memoryPoolInfo.getQueryMemoryReservations());
            nodeDescription.append('\n');
        }
        log.info(nodeDescription.toString());
    }

    @VisibleForTesting
    synchronized Map<MemoryPoolId, ClusterMemoryPool> getPools()
    {
        return ImmutableMap.copyOf(pools);
    }

    public synchronized Map<MemoryPoolId, MemoryPoolInfo> getMemoryPoolInfo()
    {
        ImmutableMap.Builder<MemoryPoolId, MemoryPoolInfo> builder = new ImmutableMap.Builder<>();
        pools.forEach((poolId, memoryPool) -> builder.put(poolId, memoryPool.getInfo()));
        return builder.build();
    }

    private synchronized boolean isClusterOutOfMemory()
    {
        ClusterMemoryPool reservedPool = pools.get(RESERVED_POOL);
        ClusterMemoryPool generalPool = pools.get(GENERAL_POOL);
        if (reservedPool == null) {
            return generalPool.getBlockedNodes() > 0;
        }
        return reservedPool.getAssignedQueries() > 0 && generalPool.getBlockedNodes() > 0;
    }

    // TODO once the reserved pool is removed we can remove this method. We can also update
    // RemoteNodeMemory as we don't need to POST anything.
    private synchronized MemoryPoolAssignmentsRequest updateAssignments(Iterable<QueryExecution> queries)
    {
        ClusterMemoryPool reservedPool = pools.get(RESERVED_POOL);
        ClusterMemoryPool generalPool = pools.get(GENERAL_POOL);
        verify(generalPool != null, "generalPool is null");
        verify(reservedPool != null, "reservedPool is null");
        long version = memoryPoolAssignmentsVersion.incrementAndGet();
        // Check that all previous assignments have propagated to the visible nodes. This doesn't account for temporary network issues,
        // and is more of a safety check than a guarantee
        if (allAssignmentsHavePropagated(queries)) {
            if (reservedPool.getAssignedQueries() == 0 && generalPool.getBlockedNodes() > 0) {
                QueryExecution biggestQuery = null;
                long maxMemory = -1;
                for (QueryExecution queryExecution : queries) {
                    if (resourceOvercommit(queryExecution.getSession())) {
                        // Don't promote queries that requested resource overcommit to the reserved pool,
                        // since their memory usage is unbounded.
                        continue;
                    }

                    long bytesUsed = getQueryMemoryReservation(queryExecution);
                    if (bytesUsed > maxMemory) {
                        biggestQuery = queryExecution;
                        maxMemory = bytesUsed;
                    }
                }
                if (biggestQuery != null) {
                    log.info("Moving query %s to the reserved pool", biggestQuery.getQueryId());
                    biggestQuery.setMemoryPool(new VersionedMemoryPoolId(RESERVED_POOL, version));
                }
            }
        }

        ImmutableList.Builder<MemoryPoolAssignment> assignments = ImmutableList.builder();
        for (QueryExecution queryExecution : queries) {
            assignments.add(new MemoryPoolAssignment(queryExecution.getQueryId(), queryExecution.getMemoryPool().getId()));
        }
        return new MemoryPoolAssignmentsRequest(coordinatorId, version, assignments.build());
    }

    private QueryMemoryInfo createQueryMemoryInfo(QueryExecution query)
    {
        return new QueryMemoryInfo(query.getQueryId(), query.getMemoryPool().getId(), query.getTotalMemoryReservation().toBytes());
    }

    private long getQueryMemoryReservation(QueryExecution query)
    {
        return query.getTotalMemoryReservation().toBytes();
    }

    private synchronized boolean allAssignmentsHavePropagated(Iterable<QueryExecution> queries)
    {
        if (nodes.isEmpty()) {
            // Assignments can't have propagated, if there are no visible nodes.
            return false;
        }
        long newestAssignment = ImmutableList.copyOf(queries).stream()
                .map(QueryExecution::getMemoryPool)
                .mapToLong(VersionedMemoryPoolId::getVersion)
                .min()
                .orElse(-1);

        long mostOutOfDateNode = nodes.values().stream()
                .mapToLong(RemoteNodeMemory::getCurrentAssignmentVersion)
                .min()
                .orElse(Long.MAX_VALUE);

        return newestAssignment <= mostOutOfDateNode;
    }

    private synchronized void updateNodes(MemoryPoolAssignmentsRequest assignments)
    {
        ImmutableSet.Builder<InternalNode> builder = ImmutableSet.builder();
        Set<InternalNode> aliveNodes = builder
                .addAll(nodeManager.getNodes(ACTIVE))
                .addAll(nodeManager.getNodes(ISOLATING))
                .addAll(nodeManager.getNodes(ISOLATED))
                .addAll(nodeManager.getNodes(SHUTTING_DOWN))
                .build();

        ImmutableSet<String> aliveNodeIds = aliveNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableSet());

        // Remove nodes that don't exist anymore
        // Make a copy to materialize the set difference
        Set<String> deadNodes = ImmutableSet.copyOf(difference(nodes.keySet(), aliveNodeIds));
        nodes.keySet().removeAll(deadNodes);

        // Add new nodes
        for (InternalNode node : aliveNodes) {
            if (!nodes.containsKey(node.getNodeIdentifier()) && shouldIncludeNode(node)) {
                nodes.put(node.getInternalUri().toString(), new RemoteNodeMemory(node, httpClient, memoryInfoCodec, assignmentsRequestCodec, locationFactory.createMemoryInfoLocation(node), isBinaryEncoding));
                allNodes.put(node.getInternalUri().toString(), new RemoteNodeMemory(node, httpClient, memoryInfoCodec, assignmentsRequestCodec, locationFactory.createMemoryInfoLocation(node), isBinaryEncoding));
            }
        }

        // Schedule refresh
        for (RemoteNodeMemory node : nodes.values()) {
            node.asyncRefresh(assignments);
        }

        // Schedule All refresh
        for (RemoteNodeMemory node : allNodes.values()) {
            node.asyncRefresh(assignments);
        }
    }

    private boolean shouldIncludeNode(InternalNode node)
    {
        // If work isn't scheduled on the coordinator (the current node) there is no point
        // in polling or updating (when moving queries to the reserved pool) its memory pools
        return node.isWorker();
    }

    private synchronized void updatePools(Map<MemoryPoolId, Integer> queryCounts)
    {
        // Update view of cluster memory and pools
        List<MemoryInfo> nodeMemoryInfos = nodes.values().stream()
                .map(RemoteNodeMemory::getInfo)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        long totalProcessors = nodeMemoryInfos.stream()
                .mapToLong(MemoryInfo::getAvailableProcessors)
                .sum();
        totalAvailableProcessors.set(totalProcessors);

        long totalClusterMemory = nodeMemoryInfos.stream()
                .map(MemoryInfo::getTotalNodeMemory)
                .mapToLong(DataSize::toBytes)
                .sum();
        clusterMemoryBytes.set(totalClusterMemory);

        for (ClusterMemoryPool pool : pools.values()) {
            pool.update(nodeMemoryInfos, queryCounts.getOrDefault(pool.getId(), 0));
            if (changeListeners.containsKey(pool.getId())) {
                MemoryPoolInfo info = pool.getInfo();
                for (Consumer<MemoryPoolInfo> listener : changeListeners.get(pool.getId())) {
                    listenerExecutor.execute(() -> listener.accept(info));
                }
            }
        }
    }

    public synchronized Map<String, Optional<MemoryInfo>> getWorkerMemoryInfo()
    {
        Map<String, Optional<MemoryInfo>> memoryInfo = new HashMap<>();
        for (Entry<String, RemoteNodeMemory> entry : nodes.entrySet()) {
            // workerId is of the form "node_identifier [node_host] role"
            InternalNode node = entry.getValue().getNode();
            String role = node.isCoordinator() ? (node.isWorker() ? "Coordinator & Worker" : "Coordinator") :
                    "Worker";
            String workerId = entry.getKey() + " [" + entry.getValue().getNode().getHost() + "] " + role;
            memoryInfo.put(workerId, entry.getValue().getInfo());
        }
        return memoryInfo;
    }

    public synchronized Long getUsedMemory()
    {
        Long usedMemory = 0L;
        for (Entry<String, RemoteNodeMemory> entry : nodes.entrySet()) {
            MemoryInfo memoryInfo = entry.getValue().getInfo().orElse(new MemoryInfo(0, 0, 0, new DataSize(0,
                    DataSize.Unit.BYTE), new HashMap<>()));
            Map<MemoryPoolId, MemoryPoolInfo> memoryPools = memoryInfo.getPools();
            MemoryPoolId general = new MemoryPoolId("general");
            MemoryPoolId reserved = new MemoryPoolId("reserved");
            if (memoryPools.containsKey(general)) {
                usedMemory += memoryPools.get(general).getReservedBytes();
            }
            if (memoryPools.containsKey(reserved)) {
                usedMemory += memoryPools.get(reserved).getReservedBytes();
            }
        }
        return usedMemory;
    }

    public synchronized Map<String, JsonNode> getWorkerMemoryAndStateInfo()
    {
        Map<String, JsonNode> memoryInfo = new LinkedHashMap<>();
        for (Entry<String, RemoteNodeMemory> entry : allNodes.entrySet()) {
            // workerId is of the form "node_identifier [node_host] role"
            InternalNode node = entry.getValue().getNode();
            StringBuilder role = new StringBuilder(node.isCoordinator() ? (node.isWorker() ? "Coordinator & Worker" : "Coordinator") :
                    "Worker");
            role = new StringBuilder("\"" + role + "\"");
            String workerId = entry.getValue().getNode().getInternalUri().toString();
            String stateTemp = "\"" + NodeState.DISCONNECTION + "\"";
            if (nodes.containsKey(entry.getKey())) {
                stateTemp = "\"" + ACTIVE + "\"";
            }
            String state = stateTemp.substring(0, 2).toUpperCase(Locale.ENGLISH) + stateTemp.substring(2).toLowerCase(Locale.ENGLISH);
            String id = "\"" + node.getNodeIdentifier() + "\"";
            JsonNode jsonNode = null;
            try {
                MemoryInfo activeInfo = new MemoryInfo(0, 0, 0, new DataSize(0,
                        DataSize.Unit.BYTE), new HashMap<>());
                MemoryInfo info = entry.getValue().getInfo().orElse(activeInfo);
                String memoryInfoJson = new ObjectMapper().writeValueAsString(info);
                StringBuilder memoryAndStateInfo = new StringBuilder(memoryInfoJson).insert(memoryInfoJson.length() - 1, ",\"state\":" + state + ",\"id\":" + id + ",\"role\":" + role);
                jsonNode = new ObjectMapper().readTree(memoryAndStateInfo.toString());
            }
            catch (JsonProcessingException e) {
                log.error("MemoryInfo parsing error:" + e.getMessage());
            }
            memoryInfo.put(workerId, jsonNode);
        }
        return memoryInfo;
    }

    @PreDestroy
    public synchronized void destroy()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            for (ClusterMemoryPool pool : pools.values()) {
                closer.register(() -> exporter.unexport(generatedNameOf(ClusterMemoryPool.class, pool.getId().toString())));
            }
            closer.register(listenerExecutor::shutdownNow);
        }
    }

    @Managed
    public long getTotalAvailableProcessors()
    {
        return totalAvailableProcessors.get();
    }

    @Managed
    public int getNumberOfLeakedQueries()
    {
        return memoryLeakDetector.getNumberOfLeakedQueries();
    }

    @Managed
    public long getClusterUserMemoryReservation()
    {
        return clusterUserMemoryReservation.get();
    }

    @Managed
    public long getClusterTotalMemoryReservation()
    {
        return clusterTotalMemoryReservation.get();
    }

    @Managed
    public long getClusterMemoryBytes()
    {
        return clusterMemoryBytes.get();
    }

    @Managed
    public long getQueriesKilledDueToOutOfMemory()
    {
        return queriesKilledDueToOutOfMemory.get();
    }

    public void killLocalQuery(QueryExecution query)
    {
        List<MemoryInfo> nodeMemoryInfos = nodes.values().stream()
                .map(RemoteNodeMemory::getInfo)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        // See comments in  isLastKilledQueryGone for why chosenQuery might be absent.
        query.fail(new PrestoException(CLUSTER_OUT_OF_MEMORY, "Query killed because the cluster is out of memory. Please try again in a few minutes."));
        queriesKilledDueToOutOfMemory.incrementAndGet();
        lastKilledQuery = query.getQueryId();
        logQueryKill(query.getQueryId(), nodeMemoryInfos);
    }
}
