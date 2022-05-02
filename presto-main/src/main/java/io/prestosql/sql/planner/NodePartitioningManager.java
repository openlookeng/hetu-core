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

package io.prestosql.sql.planner;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.prestosql.Session;
import io.prestosql.execution.scheduler.BucketNodeMap;
import io.prestosql.execution.scheduler.FixedBucketNodeMap;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.NodeSelector;
import io.prestosql.execution.scheduler.group.DynamicBucketNodeMap;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Split;
import io.prestosql.operator.BucketPartitionFunction;
import io.prestosql.operator.PartitionFunction;
import io.prestosql.spi.connector.BucketFunction;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorBucketNodeMap;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.snapshot.SnapshotConfig.calculateTaskCount;
import static io.prestosql.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.prestosql.util.Failures.checkCondition;
import static java.util.Objects.requireNonNull;

public class NodePartitioningManager
{
    private final NodeScheduler nodeScheduler;
    private final ConcurrentMap<CatalogName, ConnectorNodePartitioningProvider> partitioningProviders = new ConcurrentHashMap<>();

    @Inject
    public NodePartitioningManager(NodeScheduler nodeScheduler)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
    }

    public void addPartitioningProvider(CatalogName catalogName, ConnectorNodePartitioningProvider nodePartitioningProvider)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        checkArgument(partitioningProviders.putIfAbsent(catalogName, nodePartitioningProvider) == null,
                "NodePartitioningProvider for connector '%s' is already registered", catalogName);
    }

    public void removePartitioningProvider(CatalogName catalogName)
    {
        partitioningProviders.remove(catalogName);
    }

    public PartitionFunction getPartitionFunction(
            Session session,
            PartitioningScheme partitioningScheme,
            List<Type> partitionChannelTypes)
    {
        Optional<int[]> bucketToPartition = partitioningScheme.getBucketToPartition();
        checkArgument(bucketToPartition.isPresent(), "Bucket to partition must be set before a partition function can be created");

        PartitioningHandle partitioningHandle = partitioningScheme.getPartitioning().getHandle();
        BucketFunction bucketFunction;
        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle) {
            checkArgument(partitioningScheme.getBucketToPartition().isPresent(), "Bucket to partition must be set before a partition function can be created");

            return ((SystemPartitioningHandle) partitioningHandle.getConnectorHandle()).getPartitionFunction(
                    partitionChannelTypes,
                    partitioningScheme.getHashColumn().isPresent(),
                    partitioningScheme.getBucketToPartition().get());
        }
        CatalogName catalogName = partitioningHandle.getConnectorId().get();
        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviders.get(catalogName);
        checkArgument(partitioningProvider != null, "No partitioning provider for connector %s", catalogName);

        bucketFunction = partitioningProvider.getBucketFunction(
                partitioningHandle.getTransactionHandle().orElse(null),
                session.toConnectorSession(catalogName),
                partitioningHandle.getConnectorHandle(),
                partitionChannelTypes,
                bucketToPartition.get().length);

        checkArgument(bucketFunction != null, "No function %s", partitioningHandle);
        return new BucketPartitionFunction(bucketFunction, partitioningScheme.getBucketToPartition().get());
    }

    public List<ConnectorPartitionHandle> listPartitionHandles(
            Session session,
            PartitioningHandle partitioningHandle)
    {
        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviders.get(partitioningHandle.getConnectorId().get());
        return partitioningProvider.listPartitionHandles(
                partitioningHandle.getTransactionHandle().orElse(null),
                session.toConnectorSession(partitioningHandle.getConnectorId().get()),
                partitioningHandle.getConnectorHandle());
    }

    public NodePartitionMap getNodePartitioningMap(Session session, PartitioningHandle partitioningHandle, boolean isSnapshotEnabled, Integer nodeCount)
    {
        requireNonNull(session, "session is null");
        requireNonNull(partitioningHandle, "partitioningHandle is null");

        if (partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle) {
            return ((SystemPartitioningHandle) partitioningHandle.getConnectorHandle()).getNodePartitionMap(session, nodeScheduler, isSnapshotEnabled, nodeCount);
        }

        CatalogName catalogName = partitioningHandle.getConnectorId()
                .orElseThrow(() -> new IllegalArgumentException("No connector ID for partitioning handle: " + partitioningHandle));
        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviders.get(catalogName);
        checkArgument(partitioningProvider != null, "No partitioning provider for connector %s", catalogName);

        ConnectorBucketNodeMap connectorBucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle);
        // safety check for crazy partitioning
        checkArgument(connectorBucketNodeMap.getBucketCount() < 1_000_000, "Too many buckets in partitioning: %s", connectorBucketNodeMap.getBucketCount());

        List<InternalNode> bucketToNode;
        if (connectorBucketNodeMap.hasFixedMapping()) {
            bucketToNode = getFixedMapping(connectorBucketNodeMap);
        }
        else {
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(catalogName, false, null);
            List<InternalNode> nodes;
            if (isSnapshotEnabled) {
                Integer count = nodeCount;
                if (count == null) {
                    // Initial schedule: reserve some nodes
                    count = calculateTaskCount(nodeSelector.selectableNodeCount());
                }
                nodes = nodeSelector.selectRandomNodes(count);
                checkCondition(nodes.size() == count, NO_NODES_AVAILABLE, "Snapshot: not enough worker nodes to resume expected number of tasks: " + count);
            }
            else {
                nodes = nodeSelector.allNodes();
            }
            bucketToNode = createArbitraryBucketToNode(
                    nodes,
                    connectorBucketNodeMap.getBucketCount());
        }

        int[] bucketToPartition = new int[connectorBucketNodeMap.getBucketCount()];
        BiMap<InternalNode, Integer> nodeToPartition = HashBiMap.create();
        int nextPartitionId = 0;
        for (int bucket = 0; bucket < bucketToNode.size(); bucket++) {
            InternalNode node = bucketToNode.get(bucket);
            Integer partitionId = nodeToPartition.get(node);
            if (partitionId == null) {
                partitionId = nextPartitionId++;
                nodeToPartition.put(node, partitionId);
            }
            bucketToPartition[bucket] = partitionId;
        }

        List<InternalNode> partitionToNode = IntStream.range(0, nodeToPartition.size())
                .mapToObj(partitionId -> nodeToPartition.inverse().get(partitionId))
                .collect(toImmutableList());

        return new NodePartitionMap(partitionToNode, bucketToPartition, getSplitToBucket(session, partitioningHandle));
    }

    public BucketNodeMap getBucketNodeMap(Session session, PartitioningHandle partitioningHandle, boolean preferDynamic)
    {
        return getBucketNodeMap(session, partitioningHandle, preferDynamic, null);
    }

    // Snapshot: When nodeCount > 0, it indicates how many nodes must be allocated
    public BucketNodeMap getBucketNodeMap(Session session, PartitioningHandle partitioningHandle, boolean preferDynamic, List<InternalNode> inputNodes)
    {
        List<InternalNode> nodes = inputNodes;
        ConnectorBucketNodeMap connectorBucketNodeMap = getConnectorBucketNodeMap(session, partitioningHandle);

        // Snapshot: this is never true for all implementations, so no change
        if (connectorBucketNodeMap.hasFixedMapping()) {
            return new FixedBucketNodeMap(getSplitToBucket(session, partitioningHandle), getFixedMapping(connectorBucketNodeMap));
        }

        // Snapshot: this doesn't have pointers to nodes or number of nodes initially, so no change
        if (preferDynamic) {
            return new DynamicBucketNodeMap(getSplitToBucket(session, partitioningHandle), connectorBucketNodeMap.getBucketCount());
        }

        // Snapshot: only need to make sure this use the supplied node list if any
        if (nodes == null) {
            nodes = nodeScheduler.createNodeSelector(partitioningHandle.getConnectorId().get(), false, null).allNodes();
        }
        return new FixedBucketNodeMap(
                getSplitToBucket(session, partitioningHandle),
                createArbitraryBucketToNode(
                        new ArrayList<>(nodes),
                        connectorBucketNodeMap.getBucketCount()));
    }

    private static List<InternalNode> getFixedMapping(ConnectorBucketNodeMap connectorBucketNodeMap)
    {
        return connectorBucketNodeMap.getFixedMapping().stream()
                .map(InternalNode.class::cast)
                .collect(toImmutableList());
    }

    private ConnectorBucketNodeMap getConnectorBucketNodeMap(Session session, PartitioningHandle partitioningHandle)
    {
        checkArgument(!(partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle));

        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviders.get(partitioningHandle.getConnectorId().get());
        checkArgument(partitioningProvider != null, "No partitioning provider for connector %s", partitioningHandle.getConnectorId().get());

        ConnectorBucketNodeMap connectorBucketNodeMap = partitioningProvider.getBucketNodeMap(
                partitioningHandle.getTransactionHandle().orElse(null),
                session.toConnectorSession(),
                partitioningHandle.getConnectorHandle());

        checkArgument(connectorBucketNodeMap != null, "No partition map %s", partitioningHandle);
        return connectorBucketNodeMap;
    }

    private ToIntFunction<Split> getSplitToBucket(Session session, PartitioningHandle partitioningHandle)
    {
        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviders.get(partitioningHandle.getConnectorId().get());
        checkArgument(partitioningProvider != null, "No partitioning provider for connector %s", partitioningHandle.getConnectorId().get());

        ToIntFunction<ConnectorSplit> splitBucketFunction = partitioningProvider.getSplitBucketFunction(
                partitioningHandle.getTransactionHandle().orElse(null),
                session.toConnectorSession(),
                partitioningHandle.getConnectorHandle());
        checkArgument(splitBucketFunction != null, "No partitioning %s", partitioningHandle);

        return split -> {
            int bucket;
            if (split.getConnectorSplit().isSplitEmpty()) {
                bucket = split.getLifespan().isTaskWide() ? 0 : split.getLifespan().getId();
            }
            else {
                bucket = splitBucketFunction.applyAsInt(split.getConnectorSplit());
            }
            if (!split.getLifespan().isTaskWide()) {
                checkArgument(split.getLifespan().getId() == bucket);
            }
            return bucket;
        };
    }

    private static List<InternalNode> createArbitraryBucketToNode(List<InternalNode> nodes, int bucketCount)
    {
        return cyclingShuffledStream(nodes)
                .limit(bucketCount)
                .collect(toImmutableList());
    }

    private static <T> Stream<T> cyclingShuffledStream(Collection<T> collection)
    {
        List<T> list = new ArrayList<>(collection);
        Collections.shuffle(list);
        return Stream.generate(() -> list).flatMap(List::stream);
    }
}
