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
package io.prestosql.metadata;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.prestosql.client.NodeVersion;
import io.prestosql.connector.system.GlobalSystemConnector;
import io.prestosql.failuredetector.FailureDetector;
import io.prestosql.server.InternalCommunicationConfig;
import io.prestosql.spi.connector.CatalogName;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.prestosql.connector.DataCenterUtility.isDCCatalog;
import static io.prestosql.metadata.NodeState.ACTIVE;
import static io.prestosql.metadata.NodeState.INACTIVE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

@ThreadSafe
public final class DiscoveryNodeManager
        implements InternalNodeManager
{
    private static final Logger log = Logger.get(DiscoveryNodeManager.class);

    private static final Splitter CONNECTOR_ID_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private final ServiceSelector serviceSelector;
    private final FailureDetector failureDetector;
    private final NodeVersion expectedNodeVersion;
    private final ConcurrentHashMap<String, RemoteNodeState> nodeStates = new ConcurrentHashMap<>();
    private final HttpClient httpClient;
    private final ScheduledExecutorService nodeStateUpdateExecutor;
    private final ExecutorService nodeStateEventExecutor;
    private final boolean httpsRequired;
    private final InternalNode currentNode;
    private final Metadata metadata;

    @GuardedBy("this")
    private SetMultimap<CatalogName, InternalNode> activeNodesByCatalogName;

    @GuardedBy("this")
    private SetMultimap<CatalogName, InternalNode> allNodesByCatalogName;

    @GuardedBy("this")
    private AllNodes allNodes;

    @GuardedBy("this")
    private Set<InternalNode> coordinators;

    @GuardedBy("this")
    private final List<Consumer<AllNodes>> listeners = new ArrayList<>();

    @Inject
    public DiscoveryNodeManager(
            @ServiceType("presto") ServiceSelector serviceSelector,
            NodeInfo nodeInfo,
            FailureDetector failureDetector,
            NodeVersion expectedNodeVersion,
            @ForNodeManager HttpClient httpClient,
            InternalCommunicationConfig internalCommunicationConfig,
            Metadata metadata)
    {
        this.serviceSelector = requireNonNull(serviceSelector, "serviceSelector is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.expectedNodeVersion = requireNonNull(expectedNodeVersion, "expectedNodeVersion is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.nodeStateUpdateExecutor = newSingleThreadScheduledExecutor(threadsNamed("node-state-poller-%s"));
        this.nodeStateEventExecutor = newCachedThreadPool(threadsNamed("node-state-events-%s"));
        this.httpsRequired = internalCommunicationConfig.isHttpsRequired();
        this.metadata = metadata;

        this.currentNode = findCurrentNode(
                serviceSelector.selectAllServices(),
                requireNonNull(nodeInfo, "nodeInfo is null").getNodeId(),
                expectedNodeVersion,
                httpsRequired);

        refreshNodesInternal();
    }

    private static InternalNode findCurrentNode(List<ServiceDescriptor> allServices, String currentNodeId, NodeVersion expectedNodeVersion, boolean httpsRequired)
    {
        for (ServiceDescriptor service : allServices) {
            URI uri = getHttpUri(service, httpsRequired);
            NodeVersion nodeVersion = getNodeVersion(service);
            if (uri != null && nodeVersion != null) {
                InternalNode node = new InternalNode(service.getNodeId(), uri, nodeVersion, isCoordinator(service), isWorker(service));

                if (node.getNodeIdentifier().equals(currentNodeId)) {
                    checkState(
                            node.getNodeVersion().equals(expectedNodeVersion),
                            "INVARIANT: current node version (%s) should be equal to %s",
                            node.getNodeVersion(),
                            expectedNodeVersion);
                    return node;
                }
            }
        }
        throw new IllegalStateException("INVARIANT: current node not returned from service selector");
    }

    @PostConstruct
    public void startPollingNodeStates()
    {
        nodeStateUpdateExecutor.scheduleWithFixedDelay(() -> {
            try {
                pollWorkers();
            }
            catch (Exception e) {
                log.error(e, "Error polling state of nodes");
            }
        }, 5, 5, TimeUnit.SECONDS);
        pollWorkers();
    }

    @Override
    public void refreshWorkerStates()
    {
        failureDetector.waitForServiceStateRefresh();
        pollWorkers();
        AllNodes allNodesOverDiscovery = getAllNodes();
        Set<InternalNode> aliveNodes = ImmutableSet.<InternalNode>builder()
                .addAll(allNodesOverDiscovery.getActiveNodes())
                .addAll(allNodesOverDiscovery.getIsolatingNodes())
                .addAll(allNodesOverDiscovery.getIsolatedNodes())
                .addAll(allNodesOverDiscovery.getShuttingDownNodes())
                .build();

        ImmutableSet<String> aliveNodeIds = aliveNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableSet());

        // Remove nodes that don't exist anymore
        // Make a copy to materialize the set difference
        Set<String> deadNodes = difference(nodeStates.keySet(), aliveNodeIds).immutableCopy();
        nodeStates.keySet().removeAll(deadNodes);
    }

    private void pollWorkers()
    {
        AllNodes allNodesOverDiscovery = getAllNodes();
        Set<InternalNode> aliveNodes = ImmutableSet.<InternalNode>builder()
                .addAll(allNodesOverDiscovery.getActiveNodes())
                .addAll(allNodesOverDiscovery.getIsolatingNodes())
                .addAll(allNodesOverDiscovery.getIsolatedNodes())
                .addAll(allNodesOverDiscovery.getShuttingDownNodes())
                .build();

        ImmutableSet<String> aliveNodeIds = aliveNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableSet());

        // Remove nodes that don't exist anymore
        // Make a copy to materialize the set difference
        Set<String> deadNodes = difference(nodeStates.keySet(), aliveNodeIds).immutableCopy();
        nodeStates.keySet().removeAll(deadNodes);

        // Add new nodes
        for (InternalNode node : aliveNodes) {
            nodeStates.putIfAbsent(node.getNodeIdentifier(),
                    new RemoteNodeState(httpClient, uriBuilderFrom(node.getInternalUri()).appendPath("/v1/info/state").build()));
        }

        // Schedule refresh
        nodeStates.values().forEach(RemoteNodeState::asyncRefresh);

        // update indexes
        refreshNodesInternal();
    }

    @PreDestroy
    public void stop()
    {
        nodeStateUpdateExecutor.shutdownNow();
    }

    @Override
    public void refreshNodes()
    {
        refreshNodesInternal();
    }

    private void putConnectorIdsToMap(ImmutableSetMultimap.Builder<CatalogName, InternalNode> mapBuilder, String connectorIdProperty, InternalNode node)
    {
        String totalConnectorIds = connectorIdProperty;
        if (totalConnectorIds != null) {
            totalConnectorIds = totalConnectorIds.toLowerCase(ENGLISH);
            for (String connectorId : CONNECTOR_ID_SPLITTER.split(totalConnectorIds)) {
                mapBuilder.put(new CatalogName(connectorId), node);
            }
        }

        // always add system connector
        mapBuilder.put(new CatalogName(GlobalSystemConnector.NAME), node);
    }

    private synchronized void refreshNodesInternal()
    {
        // This is currently a blacklist.
        // TODO: make it a whitelist (a failure-detecting service selector) and maybe build in support for injecting this in airlift
        Set<ServiceDescriptor> services = serviceSelector.selectAllServices().stream()
                .filter(service -> !failureDetector.getFailed().contains(service))
                .collect(toImmutableSet());

        ImmutableSet.Builder<InternalNode> activeNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> inactiveNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> shuttingDownNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> coordinatorsBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> isolatingNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> isolatedNodesBuilder = ImmutableSet.builder();
        ImmutableSetMultimap.Builder<CatalogName, InternalNode> byConnectorIdBuilder = ImmutableSetMultimap.builder();
        ImmutableSetMultimap.Builder<CatalogName, InternalNode> byAllConnectorIdBuilder = ImmutableSetMultimap.builder();

        for (ServiceDescriptor service : services) {
            URI uri = getHttpUri(service, httpsRequired);
            NodeVersion nodeVersion = getNodeVersion(service);
            boolean coordinator = isCoordinator(service);
            if (uri != null && nodeVersion != null) {
                InternalNode node = new InternalNode(service.getNodeId(), uri, nodeVersion, coordinator, isWorker(service));
                NodeState nodeState = getNodeState(node);

                switch (nodeState) {
                    case ACTIVE:
                        activeNodesBuilder.add(node);
                        if (coordinator) {
                            coordinatorsBuilder.add(node);
                        }

                        // record available active nodes organized by connector id
                        putConnectorIdsToMap(byConnectorIdBuilder,
                                service.getProperties().get("connectorIds"),
                                node);

                        // record available all nodes organized by connector id
                        putConnectorIdsToMap(byAllConnectorIdBuilder,
                                service.getProperties().get("allConnectorIds"),
                                node);
                        break;
                    case INACTIVE:
                        inactiveNodesBuilder.add(node);
                        break;
                    case ISOLATING:
                        isolatingNodesBuilder.add(node);
                        break;
                    case ISOLATED:
                        isolatedNodesBuilder.add(node);
                        break;
                    case SHUTTING_DOWN:
                        shuttingDownNodesBuilder.add(node);
                        break;
                    default:
                        log.error("Unknown state %s for node %s", nodeState, node);
                }
            }
        }

        if (allNodes != null) {
            // log node that are no longer active (but not shutting down)
            Set<InternalNode> expectedNodes = Sets.union(activeNodesBuilder.build(), shuttingDownNodesBuilder.build());
            expectedNodes = Sets.union(expectedNodes, isolatingNodesBuilder.build());
            expectedNodes = Sets.union(expectedNodes, isolatedNodesBuilder.build());
            SetView<InternalNode> missingNodes = difference(allNodes.getActiveNodes(), expectedNodes);
            for (InternalNode missingNode : missingNodes) {
                log.info("Previously active node is missing: %s (last seen at %s)", missingNode.getNodeIdentifier(), missingNode.getHost());
            }
        }

        // nodes by connector id changes anytime a node adds or removes a connector (note: this is not part of the listener system)
        activeNodesByCatalogName = byConnectorIdBuilder.build();
        allNodesByCatalogName = byAllConnectorIdBuilder.build();

        AllNodes allNodesBuild = new AllNodes(activeNodesBuilder.build(), inactiveNodesBuilder.build(),
                shuttingDownNodesBuilder.build(), coordinatorsBuilder.build(),
                isolatingNodesBuilder.build(), isolatedNodesBuilder.build());
        // only update if all nodes actually changed (note: this does not include the connectors registered with the nodes)
        if (!allNodesBuild.equals(this.allNodes)) {
            // assign allNodes to a local variable for use in the callback below
            this.allNodes = allNodesBuild;
            coordinators = coordinatorsBuilder.build();

            // notify listeners
            List<Consumer<AllNodes>> listenersList = ImmutableList.copyOf(this.listeners);
            nodeStateEventExecutor.submit(() -> listenersList.forEach(listener -> listener.accept(allNodesBuild)));
        }
    }

    private NodeState getNodeState(InternalNode node)
    {
        if (expectedNodeVersion.equals(node.getNodeVersion())) {
            Optional<NodeState> remoteNodeState = nodeStates.containsKey(node.getNodeIdentifier())
                    ? nodeStates.get(node.getNodeIdentifier()).getNodeState()
                    : Optional.empty();

            if (!remoteNodeState.isPresent()) {
                return ACTIVE;
            }

            return remoteNodeState.get();
        }
        else {
            return INACTIVE;
        }
    }

    @Override
    public synchronized AllNodes getAllNodes()
    {
        return allNodes;
    }

    @Managed
    public int getActiveNodeCount()
    {
        return getAllNodes().getActiveNodes().size();
    }

    @Managed
    public int getInactiveNodeCount()
    {
        return getAllNodes().getInactiveNodes().size();
    }

    @Managed
    public int getShuttingDownNodeCount()
    {
        return getAllNodes().getShuttingDownNodes().size();
    }

    @Override
    public Set<InternalNode> getNodes(NodeState state)
    {
        switch (state) {
            case ACTIVE:
                return getAllNodes().getActiveNodes();
            case INACTIVE:
                return getAllNodes().getInactiveNodes();
            case ISOLATING:
                return getAllNodes().getIsolatingNodes();
            case ISOLATED:
                return getAllNodes().getIsolatedNodes();
            case SHUTTING_DOWN:
                return getAllNodes().getShuttingDownNodes();
            default:
                throw new IllegalArgumentException("Unknown node state " + state);
        }
    }

    @Override
    public synchronized Set<InternalNode> getActiveConnectorNodes(CatalogName catalogName)
    {
        if (catalogName.getCatalogName().contains(".")) {
            String fullCatalogName = catalogName.getCatalogName();
            String parentCatalogName = fullCatalogName.substring(0, fullCatalogName.indexOf("."));
            if (isDCCatalog(metadata, parentCatalogName)) {
                return activeNodesByCatalogName.get(new CatalogName(parentCatalogName));
            }
        }
        return activeNodesByCatalogName.get(catalogName);
    }

    @Override
    public synchronized Set<InternalNode> getAllConnectorNodes(CatalogName catalogName)
    {
        return allNodesByCatalogName.get(catalogName);
    }

    @Override
    public InternalNode getCurrentNode()
    {
        return currentNode;
    }

    @Override
    public synchronized Set<InternalNode> getCoordinators()
    {
        return coordinators;
    }

    @Override
    public synchronized void addNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.add(requireNonNull(listener, "listener is null"));
        AllNodes allNodesToListener = this.allNodes;
        nodeStateEventExecutor.submit(() -> listener.accept(allNodesToListener));
    }

    @Override
    public synchronized void removeNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.remove(requireNonNull(listener, "listener is null"));
    }

    private static URI getHttpUri(ServiceDescriptor descriptor, boolean httpsRequired)
    {
        String url = descriptor.getProperties().get(httpsRequired ? "https" : "http");
        if (url != null) {
            try {
                return new URI(url);
            }
            catch (URISyntaxException ignored) {
                // could be ignored
            }
        }
        return null;
    }

    private static NodeVersion getNodeVersion(ServiceDescriptor descriptor)
    {
        String nodeVersion = descriptor.getProperties().get("node_version");
        return nodeVersion == null ? null : new NodeVersion(nodeVersion);
    }

    private static boolean isCoordinator(ServiceDescriptor service)
    {
        return Boolean.parseBoolean(service.getProperties().get("coordinator"));
    }

    private static boolean isWorker(ServiceDescriptor service)
    {
        return Boolean.parseBoolean(service.getProperties().get("worker"));
    }
}
