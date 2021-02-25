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
package io.prestosql.server.testing;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceSelectorManager;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.dynamicfilter.DynamicFilterListener;
import io.prestosql.eventlistener.EventListenerManager;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.SqlQueryManager;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.TaskManager;
import io.prestosql.execution.resourcegroups.InternalResourceGroupManager;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.memory.ClusterMemoryManager;
import io.prestosql.memory.LocalMemoryManager;
import io.prestosql.metadata.AllNodes;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.protocol.SmileModule;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AccessControlManager;
import io.prestosql.security.PasswordSecurityModule;
import io.prestosql.server.NodeStateChangeHandler;
import io.prestosql.server.PluginManager;
import io.prestosql.server.ServerConfig;
import io.prestosql.server.ServerMainModule;
import io.prestosql.server.ShutdownAction;
import io.prestosql.server.security.ServerSecurityModule;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.split.PageSourceManager;
import io.prestosql.split.SplitManager;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.sql.planner.ConnectorPlanOptimizerManager;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.Plan;
import io.prestosql.statestore.EmbeddedStateStoreLauncher;
import io.prestosql.statestore.StateStoreLauncher;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.statestore.listener.StateStoreListenerManager;
import io.prestosql.testing.ProcedureTester;
import io.prestosql.testing.TestingAccessControlManager;
import io.prestosql.testing.TestingEventListenerManager;
import io.prestosql.testing.TestingWarningCollectorModule;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.utils.HetuConfig;
import org.weakref.jmx.guice.MBeanModule;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.prestosql.utils.DynamicFilterUtils.MERGED_DYNAMIC_FILTERS;
import static java.lang.Integer.parseInt;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.isDirectory;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestingPrestoServer
        implements Closeable
{
    private final Injector injector;
    private final Path baseDataDir;
    private final boolean preserveData;
    private final LifeCycleManager lifeCycleManager;
    private final PluginManager pluginManager;
    private final ConnectorManager connectorManager;
    private final TestingHttpServer server;
    private final CatalogManager catalogManager;
    private final HetuMetaStoreManager hetuMetaStoreManager;
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final StatsCalculator statsCalculator;
    private final TestingAccessControlManager accessControl;
    private final ProcedureTester procedureTester;
    private final Optional<InternalResourceGroupManager<?>> resourceGroupManager;
    private final SplitManager splitManager;
    private final HeuristicIndexerManager heuristicIndexerManager;
    private final PageSourceManager pageSourceManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final ConnectorPlanOptimizerManager planOptimizerManager;
    private final ClusterMemoryManager clusterMemoryManager;
    private final LocalMemoryManager localMemoryManager;
    private final InternalNodeManager nodeManager;
    private final ServiceSelectorManager serviceSelectorManager;
    private final Announcer announcer;
    private final DispatchManager dispatchManager;
    private final SqlQueryManager queryManager;
    private final TaskManager taskManager;
    private final NodeStateChangeHandler nodeStateChangeHandler;
    private final ShutdownAction shutdownAction;
    private final boolean coordinator;
    private StateStoreProvider stateStoreProvider;

    public static class TestShutdownAction
            implements ShutdownAction
    {
        private final CountDownLatch shutdownCalled = new CountDownLatch(1);

        @GuardedBy("this")
        private boolean isShutdown;

        @Override
        public synchronized void onShutdown()
        {
            isShutdown = true;
            shutdownCalled.countDown();
        }

        public void waitForShutdownComplete(long millis)
                throws InterruptedException
        {
            shutdownCalled.await(millis, MILLISECONDS);
        }

        public synchronized boolean isShutdown()
        {
            return isShutdown;
        }
    }

    public TestingPrestoServer()
            throws Exception
    {
        this(ImmutableList.of());
    }

    public TestingPrestoServer(List<Module> additionalModules)
            throws Exception
    {
        this(true, ImmutableMap.of(), null, null, new SqlParserOptions(), additionalModules, Optional.empty());
    }

    public TestingPrestoServer(Map<String, String> properties)
            throws Exception
    {
        this(true, properties, null, null, new SqlParserOptions(), ImmutableList.of(), Optional.empty());
    }

    public TestingPrestoServer(
            boolean coordinator,
            Map<String, String> properties,
            String environment,
            URI discoveryUri,
            SqlParserOptions parserOptions,
            List<Module> additionalModules,
            Optional<Path> baseDataDir)
            throws Exception
    {
        this.coordinator = coordinator;

        this.baseDataDir = baseDataDir.orElseGet(TestingPrestoServer::tempDirectory);
        this.preserveData = baseDataDir.isPresent();

        properties = new HashMap<>(properties);
        String coordinatorPort = properties.remove("http-server.http.port");
        if (coordinatorPort == null) {
            coordinatorPort = "0";
        }

        ImmutableMap.Builder<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .putAll(properties)
                .put("coordinator", String.valueOf(coordinator))
                .put("presto.version", "testversion")
                .put("task.concurrency", "4")
                .put("task.max-worker-threads", "4")
                .put("exchange.client-threads", "4");

        if (coordinator) {
            // TODO: enable failure detector
            serverProperties.put("failure-detector.enabled", "false");
        }

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingNodeModule(Optional.ofNullable(environment)))
                .add(new TestingHttpServerModule(parseInt(coordinator ? coordinatorPort : "0")))
                .add(new JsonModule())
                .add(new SmileModule())
                .add(new JaxrsModule())
                .add(new MBeanModule())
                .add(new TestingJmxModule())
                .add(new EventModule())
                .add(new TraceTokenModule())
                .add(new ServerSecurityModule())
                .add(new PasswordSecurityModule())
                .add(new ServerMainModule(parserOptions))
                .add(new TestingWarningCollectorModule())
                .add(binder -> {
                    binder.bind(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControlManager.class).to(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(EventListenerManager.class).to(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControl.class).to(AccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(ShutdownAction.class).to(TestShutdownAction.class).in(Scopes.SINGLETON);
                    binder.bind(NodeStateChangeHandler.class).in(Scopes.SINGLETON);
                    binder.bind(ProcedureTester.class).in(Scopes.SINGLETON);
                });

        if (discoveryUri != null) {
            requireNonNull(environment, "environment required when discoveryUri is present");
            serverProperties.put("discovery.uri", discoveryUri.toString());
            modules.add(new DiscoveryModule());
        }
        else {
            modules.add(new TestingDiscoveryModule());
        }

        modules.addAll(additionalModules);

        Bootstrap app = new Bootstrap(modules.build());

        Map<String, String> optionalProperties = new HashMap<>();
        if (environment != null) {
            optionalProperties.put("node.environment", environment);
        }

        injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(serverProperties.build())
                .setOptionalConfigurationProperties(optionalProperties)
                .quiet()
                .initialize();

        injector.getInstance(Announcer.class).start();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        pluginManager = injector.getInstance(PluginManager.class);

        connectorManager = injector.getInstance(ConnectorManager.class);

        hetuMetaStoreManager = injector.getInstance(HetuMetaStoreManager.class);

        heuristicIndexerManager = injector.getInstance(HeuristicIndexerManager.class);

        server = injector.getInstance(TestingHttpServer.class);
        catalogManager = injector.getInstance(CatalogManager.class);
        transactionManager = injector.getInstance(TransactionManager.class);
        metadata = injector.getInstance(Metadata.class);
        accessControl = injector.getInstance(TestingAccessControlManager.class);
        procedureTester = injector.getInstance(ProcedureTester.class);
        splitManager = injector.getInstance(SplitManager.class);
        pageSourceManager = injector.getInstance(PageSourceManager.class);
        if (coordinator) {
            dispatchManager = injector.getInstance(DispatchManager.class);
            queryManager = (SqlQueryManager) injector.getInstance(QueryManager.class);
            resourceGroupManager = Optional.of(injector.getInstance(InternalResourceGroupManager.class));
            nodePartitioningManager = injector.getInstance(NodePartitioningManager.class);
            planOptimizerManager = injector.getInstance(ConnectorPlanOptimizerManager.class);
            clusterMemoryManager = injector.getInstance(ClusterMemoryManager.class);
            statsCalculator = injector.getInstance(StatsCalculator.class);
        }
        else {
            dispatchManager = null;
            queryManager = null;
            resourceGroupManager = Optional.empty();
            nodePartitioningManager = null;
            planOptimizerManager = null;
            clusterMemoryManager = null;
            statsCalculator = null;
        }
        localMemoryManager = injector.getInstance(LocalMemoryManager.class);
        nodeManager = injector.getInstance(InternalNodeManager.class);
        serviceSelectorManager = injector.getInstance(ServiceSelectorManager.class);
        nodeStateChangeHandler = injector.getInstance(NodeStateChangeHandler.class);
        taskManager = injector.getInstance(TaskManager.class);
        shutdownAction = injector.getInstance(ShutdownAction.class);
        announcer = injector.getInstance(Announcer.class);

        announcer.forceAnnounce();

        refreshNodes();
    }

    public void loadStateSotre()
    {
        try {
            launchEmbeddedStateStore(injector.getInstance(HetuConfig.class), injector.getInstance(StateStoreLauncher.class));
            stateStoreProvider = injector.getInstance(StateStoreProvider.class);
            stateStoreProvider.loadStateStore();
            // register dynamic filter listener
            registerStateStoreListeners(
                    injector.getInstance(StateStoreListenerManager.class),
                    injector.getInstance(DynamicFilterCacheManager.class),
                    injector.getInstance(ServerConfig.class),
                    injector.getInstance(NodeSchedulerConfig.class));
        }
        catch (Exception e) {
            // ignore
        }
    }

    private static void launchEmbeddedStateStore(HetuConfig config, StateStoreLauncher launcher)
            throws Exception
    {
        // Only launch embedded state store when enabled
        if (config.isEmbeddedStateStoreEnabled()) {
            if (launcher instanceof EmbeddedStateStoreLauncher) {
                Set<String> ips = Sets.newHashSet(Arrays.asList("127.0.0.1"));
                Map<String, String> stateStoreProperties = new HashMap<>();
                stateStoreProperties.put("hazelcast.discovery.port", "5701");
                stateStoreProperties.putIfAbsent("hazelcast.discovery.mode", "tcp-ip");
                ((EmbeddedStateStoreLauncher) launcher).launchStateStore(ips, stateStoreProperties);
            }
            launcher.launchStateStore();
        }
    }

    private static void registerStateStoreListeners(
            StateStoreListenerManager stateStoreListenerManager,
            DynamicFilterCacheManager dynamicFilterCacheManager,
            ServerConfig serverConfig,
            NodeSchedulerConfig nodeSchedulerConfig)
    {
        if (serverConfig.isCoordinator() && !nodeSchedulerConfig.isIncludeCoordinator()) {
            return;
        }
        stateStoreListenerManager.addStateStoreListener(new DynamicFilterListener(dynamicFilterCacheManager), MERGED_DYNAMIC_FILTERS);
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            if (lifeCycleManager != null) {
                lifeCycleManager.stop();
            }
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally {
            if (isDirectory(baseDataDir) && !preserveData) {
                deleteRecursively(baseDataDir, ALLOW_INSECURE);
            }
        }
    }

    public void installPlugin(Plugin plugin)
    {
        pluginManager.installPlugin(plugin);
    }

    public DispatchManager getDispatchManager()
    {
        return dispatchManager;
    }

    public QueryManager getQueryManager()
    {
        return queryManager;
    }

    public Plan getQueryPlan(QueryId queryId)
    {
        return queryManager.getQueryPlan(queryId);
    }

    public void addFinalQueryInfoListener(QueryId queryId, StateChangeListener<QueryInfo> stateChangeListener)
    {
        queryManager.addFinalQueryInfoListener(queryId, stateChangeListener);
    }

    public void loadMetastore()
            throws IOException
    {
        hetuMetaStoreManager.loadHetuMetastore(new FileSystemClientManager());
    }

    public void loadMetastore(Map<String, String> config)
            throws IOException
    {
        hetuMetaStoreManager.loadHetuMetastore(new FileSystemClientManager(), config);
    }

    public CatalogName createCatalog(String catalogName, String connectorName)
    {
        return createCatalog(catalogName, connectorName, ImmutableMap.of());
    }

    public CatalogName createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        CatalogName catalog = connectorManager.createConnection(catalogName, connectorName, properties);
        updateConnectorIdAnnouncement(announcer, catalog, nodeManager);
        return catalog;
    }

    /**
     * Hetu DC Connector requires this method to delete all the catalog internals for given catalogName
     *
     * @param catalogName the catalog name
     */
    public void deleteCatalog(String catalogName)
    {
        connectorManager.dropConnection(catalogName);
    }

    /**
     * Hetu DC Connector requires this method to create DC catalogs in the testing server for unit test.
     *
     * @param catalogName the catalog name
     * @param connectorName connector name
     * @param properties catalog properties
     * @return the set of dynamic catalog names
     */
    public Set<CatalogName> createDCCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        Set<CatalogName> catalogs = new HashSet<>();
        CatalogName catalog = connectorManager.createConnection(catalogName, connectorName, properties);
        // This is a hack which can be removed when Presto removes their hack in line# 497
        for (Catalog cat : catalogManager.getCatalogs()) {
            String name = cat.getCatalogName();
            if (name.startsWith(catalogName + ".")) {
                updateConnectorIdAnnouncement(announcer, new CatalogName(cat.getCatalogName()), nodeManager);
                catalogs.add(new CatalogName(cat.getCatalogName()));
            }
        }
        return catalogs;
    }

    public Path getBaseDataDir()
    {
        return baseDataDir;
    }

    public URI getBaseUrl()
    {
        return server.getBaseUrl();
    }

    public URI resolve(String path)
    {
        return server.getBaseUrl().resolve(path);
    }

    public HostAndPort getAddress()
    {
        return HostAndPort.fromParts(getBaseUrl().getHost(), getBaseUrl().getPort());
    }

    public HostAndPort getHttpsAddress()
    {
        URI httpsUri = server.getHttpServerInfo().getHttpsUri();
        return HostAndPort.fromParts(httpsUri.getHost(), httpsUri.getPort());
    }

    public CatalogManager getCatalogManager()
    {
        return catalogManager;
    }

    public TransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public StatsCalculator getStatsCalculator()
    {
        checkState(coordinator, "not a coordinator");
        return statsCalculator;
    }

    public TestingAccessControlManager getAccessControl()
    {
        return accessControl;
    }

    public ProcedureTester getProcedureTester()
    {
        return procedureTester;
    }

    public SplitManager getSplitManager()
    {
        return splitManager;
    }

    public PageSourceManager getPageSourceManager()
    {
        return pageSourceManager;
    }

    public Optional<InternalResourceGroupManager<?>> getResourceGroupManager()
    {
        return resourceGroupManager;
    }

    public NodePartitioningManager getNodePartitioningManager()
    {
        return nodePartitioningManager;
    }

    public ConnectorPlanOptimizerManager getPlanOptimizerManager()
    {
        return planOptimizerManager;
    }

    public LocalMemoryManager getLocalMemoryManager()
    {
        return localMemoryManager;
    }

    public ClusterMemoryManager getClusterMemoryManager()
    {
        checkState(coordinator, "not a coordinator");
        return clusterMemoryManager;
    }

    public HeuristicIndexerManager getHeuristicIndexerManager()
    {
        return this.heuristicIndexerManager;
    }

    public NodeStateChangeHandler getNodeStateChangeHandler()
    {
        return nodeStateChangeHandler;
    }

    public TaskManager getTaskManager()
    {
        return taskManager;
    }

    public ShutdownAction getShutdownAction()
    {
        return shutdownAction;
    }

    public boolean isCoordinator()
    {
        return coordinator;
    }

    public final AllNodes refreshNodes()
    {
        serviceSelectorManager.forceRefresh();
        nodeManager.refreshNodes();
        return nodeManager.getAllNodes();
    }

    public Set<InternalNode> getActiveNodesWithConnector(CatalogName catalogName)
    {
        return nodeManager.getActiveConnectorNodes(catalogName);
    }

    public <T> T getInstance(Key<T> key)
    {
        return injector.getInstance(key);
    }

    private static void updateConnectorIdAnnouncement(Announcer announcer, CatalogName catalogName, InternalNodeManager nodeManager)
    {
        //
        // This code was copied from PrestoServer, and is a hack that should be removed when the connectorId property is removed
        //

        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // update connectorIds property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.add(catalogName.toString());
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();

        nodeManager.refreshNodes();
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new RuntimeException("Presto announcement not found: " + announcements);
    }

    private static Path tempDirectory()
    {
        try {
            return createTempDirectory("PrestoTest");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
