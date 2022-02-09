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
package io.prestosql.connector;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.prestosql.connector.informationschema.InformationSchemaConnector;
import io.prestosql.connector.system.DelegatingSystemTablesProvider;
import io.prestosql.connector.system.MetadataBasedSystemTablesProvider;
import io.prestosql.connector.system.StaticSystemTablesProvider;
import io.prestosql.connector.system.SystemConnector;
import io.prestosql.connector.system.SystemTablesProvider;
import io.prestosql.cost.ConnectorFilterStatsCalculatorService;
import io.prestosql.cost.FilterStatsCalculator;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.index.IndexManager;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.HandleResolver;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.security.AccessControlManager;
import io.prestosql.server.ServerConfig;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.PrestoTransportException;
import io.prestosql.spi.VersionEmbedder;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorIndexProvider;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorPlanOptimizerProvider;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.function.ExternalFunctionHub;
import io.prestosql.spi.function.SqlInvokedFunction;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.DomainTranslator;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.split.PageSinkManager;
import io.prestosql.split.PageSourceManager;
import io.prestosql.split.RecordPageSourceProvider;
import io.prestosql.split.SplitManager;
import io.prestosql.sql.planner.ConnectorPlanOptimizerManager;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.relational.ConnectorRowExpressionService;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.type.InternalTypeManager;
import io.prestosql.version.EmbedVersion;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.prestosql.metadata.FunctionExtractor.extractExternalFunctions;
import static io.prestosql.spi.HetuConstant.CONNECTION_USER;
import static io.prestosql.spi.HetuConstant.DATA_CENTER_CONNECTOR_NAME;
import static io.prestosql.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static io.prestosql.spi.connector.CatalogName.createInformationSchemaCatalogName;
import static io.prestosql.spi.connector.CatalogName.createSystemTablesCatalogName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ConnectorManager
{
    private static final Logger log = Logger.get(ConnectorManager.class);

    private final HetuMetaStoreManager hetuMetaStoreManager;
    private final MetadataManager metadataManager;
    private final CatalogManager catalogManager;
    private final AccessControlManager accessControlManager;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final IndexManager indexManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final ConnectorPlanOptimizerManager connectorPlanOptimizerManager;

    private final PageSinkManager pageSinkManager;
    private final HandleResolver handleResolver;
    private final InternalNodeManager nodeManager;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final NodeInfo nodeInfo;
    private final VersionEmbedder versionEmbedder;
    private final TransactionManager transactionManager;
    // dataCenterConnectorStore is used to store the Connector objects of the catalogs
    private final CatalogConnectorStore catalogConnectorStore;
    private final HeuristicIndexerManager heuristicIndexerManager;

    @GuardedBy("this")
    private final ConcurrentMap<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final ConcurrentMap<CatalogName, MaterializedConnector> connectors = new ConcurrentHashMap<>();

    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Announcer announcer;
    private final ServerConfig serverConfig;
    private final NodeSchedulerConfig schedulerConfig;

    private final DomainTranslator domainTranslator;
    private final DeterminismEvaluator determinismEvaluator;
    private final FilterStatsCalculator filterStatsCalculator;

    @Inject
    public ConnectorManager(
            HetuMetaStoreManager hetuMetaStoreManager,
            MetadataManager metadataManager,
            CatalogManager catalogManager,
            AccessControlManager accessControlManager,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            IndexManager indexManager,
            NodePartitioningManager nodePartitioningManager,
            ConnectorPlanOptimizerManager connectorPlanOptimizerManager,
            PageSinkManager pageSinkManager,
            HandleResolver handleResolver,
            InternalNodeManager nodeManager,
            NodeInfo nodeInfo,
            EmbedVersion embedVersion,
            PageSorter pageSorter,
            PageIndexerFactory pageIndexerFactory,
            TransactionManager transactionManager,
            CatalogConnectorStore catalogConnectorStore,
            Announcer announcer,
            ServerConfig serverConfig,
            NodeSchedulerConfig schedulerConfig,
            HeuristicIndexerManager heuristicIndexerManager,
            DomainTranslator domainTranslator,
            DeterminismEvaluator determinismEvaluator,
            FilterStatsCalculator filterStatsCalculator)
    {
        this.hetuMetaStoreManager = hetuMetaStoreManager;
        this.metadataManager = metadataManager;
        this.catalogManager = catalogManager;
        this.accessControlManager = accessControlManager;
        this.splitManager = splitManager;
        this.pageSourceManager = pageSourceManager;
        this.indexManager = indexManager;
        this.nodePartitioningManager = nodePartitioningManager;
        this.connectorPlanOptimizerManager = connectorPlanOptimizerManager;
        this.pageSinkManager = pageSinkManager;
        this.handleResolver = handleResolver;
        this.nodeManager = nodeManager;
        this.pageSorter = pageSorter;
        this.pageIndexerFactory = pageIndexerFactory;
        this.nodeInfo = nodeInfo;
        this.versionEmbedder = embedVersion;
        this.transactionManager = transactionManager;
        this.catalogConnectorStore = catalogConnectorStore;
        this.announcer = announcer;
        this.serverConfig = serverConfig;
        this.schedulerConfig = schedulerConfig;
        this.heuristicIndexerManager = heuristicIndexerManager;
        this.domainTranslator = domainTranslator;
        this.determinismEvaluator = determinismEvaluator;
        this.filterStatsCalculator = filterStatsCalculator;
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (stopped.getAndSet(true)) {
            return;
        }

        for (Map.Entry<CatalogName, MaterializedConnector> entry : connectors.entrySet()) {
            Connector connector = entry.getValue().getConnector();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connector.getClass().getClassLoader())) {
                connector.shutdown();
            }
            catch (Throwable t) {
                log.error(t, "Error shutting down connector: %s", entry.getKey());
            }
        }
    }

    /**
     * Hetu DC requires the method to return the connectorFactories of ConnectorManager class
     */
    public synchronized ConcurrentMap<String, ConnectorFactory> getConnectorFactories()
    {
        return connectorFactories;
    }

    /**
     * Hetu DC requires the method to return the stopped of ConnectorManager class
     */
    public synchronized AtomicBoolean getStopped()
    {
        return stopped;
    }

    /**
     * Hetu DC requires the method to return the stopped of ConnectorManager class
     */
    public synchronized ConcurrentMap<CatalogName, MaterializedConnector> getConnectors()
    {
        return connectors;
    }

    public synchronized void addConnectorFactory(ConnectorFactory connectorFactory)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        ConnectorFactory existingConnectorFactory = connectorFactories.putIfAbsent(connectorFactory.getName(), connectorFactory);
        checkArgument(existingConnectorFactory == null, "Connector %s is already registered", connectorFactory.getName());
        handleResolver.addConnectorName(connectorFactory.getName(), connectorFactory.getHandleResolver());
    }

    public synchronized CatalogName createAndCheckConnection(String catalogName, String connectorName, Map<String, String> properties)
    {
        requireNonNull(connectorName, "connectorName is null");
        ConnectorFactory connectorFactory = connectorFactories.get(connectorName);
        checkArgument(connectorFactory != null, "No factory for connector [%s].  Available factories: %s", connectorName, connectorFactories.keySet());
        return createConnection(catalogName, connectorFactory, properties, true);
    }

    public synchronized CatalogName createConnection(String catalogName, String connectorName, Map<String, String> properties)
    {
        requireNonNull(connectorName, "connectorName is null");
        ConnectorFactory connectorFactory = connectorFactories.get(connectorName);
        checkArgument(connectorFactory != null, "No factory for connector [%s].  Available factories: %s", connectorName, connectorFactories.keySet());
        return createConnection(catalogName, connectorFactory, properties, false);
    }

    private synchronized CatalogName createConnection(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties, boolean checkConnection)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(properties, "properties is null");
        requireNonNull(connectorFactory, "connectorFactory is null");
        checkArgument(!catalogManager.getCatalog(catalogName).isPresent(), "A catalog already exists for %s", catalogName);

        CatalogName catalog = new CatalogName(catalogName);
        checkState(!connectors.containsKey(catalog), "A catalog %s already exists", catalog);

        addCatalogConnector(catalog, connectorFactory, properties, checkConnection);

        return catalog;
    }

    /**
     * Hetu requires this method to treat DC Connectors differently from other connectors.
     *
     * @param catalogName the catalog name
     * @param factory the connector factory
     * @param properties catalog properties
     * @param checkConnection check whether connection of catalog is available.
     */
    private synchronized void addCatalogConnector(CatalogName catalogName, ConnectorFactory factory, Map<String, String> properties, boolean checkConnection)
    {
        // create all connectors before adding, so a broken connector does not leave the system half updated
        // Hetu reads the DC Connector properties file and dynamically creates <data-center>.<catalog-name> catalogs for each catalogs in that data center
        Connector catalogConnector = createConnector(catalogName, factory, properties);
        if (DATA_CENTER_CONNECTOR_NAME.equals(factory.getName())) {
            // It registers the Connector and Properties in the DataCenterConnectorStore
            catalogConnectorStore.registerConnectorAndProperties(catalogName, catalogConnector, properties);
            if (checkConnection) {
                // check connection
                String catalog = catalogName.getCatalogName();
                Connector connector = catalogConnectorStore.getCatalogConnector(catalog);
                try {
                    connector.getCatalogs(properties.get(CONNECTION_USER), properties);
                }
                catch (PrestoTransportException e) {
                    throw new PrestoException(REMOTE_TASK_ERROR, "Failed to get catalogs from remote data center.");
                }
            }
        }
        else {
            addCatalogConnector(catalogName, catalogConnector);
        }
    }

    public synchronized void addCatalogConnector(CatalogName catalogName, Connector catalogConnector)
    {
        MaterializedConnector connector = new MaterializedConnector(catalogName, catalogConnector);

        MaterializedConnector informationSchemaConnector = new MaterializedConnector(
                createInformationSchemaCatalogName(catalogName),
                new InformationSchemaConnector(catalogName.getCatalogName(), nodeManager, metadataManager, accessControlManager));

        CatalogName systemId = createSystemTablesCatalogName(catalogName);
        SystemTablesProvider systemTablesProvider;

        if (nodeManager.getCurrentNode().isCoordinator()) {
            systemTablesProvider = new DelegatingSystemTablesProvider(
                    new StaticSystemTablesProvider(connector.getSystemTables()),
                    new MetadataBasedSystemTablesProvider(metadataManager, catalogName.getCatalogName()));
        }
        else {
            systemTablesProvider = new StaticSystemTablesProvider(connector.getSystemTables());
        }

        MaterializedConnector systemConnector = new MaterializedConnector(systemId, new SystemConnector(
                nodeManager,
                systemTablesProvider,
                transactionId -> transactionManager.getConnectorTransaction(transactionId, catalogName)));

        Catalog catalog = new Catalog(
                catalogName.getCatalogName(),
                connector.getCatalogName(),
                connector.getConnector(),
                informationSchemaConnector.getCatalogName(),
                informationSchemaConnector.getConnector(),
                systemConnector.getCatalogName(),
                systemConnector.getConnector());

        try {
            addConnectorInternal(connector);
            addConnectorInternal(informationSchemaConnector);
            addConnectorInternal(systemConnector);
            catalogManager.registerCatalog(catalog);
        }
        catch (Throwable e) {
            catalogManager.removeCatalog(catalog.getCatalogName());
            removeConnectorInternal(systemConnector.getCatalogName());
            removeConnectorInternal(informationSchemaConnector.getCatalogName());
            removeConnectorInternal(connector.getCatalogName());
            throw e;
        }
    }

    private synchronized void addConnectorInternal(MaterializedConnector connector)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        CatalogName catalogName = connector.getCatalogName();
        checkState(!connectors.containsKey(catalogName), "A connector %s already exists", catalogName);
        connectors.put(catalogName, connector);

        connector.getSplitManager()
                .ifPresent(connectorSplitManager -> splitManager.addConnectorSplitManager(catalogName, connectorSplitManager));

        connector.getPageSourceProvider()
                .ifPresent(pageSourceProvider -> pageSourceManager.addConnectorPageSourceProvider(catalogName, pageSourceProvider));

        connector.getPageSinkProvider()
                .ifPresent(pageSinkProvider -> pageSinkManager.addConnectorPageSinkProvider(catalogName, pageSinkProvider));

        connector.getIndexProvider()
                .ifPresent(indexProvider -> indexManager.addIndexProvider(catalogName, indexProvider));

        connector.getPartitioningProvider()
                .ifPresent(partitioningProvider -> nodePartitioningManager.addPartitioningProvider(catalogName, partitioningProvider));

        if (nodeManager.getCurrentNode().isCoordinator()) {
            connector.getPlanOptimizerProvider()
                    .ifPresent(planOptimizerProvider -> connectorPlanOptimizerManager.addPlanOptimizerProvider(catalogName, planOptimizerProvider));
        }

        metadataManager.getProcedureRegistry().addProcedures(catalogName, connector.getProcedures());

        connector.getAccessControl()
                .ifPresent(accessControl -> accessControlManager.addCatalogAccessControl(catalogName, accessControl));

        metadataManager.getTablePropertyManager().addProperties(catalogName, connector.getTableProperties());
        metadataManager.getColumnPropertyManager().addProperties(catalogName, connector.getColumnProperties());
        metadataManager.getSchemaPropertyManager().addProperties(catalogName, connector.getSchemaProperties());
        metadataManager.getAnalyzePropertyManager().addProperties(catalogName, connector.getAnalyzeProperties());
        metadataManager.getSessionPropertyManager().addConnectorSessionProperties(catalogName, connector.getSessionProperties());
    }

    public synchronized void dropConnection(String catalogName)
    {
        requireNonNull(catalogName, "catalogName is null");

        catalogManager.removeCatalog(catalogName).ifPresent(catalog -> {
            // todo wait for all running transactions using the connector to complete before removing the services
            removeConnectorInternal(catalog);
            removeConnectorInternal(createInformationSchemaCatalogName(catalog));
            removeConnectorInternal(createSystemTablesCatalogName(catalog));
        });
    }

    private synchronized void removeConnectorInternal(CatalogName catalogName)
    {
        splitManager.removeConnectorSplitManager(catalogName);
        pageSourceManager.removeConnectorPageSourceProvider(catalogName);
        pageSinkManager.removeConnectorPageSinkProvider(catalogName);
        indexManager.removeIndexProvider(catalogName);
        nodePartitioningManager.removePartitioningProvider(catalogName);
        metadataManager.getProcedureRegistry().removeProcedures(catalogName);
        accessControlManager.removeCatalogAccessControl(catalogName);
        metadataManager.getTablePropertyManager().removeProperties(catalogName);
        metadataManager.getColumnPropertyManager().removeProperties(catalogName);
        metadataManager.getSchemaPropertyManager().removeProperties(catalogName);
        metadataManager.getAnalyzePropertyManager().removeProperties(catalogName);
        metadataManager.getSessionPropertyManager().removeConnectorSessionProperties(catalogName);
        connectorPlanOptimizerManager.removePlanOptimizerProvider(catalogName);

        MaterializedConnector materializedConnector = connectors.remove(catalogName);
        if (materializedConnector != null) {
            Connector connector = materializedConnector.getConnector();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connector.getClass().getClassLoader())) {
                connector.shutdown();
            }
            catch (Throwable t) {
                log.error(t, "Error shutting down connector: %s", catalogName);
            }
        }
    }

    public Connector createConnector(CatalogName catalogName, ConnectorFactory factory, Map<String, String> properties)
    {
        ConnectorContext context = new ConnectorContextInstance(
                new ConnectorAwareNodeManager(nodeManager, nodeInfo.getEnvironment(), catalogName),
                versionEmbedder,
                new InternalTypeManager(metadataManager.getFunctionAndTypeManager()),
                pageSorter,
                pageIndexerFactory,
                hetuMetaStoreManager.getHetuMetastore(),
                heuristicIndexerManager.getIndexClient(),
                new ConnectorRowExpressionService(domainTranslator, determinismEvaluator),
                metadataManager.getFunctionAndTypeManager(),
                new FunctionResolution(metadataManager.getFunctionAndTypeManager()),
                metadataManager.getFunctionAndTypeManager().getBlockEncodingSerde(),
                new ConnectorFilterStatsCalculatorService(filterStatsCalculator));

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            Connector connector = factory.create(catalogName.getCatalogName(), properties, context);
            registerExternalFunctions(connector.getExternalFunctionHub(), metadataManager.getFunctionAndTypeManager());
            return connector;
        }
    }

    private static void registerExternalFunctions(Optional<ExternalFunctionHub> externalFunctionHub, FunctionAndTypeManager functionAndTypeManager)
    {
        if (externalFunctionHub.isPresent()) {
            Set<SqlInvokedFunction> sqlInvokedFunctions = extractExternalFunctions(externalFunctionHub.get());
            for (SqlInvokedFunction sqlInvokedFunction : sqlInvokedFunctions) {
                functionAndTypeManager.createFunction(sqlInvokedFunction, true);
            }
        }
    }

    private ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Presto announcement not found: " + announcements);
    }

    /**
     * update the catalogs this node own in the service announcer.
     */
    public synchronized void updateConnectorIds()
    {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        Set<String> connectorIds = new LinkedHashSet<>();
        Set<String> allConnectorIds = new LinkedHashSet<>();

        // automatically build connectorIds if not configured
        List<Catalog> catalogs = catalogManager.getCatalogs();

        // add data center names to connectorIds, then NodeScheduler can schedule task to this node.
        List<String> dataCenterNames = catalogConnectorStore.getDataCenterNames();

        // if this is a dedicated coordinator, only add jmx
        if (serverConfig.isCoordinator() && !schedulerConfig.isIncludeCoordinator()) {
            catalogs.stream()
                    .map(Catalog::getConnectorCatalogName)
                    .filter(connectorId -> connectorId.getCatalogName().equals("jmx"))
                    .map(Object::toString)
                    .forEach(connectorIds::add);
        }
        else {
            catalogs.stream()
                    .map(Catalog::getConnectorCatalogName)
                    .map(Object::toString)
                    .forEach(connectorIds::add);
            dataCenterNames.stream()
                    .forEach(connectorIds::add);
        }

        catalogs.stream()
                .map(Catalog::getConnectorCatalogName)
                .map(Object::toString)
                .forEach(allConnectorIds::add);
        dataCenterNames.stream()
                .forEach(allConnectorIds::add);

        // build announcement with updated sources
        Map<String, String> properties = new HashMap<>();
        properties.putAll(announcement.getProperties());
        properties.put("connectorIds", Joiner.on(",").join(connectorIds));
        properties.put("allConnectorIds", Joiner.on(",").join(allConnectorIds));

        // update announcement
        try {
            Field propertiesField = announcement.getClass().getDeclaredField("properties");
            propertiesField.setAccessible(true);
            propertiesField.set(announcement, properties);
        }
        catch (NoSuchFieldException | IllegalAccessException ex) {
            log.error(ex, "Set announcement properties failed");
            throw new RuntimeException("Set announcement properties failed", ex);
        }

        announcer.addServiceAnnouncement(announcement);
        announcer.forceAnnounce();
        nodeManager.refreshNodes();
        log.info("Announce connector ids, ACTIVE %s, All %s", connectorIds, allConnectorIds);
    }

    private static class MaterializedConnector
    {
        private final CatalogName catalogName;
        private final Connector connector;
        private final Set<SystemTable> systemTables;
        private final Set<Procedure> procedures;
        private final Optional<ConnectorSplitManager> splitManager;
        private final Optional<ConnectorPageSourceProvider> pageSourceProvider;
        private final Optional<ConnectorPageSinkProvider> pageSinkProvider;
        private final Optional<ConnectorIndexProvider> indexProvider;
        private final Optional<ConnectorNodePartitioningProvider> partitioningProvider;
        private final Optional<ConnectorPlanOptimizerProvider> planOptimizerProvider;
        private final Optional<ConnectorAccessControl> accessControl;
        private final List<PropertyMetadata<?>> sessionProperties;
        private final List<PropertyMetadata<?>> tableProperties;
        private final List<PropertyMetadata<?>> schemaProperties;
        private final List<PropertyMetadata<?>> columnProperties;
        private final List<PropertyMetadata<?>> analyzeProperties;

        public MaterializedConnector(CatalogName catalogName, Connector connector)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.connector = requireNonNull(connector, "connector is null");

            Set<SystemTable> connectorSystemTables = connector.getSystemTables();
            requireNonNull(connectorSystemTables, "Connector %s returned a null system tables set");
            this.systemTables = ImmutableSet.copyOf(connectorSystemTables);

            Set<Procedure> connectorProcedures = connector.getProcedures();
            requireNonNull(connectorProcedures, "Connector %s returned a null procedures set");
            this.procedures = ImmutableSet.copyOf(connectorProcedures);

            ConnectorSplitManager connectorSplitManager = null;
            try {
                connectorSplitManager = connector.getSplitManager();
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.splitManager = Optional.ofNullable(connectorSplitManager);

            ConnectorPageSourceProvider connectorPageSourceProvider = null;
            try {
                connectorPageSourceProvider = connector.getPageSourceProvider();
                requireNonNull(connectorPageSourceProvider, format("Connector %s returned a null page source provider", catalogName));
            }
            catch (UnsupportedOperationException ignored) {
                // could be ignored
            }

            try {
                ConnectorRecordSetProvider connectorRecordSetProvider = connector.getRecordSetProvider();
                requireNonNull(connectorRecordSetProvider, format("Connector %s returned a null record set provider", catalogName));
                verify(connectorPageSourceProvider == null, "Connector %s returned both page source and record set providers", catalogName);
                connectorPageSourceProvider = new RecordPageSourceProvider(connectorRecordSetProvider);
            }
            catch (UnsupportedOperationException ignored) {
                // could be ignored
            }
            this.pageSourceProvider = Optional.ofNullable(connectorPageSourceProvider);

            ConnectorPageSinkProvider connectorPageSinkProvider = null;
            try {
                connectorPageSinkProvider = connector.getPageSinkProvider();
                requireNonNull(connectorPageSinkProvider, format("Connector %s returned a null page sink provider", catalogName));
            }
            catch (UnsupportedOperationException ignored) {
                // could be ignored
            }
            this.pageSinkProvider = Optional.ofNullable(connectorPageSinkProvider);

            ConnectorIndexProvider connectorIndexProvider = null;
            try {
                connectorIndexProvider = connector.getIndexProvider();
                requireNonNull(connectorIndexProvider, format("Connector %s returned a null index provider", catalogName));
            }
            catch (UnsupportedOperationException ignored) {
                // could be ignored
            }
            this.indexProvider = Optional.ofNullable(connectorIndexProvider);

            ConnectorNodePartitioningProvider connectorNodePartitioningProvider = null;
            try {
                connectorNodePartitioningProvider = connector.getNodePartitioningProvider();
                requireNonNull(connectorNodePartitioningProvider, format("Connector %s returned a null partitioning provider", catalogName));
            }
            catch (UnsupportedOperationException ignored) {
                // could be ignored
            }
            this.partitioningProvider = Optional.ofNullable(connectorNodePartitioningProvider);

            ConnectorPlanOptimizerProvider connectorPlanOptimizerProvider = null;
            try {
                connectorPlanOptimizerProvider = connector.getConnectorPlanOptimizerProvider();
                requireNonNull(connectorPlanOptimizerProvider, format("Connector %s returned a null plan optimizer provider", catalogName));
            }
            catch (UnsupportedOperationException ignored) {
                // could be ignored
            }
            this.planOptimizerProvider = Optional.ofNullable(connectorPlanOptimizerProvider);

            ConnectorAccessControl connectorAccessControl = null;
            try {
                connectorAccessControl = connector.getAccessControl();
            }
            catch (UnsupportedOperationException ignored) {
                // could be ignored
            }
            this.accessControl = Optional.ofNullable(connectorAccessControl);

            List<PropertyMetadata<?>> connectorSessionProperties = connector.getSessionProperties();
            requireNonNull(connectorSessionProperties, "Connector %s returned a null system properties set");
            this.sessionProperties = ImmutableList.copyOf(connectorSessionProperties);

            List<PropertyMetadata<?>> connectorTableProperties = connector.getTableProperties();
            requireNonNull(connectorTableProperties, "Connector %s returned a null table properties set");
            this.tableProperties = ImmutableList.copyOf(connectorTableProperties);

            List<PropertyMetadata<?>> connectorSchemaProperties = connector.getSchemaProperties();
            requireNonNull(connectorSchemaProperties, "Connector %s returned a null schema properties set");
            this.schemaProperties = ImmutableList.copyOf(connectorSchemaProperties);

            List<PropertyMetadata<?>> connectorColumnProperties = connector.getColumnProperties();
            requireNonNull(connectorColumnProperties, "Connector %s returned a null column properties set");
            this.columnProperties = ImmutableList.copyOf(connectorColumnProperties);

            List<PropertyMetadata<?>> connectorAnalyzeProperties = connector.getAnalyzeProperties();
            requireNonNull(connectorAnalyzeProperties, "Connector %s returned a null analyze properties set");
            this.analyzeProperties = ImmutableList.copyOf(connectorAnalyzeProperties);
        }

        public CatalogName getCatalogName()
        {
            return catalogName;
        }

        public Connector getConnector()
        {
            return connector;
        }

        public Set<SystemTable> getSystemTables()
        {
            return systemTables;
        }

        public Set<Procedure> getProcedures()
        {
            return procedures;
        }

        public Optional<ConnectorSplitManager> getSplitManager()
        {
            return splitManager;
        }

        public Optional<ConnectorPageSourceProvider> getPageSourceProvider()
        {
            return pageSourceProvider;
        }

        public Optional<ConnectorPageSinkProvider> getPageSinkProvider()
        {
            return pageSinkProvider;
        }

        public Optional<ConnectorIndexProvider> getIndexProvider()
        {
            return indexProvider;
        }

        public Optional<ConnectorNodePartitioningProvider> getPartitioningProvider()
        {
            return partitioningProvider;
        }

        public Optional<ConnectorPlanOptimizerProvider> getPlanOptimizerProvider()
        {
            return planOptimizerProvider;
        }

        public Optional<ConnectorAccessControl> getAccessControl()
        {
            return accessControl;
        }

        public List<PropertyMetadata<?>> getSessionProperties()
        {
            return sessionProperties;
        }

        public List<PropertyMetadata<?>> getTableProperties()
        {
            return tableProperties;
        }

        public List<PropertyMetadata<?>> getColumnProperties()
        {
            return columnProperties;
        }

        public List<PropertyMetadata<?>> getSchemaProperties()
        {
            return schemaProperties;
        }

        public List<PropertyMetadata<?>> getAnalyzeProperties()
        {
            return analyzeProperties;
        }
    }
}
