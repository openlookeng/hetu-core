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
package io.prestosql.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.util.Modules;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.server.EmbeddedDiscoveryModule;
import io.prestosql.catalog.CatalogModule;
import io.prestosql.catalog.DynamicCatalogConfig;
import io.prestosql.catalog.showcatalog.ShowCatalogModule;
import io.prestosql.client.QueryResults;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.CostCalculator.EstimatedExchanges;
import io.prestosql.cost.CostCalculatorUsingExchanges;
import io.prestosql.cost.CostCalculatorWithEstimatedExchanges;
import io.prestosql.cost.CostComparator;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.cost.StatsCalculatorModule;
import io.prestosql.cost.TaskCountEstimator;
import io.prestosql.discovery.server.HetuEmbeddedDiscoveryModule;
import io.prestosql.dispatcher.DispatchExecutor;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.dispatcher.DispatchQueryFactory;
import io.prestosql.dispatcher.FailedDispatchQueryFactory;
import io.prestosql.dispatcher.LocalDispatchQueryFactory;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.event.QueryMonitor;
import io.prestosql.event.QueryMonitorConfig;
import io.prestosql.execution.AddColumnTask;
import io.prestosql.execution.CallTask;
import io.prestosql.execution.ClusterSizeMonitor;
import io.prestosql.execution.CommentTask;
import io.prestosql.execution.CommitTask;
import io.prestosql.execution.CreateCubeTask;
import io.prestosql.execution.CreateRoleTask;
import io.prestosql.execution.CreateSchemaTask;
import io.prestosql.execution.CreateTableTask;
import io.prestosql.execution.CreateViewTask;
import io.prestosql.execution.DataDefinitionTask;
import io.prestosql.execution.DeallocateTask;
import io.prestosql.execution.DropCacheTask;
import io.prestosql.execution.DropColumnTask;
import io.prestosql.execution.DropCubeTask;
import io.prestosql.execution.DropIndexTask;
import io.prestosql.execution.DropRoleTask;
import io.prestosql.execution.DropSchemaTask;
import io.prestosql.execution.DropTableTask;
import io.prestosql.execution.DropViewTask;
import io.prestosql.execution.ExplainAnalyzeContext;
import io.prestosql.execution.ForQueryExecution;
import io.prestosql.execution.GrantRolesTask;
import io.prestosql.execution.GrantTask;
import io.prestosql.execution.PrepareTask;
import io.prestosql.execution.QueryExecution;
import io.prestosql.execution.QueryExecutionMBean;
import io.prestosql.execution.QueryIdGenerator;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.QueryPerformanceFetcher;
import io.prestosql.execution.QueryPreparer;
import io.prestosql.execution.RemoteTaskFactory;
import io.prestosql.execution.RenameColumnTask;
import io.prestosql.execution.RenameIndexTask;
import io.prestosql.execution.RenameSchemaTask;
import io.prestosql.execution.RenameTableTask;
import io.prestosql.execution.ResetSessionTask;
import io.prestosql.execution.RevokeRolesTask;
import io.prestosql.execution.RevokeTask;
import io.prestosql.execution.RollbackTask;
import io.prestosql.execution.SetPathTask;
import io.prestosql.execution.SetRoleTask;
import io.prestosql.execution.SetSessionTask;
import io.prestosql.execution.SplitCacheStateManager;
import io.prestosql.execution.SqlQueryManager;
import io.prestosql.execution.StartTransactionTask;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.execution.UseTask;
import io.prestosql.execution.resourcegroups.InternalResourceGroupManager;
import io.prestosql.execution.resourcegroups.LegacyResourceGroupConfigurationManager;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.execution.scheduler.AllAtOnceExecutionPolicy;
import io.prestosql.execution.scheduler.ExecutionPolicy;
import io.prestosql.execution.scheduler.PhasedExecutionPolicy;
import io.prestosql.execution.scheduler.SplitSchedulerStats;
import io.prestosql.failuredetector.CoordinatorGossipFailureDetectorModule;
import io.prestosql.failuredetector.FailureDetectorModule;
import io.prestosql.memory.ClusterMemoryManager;
import io.prestosql.memory.ForMemoryManager;
import io.prestosql.memory.LowMemoryKiller;
import io.prestosql.memory.MemoryManagerConfig;
import io.prestosql.memory.MemoryManagerConfig.LowMemoryKillerPolicy;
import io.prestosql.memory.NoneLowMemoryKiller;
import io.prestosql.memory.TotalReservationLowMemoryKiller;
import io.prestosql.memory.TotalReservationOnBlockedNodesLowMemoryKiller;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.operator.ForScheduler;
import io.prestosql.queryeditorui.QueryEditorUIModule;
import io.prestosql.queryhistory.QueryHistoryModule;
import io.prestosql.resourcemanager.ForResourceMonitor;
import io.prestosql.resourcemanager.QueryResourceManagerService;
import io.prestosql.server.remotetask.RemoteTaskStats;
import io.prestosql.spi.memory.ClusterMemoryPoolManager;
import io.prestosql.spi.resourcegroups.QueryType;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.planner.PlanFragmenter;
import io.prestosql.sql.planner.PlanOptimizers;
import io.prestosql.sql.tree.AddColumn;
import io.prestosql.sql.tree.Call;
import io.prestosql.sql.tree.Comment;
import io.prestosql.sql.tree.Commit;
import io.prestosql.sql.tree.CreateCube;
import io.prestosql.sql.tree.CreateRole;
import io.prestosql.sql.tree.CreateSchema;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.Deallocate;
import io.prestosql.sql.tree.DropCache;
import io.prestosql.sql.tree.DropColumn;
import io.prestosql.sql.tree.DropCube;
import io.prestosql.sql.tree.DropIndex;
import io.prestosql.sql.tree.DropRole;
import io.prestosql.sql.tree.DropSchema;
import io.prestosql.sql.tree.DropTable;
import io.prestosql.sql.tree.DropView;
import io.prestosql.sql.tree.Grant;
import io.prestosql.sql.tree.GrantRoles;
import io.prestosql.sql.tree.Prepare;
import io.prestosql.sql.tree.RenameColumn;
import io.prestosql.sql.tree.RenameIndex;
import io.prestosql.sql.tree.RenameSchema;
import io.prestosql.sql.tree.RenameTable;
import io.prestosql.sql.tree.ResetSession;
import io.prestosql.sql.tree.Revoke;
import io.prestosql.sql.tree.RevokeRoles;
import io.prestosql.sql.tree.Rollback;
import io.prestosql.sql.tree.SetPath;
import io.prestosql.sql.tree.SetRole;
import io.prestosql.sql.tree.SetSession;
import io.prestosql.sql.tree.StartTransaction;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.Use;
import io.prestosql.statestore.EmbeddedStateStoreLauncher;
import io.prestosql.statestore.StateStoreLauncher;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.transaction.ForTransactionManager;
import io.prestosql.transaction.InMemoryTransactionManager;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.transaction.TransactionManagerConfig;
import io.prestosql.vacuum.AutoVacuumConfig;
import io.prestosql.vacuum.AutoVacuumScanner;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Verify.verify;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.prestosql.execution.DataDefinitionExecution.DataDefinitionExecutionFactory;
import static io.prestosql.execution.QueryExecution.QueryExecutionFactory;
import static io.prestosql.execution.SqlQueryExecution.SqlQueryExecutionFactory;
import static io.prestosql.util.StatementUtils.getAllQueryTypes;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CoordinatorModule
        extends AbstractConfigurationAwareModule
{
    private boolean gossip;

    public CoordinatorModule(boolean gossip)
    {
        this.gossip = gossip;
    }

    public CoordinatorModule()
    {
        this(false);
    }

    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);

        httpServerBinder(binder).bindResource("/ui", "webapp").withWelcomeFile("queryeditor.html");
        httpServerBinder(binder).bindResource("/tableau", "webapp/tableau");

        // discovery server
        install(installModuleIf(EmbeddedDiscoveryConfig.class, EmbeddedDiscoveryConfig::isEnabled, Modules.override(new EmbeddedDiscoveryModule())
                .with(new HetuEmbeddedDiscoveryModule())));

        // presto coordinator announcement
        discoveryBinder(binder).bindHttpAnnouncement("presto-coordinator");

        // statement resource
        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(QueryResults.class);
        jsonCodecBinder(binder).bindJsonCodec(SelectedRole.class);
        jaxrsBinder(binder).bind(io.prestosql.dispatcher.QueuedStatementResource.class);
        jaxrsBinder(binder).bind(io.prestosql.datacenter.DataCenterStatementResource.class);
        jaxrsBinder(binder).bind(io.prestosql.server.protocol.ExecutingStatementResource.class);
        binder.bind(StatementHttpExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(StatementHttpExecutionMBean.class).withGeneratedName();

        //hetu-ui module
        binder.install(new QueryEditorUIModule());

        // catalog resource
        install(installModuleIf(DynamicCatalogConfig.class, DynamicCatalogConfig::isDynamicCatalogEnabled, new CatalogModule()));

        //showcatalog resourece
        install(new ShowCatalogModule());

        //QueryHistory Module
        binder.install(new QueryHistoryModule());

        // resource for serving static content
        jaxrsBinder(binder).bind(WebUiResource.class);

        // failure detector
        if (gossip) {
            binder.install(new CoordinatorGossipFailureDetectorModule());
            jaxrsBinder(binder).bind(GossipNodeResource.class);
            jaxrsBinder(binder).bind(WorkerResource.class);
        }
        else {
            binder.install(new FailureDetectorModule());
            jaxrsBinder(binder).bind(NodeResource.class);
            jaxrsBinder(binder).bind(WorkerResource.class);
        }
        httpClientBinder(binder).bindHttpClient("workerInfo", ForWorkerInfo.class);

        // query monitor
        jsonCodecBinder(binder).bindJsonCodec(StatsAndCosts.class);
        configBinder(binder).bindConfig(QueryMonitorConfig.class);
        binder.bind(QueryMonitor.class).in(Scopes.SINGLETON);

        // state store
        binder.bind(StateStoreLauncher.class).to(EmbeddedStateStoreLauncher.class).in(Scopes.SINGLETON);

        // query manager
        jaxrsBinder(binder).bind(QueryResource.class);
        jaxrsBinder(binder).bind(StageResource.class);
        jaxrsBinder(binder).bind(QueryStateInfoResource.class);
        jaxrsBinder(binder).bind(ResourceGroupStateInfoResource.class);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryManager.class).withGeneratedName();
        binder.bind(QueryPreparer.class).in(Scopes.SINGLETON);
        binder.bind(SessionSupplier.class).to(QuerySessionSupplier.class).in(Scopes.SINGLETON);
        binder.bind(InternalResourceGroupManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(InternalResourceGroupManager.class).withGeneratedName();
        binder.bind(ResourceGroupManager.class).to(InternalResourceGroupManager.class);
        binder.bind(LegacyResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);

        // dispatcher
        binder.bind(DispatchManager.class).in(Scopes.SINGLETON);
        binder.bind(FailedDispatchQueryFactory.class).in(Scopes.SINGLETON);
        binder.bind(DispatchExecutor.class).in(Scopes.SINGLETON);

        // local dispatcher
        binder.bind(DispatchQueryFactory.class).to(LocalDispatchQueryFactory.class);

        // cluster memory manager
        binder.bind(ClusterMemoryManager.class).in(Scopes.SINGLETON);
        binder.bind(ClusterMemoryPoolManager.class).to(ClusterMemoryManager.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("memoryManager", ForMemoryManager.class)
                .withTracing()
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(serverConfig.getHttpClientIdleTimeout());
                    config.setRequestTimeout(serverConfig.getHttpClientRequestTimeout());
                });
        bindLowMemoryKiller(LowMemoryKillerPolicy.NONE, NoneLowMemoryKiller.class);
        bindLowMemoryKiller(LowMemoryKillerPolicy.TOTAL_RESERVATION, TotalReservationLowMemoryKiller.class);
        bindLowMemoryKiller(LowMemoryKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES, TotalReservationOnBlockedNodesLowMemoryKiller.class);
        newExporter(binder).export(ClusterMemoryManager.class).withGeneratedName();

        // node monitor
        binder.bind(ClusterSizeMonitor.class).in(Scopes.SINGLETON);

        // statistics calculator
        binder.install(new StatsCalculatorModule());

        // cost calculator
        binder.bind(TaskCountEstimator.class).in(Scopes.SINGLETON);
        binder.bind(CostCalculator.class).to(CostCalculatorUsingExchanges.class).in(Scopes.SINGLETON);
        binder.bind(CostCalculator.class).annotatedWith(EstimatedExchanges.class).to(CostCalculatorWithEstimatedExchanges.class).in(Scopes.SINGLETON);
        binder.bind(CostComparator.class).in(Scopes.SINGLETON);

        // cluster statistics
        jaxrsBinder(binder).bind(ClusterStatsResource.class);

        // planner
        binder.bind(PlanFragmenter.class).in(Scopes.SINGLETON);
        binder.bind(PlanOptimizers.class).in(Scopes.SINGLETON);

        // dynamic filtering service
        binder.bind(DynamicFilterService.class).in(Scopes.SINGLETON);

        // query explainer
        binder.bind(QueryExplainer.class).in(Scopes.SINGLETON);

        // explain analyze
        binder.bind(ExplainAnalyzeContext.class).in(Scopes.SINGLETON);

        //split cache map
        binder.bind(SplitCacheStateManager.class).in(Scopes.SINGLETON);

        binder.bind(RemoteTaskFactory.class).to(HttpRemoteTaskFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RemoteTaskFactory.class).withGeneratedName();

        binder.bind(RemoteTaskStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RemoteTaskStats.class).withGeneratedName();

        httpClientBinder(binder).bindHttpClient("scheduler", ForScheduler.class)
                .withTracing()
                .withFilter(GenerateTraceTokenRequestFilter.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(serverConfig.getHttpClientIdleTimeout());
                    config.setRequestTimeout(serverConfig.getHttpClientRequestTimeout());
                    config.setMaxConnectionsPerServer(250);
                });

        binder.bind(ExecutorService.class).annotatedWith(ForResourceMonitor.class)
                .toInstance(newSingleThreadExecutor(threadsNamed("resource-monitor-%s")));
        binder.bind(QueryResourceManagerService.class).in(Scopes.SINGLETON);

        binder.bind(ScheduledExecutorService.class).annotatedWith(ForScheduler.class)
                .toInstance(newSingleThreadScheduledExecutor(threadsNamed("stage-scheduler")));

        install(installModuleIf(
                AutoVacuumConfig.class,
                config -> config.isAutoVacuumEnabled(),
                binder1 -> binder1.bind(AutoVacuumScanner.class).in(Scopes.SINGLETON)));

        // query execution
        binder.bind(ExecutorService.class).annotatedWith(ForQueryExecution.class)
                .toInstance(newCachedThreadPool(threadsNamed("query-execution-%s")));
        binder.bind(QueryExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryExecutionMBean.class)
                .as(generator -> generator.generatedNameOf(QueryExecution.class));

        MapBinder<Class<? extends Statement>, QueryExecutionFactory<?>> executionBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<QueryExecutionFactory<?>>() {});

        binder.bind(SplitSchedulerStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SplitSchedulerStats.class).withGeneratedName();
        binder.bind(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        getAllQueryTypes().entrySet().stream()
                .filter(entry -> entry.getValue() != QueryType.DATA_DEFINITION)
                .forEach(entry -> executionBinder.addBinding(entry.getKey()).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON));

        binder.bind(DataDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
        bindDataDefinitionTask(binder, executionBinder, CreateSchema.class, CreateSchemaTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropSchema.class, DropSchemaTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameSchema.class, RenameSchemaTask.class);
        bindDataDefinitionTask(binder, executionBinder, AddColumn.class, AddColumnTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateTable.class, CreateTableTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameTable.class, RenameTableTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameIndex.class, RenameIndexTask.class);
        bindDataDefinitionTask(binder, executionBinder, Comment.class, CommentTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameColumn.class, RenameColumnTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropColumn.class, DropColumnTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropTable.class, DropTableTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateView.class, CreateViewTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropView.class, DropViewTask.class);
        bindDataDefinitionTask(binder, executionBinder, Use.class, UseTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetSession.class, SetSessionTask.class);
        bindDataDefinitionTask(binder, executionBinder, ResetSession.class, ResetSessionTask.class);
        bindDataDefinitionTask(binder, executionBinder, StartTransaction.class, StartTransactionTask.class);
        bindDataDefinitionTask(binder, executionBinder, Commit.class, CommitTask.class);
        bindDataDefinitionTask(binder, executionBinder, Rollback.class, RollbackTask.class);
        bindDataDefinitionTask(binder, executionBinder, Call.class, CallTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateRole.class, CreateRoleTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropRole.class, DropRoleTask.class);
        bindDataDefinitionTask(binder, executionBinder, GrantRoles.class, GrantRolesTask.class);
        bindDataDefinitionTask(binder, executionBinder, RevokeRoles.class, RevokeRolesTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetRole.class, SetRoleTask.class);
        bindDataDefinitionTask(binder, executionBinder, Grant.class, GrantTask.class);
        bindDataDefinitionTask(binder, executionBinder, Revoke.class, RevokeTask.class);
        bindDataDefinitionTask(binder, executionBinder, Prepare.class, PrepareTask.class);
        bindDataDefinitionTask(binder, executionBinder, Deallocate.class, DeallocateTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetPath.class, SetPathTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropCache.class, DropCacheTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropIndex.class, DropIndexTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropCube.class, DropCubeTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateCube.class, CreateCubeTask.class);

        MapBinder<String, ExecutionPolicy> executionPolicyBinder = newMapBinder(binder, String.class, ExecutionPolicy.class);
        executionPolicyBinder.addBinding("all-at-once").to(AllAtOnceExecutionPolicy.class);
        executionPolicyBinder.addBinding("phased").to(PhasedExecutionPolicy.class);

        // cleanup
        binder.bind(ExecutorCleanup.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static ResourceGroupManager<?> getResourceGroupManager(@SuppressWarnings("rawtypes") ResourceGroupManager manager)
    {
        return manager;
    }

    @Provides
    @Singleton
    public static QueryPerformanceFetcher createQueryPerformanceFetcher(QueryManager queryManager)
    {
        return queryManager::getFullQueryInfo;
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static ExecutorService createStatementResponseCoreExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("statement-response-%s"));
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static BoundedExecutor createStatementResponseExecutor(@ForStatementResource ExecutorService coreExecutor, TaskManagerConfig config)
    {
        return new BoundedExecutor(coreExecutor, config.getHttpResponseThreads());
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static ScheduledExecutorService createStatementTimeoutExecutor(TaskManagerConfig config)
    {
        return newScheduledThreadPool(config.getHttpTimeoutThreads(), daemonThreadsNamed("statement-timeout-%s"));
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ScheduledExecutorService createTransactionIdleCheckExecutor()
    {
        return newSingleThreadScheduledExecutor(daemonThreadsNamed("transaction-idle-check"));
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ExecutorService createTransactionFinishingExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("transaction-finishing-%s"));
    }

    @Provides
    @Singleton
    public static TransactionManager createTransactionManager(
            TransactionManagerConfig config,
            CatalogManager catalogManager,
            StateStoreProvider stateStoreProvider,
            @ForTransactionManager ScheduledExecutorService idleCheckExecutor,
            @ForTransactionManager ExecutorService finishingExecutor)
    {
        return InMemoryTransactionManager.create(config, idleCheckExecutor, catalogManager, finishingExecutor, stateStoreProvider);
    }

    private static <T extends Statement> void bindDataDefinitionTask(
            Binder binder,
            MapBinder<Class<? extends Statement>, QueryExecutionFactory<?>> executionBinder,
            Class<T> statement,
            Class<? extends DataDefinitionTask<T>> task)
    {
        verify(getAllQueryTypes().get(statement) == QueryType.DATA_DEFINITION);
        MapBinder<Class<? extends Statement>, DataDefinitionTask<?>> taskBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<DataDefinitionTask<?>>() {});

        taskBinder.addBinding(statement).to(task).in(Scopes.SINGLETON);
        executionBinder.addBinding(statement).to(DataDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
    }

    private void bindLowMemoryKiller(String name, Class<? extends LowMemoryKiller> clazz)
    {
        install(installModuleIf(
                MemoryManagerConfig.class,
                config -> name.equals(config.getLowMemoryKillerPolicy()),
                binder -> binder.bind(LowMemoryKiller.class).to(clazz).in(Scopes.SINGLETON)));
    }

    public static class ExecutorCleanup
    {
        private final List<ExecutorService> executors;

        @Inject
        public ExecutorCleanup(
                @ForStatementResource ExecutorService statementResponseExecutor,
                @ForStatementResource ScheduledExecutorService statementTimeoutExecutor,
                @ForQueryExecution ExecutorService queryExecutionExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor,
                @ForTransactionManager ExecutorService transactionFinishingExecutor,
                @ForTransactionManager ScheduledExecutorService transactionIdleExecutor)
        {
            executors = ImmutableList.<ExecutorService>builder()
                    .add(statementResponseExecutor)
                    .add(statementTimeoutExecutor)
                    .add(queryExecutionExecutor)
                    .add(schedulerExecutor)
                    .add(transactionFinishingExecutor)
                    .add(transactionIdleExecutor)
                    .build();
        }

        @PreDestroy
        public void shutdown()
        {
            executors.forEach(ExecutorService::shutdownNow);
        }
    }
}
