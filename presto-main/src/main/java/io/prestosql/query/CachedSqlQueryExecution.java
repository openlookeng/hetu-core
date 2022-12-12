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
package io.prestosql.query;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.cache.CachedDataManager;
import io.prestosql.cache.CachedDataStorageProvider;
import io.prestosql.cache.elements.CachedDataKey;
import io.prestosql.cache.elements.CachedDataStorage;
import io.prestosql.connector.informationschema.InformationSchemaTransactionHandle;
import io.prestosql.connector.system.GlobalSystemTransactionHandle;
import io.prestosql.connector.system.SystemTransactionHandle;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.cube.CubeManager;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.exchange.ExchangeManagerRegistry;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.QueryPreparer;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryStateMachine;
import io.prestosql.execution.RemoteTaskFactory;
import io.prestosql.execution.SqlQueryExecution;
import io.prestosql.execution.SqlTaskManager;
import io.prestosql.execution.TableExecuteContextManager;
import io.prestosql.execution.scheduler.NodeAllocatorService;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.PartitionMemoryEstimatorFactory;
import io.prestosql.execution.scheduler.SplitSchedulerStats;
import io.prestosql.execution.scheduler.TaskDescriptorStorage;
import io.prestosql.execution.scheduler.TaskExecutionStats;
import io.prestosql.execution.scheduler.TaskSourceFactory;
import io.prestosql.execution.scheduler.policy.ExecutionPolicy;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.failuredetector.FailureDetector;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.resourcemanager.QueryResourceManagerService;
import io.prestosql.security.AccessControl;
import io.prestosql.snapshot.RecoveryUtils;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.split.SplitManager;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.LogicalPlanner;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningHandle;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.PlanFragmenter;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.IterativeOptimizer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.optimizations.BeginTableWrite;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.optimizations.ResultCacheTableRead;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.tree.CTEReference;
import io.prestosql.sql.tree.CreateIndex;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateTableAsSelect;
import io.prestosql.sql.tree.CurrentPath;
import io.prestosql.sql.tree.CurrentTime;
import io.prestosql.sql.tree.CurrentUser;
import io.prestosql.sql.tree.DefaultTraversalVisitor;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.Table;
import io.prestosql.sql.tree.UpdateIndex;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.transaction.TransactionId;
import io.prestosql.utils.OptimizerUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.prestosql.SystemSessionProperties.getDataCacheCatalogName;
import static io.prestosql.SystemSessionProperties.getDataCacheSchemaName;
import static io.prestosql.SystemSessionProperties.isCTEResultCacheEnabled;
import static io.prestosql.SystemSessionProperties.isCTEReuseEnabled;
import static io.prestosql.SystemSessionProperties.isExecutionPlanCacheEnabled;
import static io.prestosql.SystemSessionProperties.isQueryResourceTrackingEnabled;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class CachedSqlQueryExecution
        extends SqlQueryExecution
{
    private final Optional<Cache<Integer, CachedSqlQueryExecutionPlan>> cache; // cache key is generated by SqlQueryExecutionCacheKeyGenerator
    private final CachedDataManager dataCache;
    private final BeginTableWrite beginTableWrite;
    private final ResultCacheTableRead resultCacheTableRead;

    public CachedSqlQueryExecution(QueryPreparer.PreparedQuery preparedQuery, QueryStateMachine stateMachine,
                                   String slug, Metadata metadata, CubeManager cubeManager, AccessControl accessControl, SqlParser sqlParser, SplitManager splitManager,
                                   NodePartitioningManager nodePartitioningManager, NodeScheduler nodeScheduler,
                                   List<PlanOptimizer> planOptimizers, PlanFragmenter planFragmenter, RemoteTaskFactory remoteTaskFactory,
                                   LocationFactory locationFactory, int scheduleSplitBatchSize, ExecutorService queryExecutor,
                                   ScheduledExecutorService schedulerExecutor, FailureDetector failureDetector, NodeTaskMap nodeTaskMap,
                                   QueryExplainer queryExplainer, ExecutionPolicy executionPolicy, SplitSchedulerStats schedulerStats,
                                   StatsCalculator statsCalculator, CostCalculator costCalculator, WarningCollector warningCollector,
                                   DynamicFilterService dynamicFilterService, Optional<Cache<Integer, CachedSqlQueryExecutionPlan>> cache,
                                   HeuristicIndexerManager heuristicIndexerManager, StateStoreProvider stateStoreProvider, RecoveryUtils recoveryUtils,
                                   ExchangeManagerRegistry exchangeManagerRegistry, SqlTaskManager coordinatorTaskManager, TaskSourceFactory taskSourceFactory,
                                   TaskDescriptorStorage taskDescriptorStorage, NodeAllocatorService nodeAllocatorService,
                                   PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory, TaskExecutionStats taskExecutionStats,
                                   QueryResourceManagerService queryResourceManager,
                                   TableExecuteContextManager tableExecuteContextManager,
                                   CachedDataManager dataCache, boolean isMultiCoordinatorEnabled)
    {
        super(tableExecuteContextManager, preparedQuery, stateMachine, slug, metadata, cubeManager, accessControl, sqlParser, splitManager,
                nodePartitioningManager, nodeScheduler, planOptimizers, planFragmenter, remoteTaskFactory, locationFactory,
                scheduleSplitBatchSize, queryExecutor, schedulerExecutor, failureDetector, nodeTaskMap, queryExplainer,
                executionPolicy, schedulerStats, statsCalculator, costCalculator, warningCollector, dynamicFilterService,
                heuristicIndexerManager, stateStoreProvider, recoveryUtils, exchangeManagerRegistry, coordinatorTaskManager,
                taskSourceFactory, taskDescriptorStorage, nodeAllocatorService, partitionMemoryEstimatorFactory, taskExecutionStats,
                queryResourceManager, isMultiCoordinatorEnabled);
        this.cache = cache;
        this.dataCache = dataCache;
        this.beginTableWrite = new BeginTableWrite(metadata);
        this.resultCacheTableRead = new ResultCacheTableRead(metadata);
    }

    @Override
    protected Plan createPlan(Analysis analysis, Session session, List<PlanOptimizer> planOptimizers,
            PlanNodeIdAllocator idAllocator, Metadata metadata, TypeAnalyzer typeAnalyzer, StatsCalculator statsCalculator,
            CostCalculator costCalculator, WarningCollector warningCollector, CachedDataStorageProvider cachedData)
    {
        Statement statement = analysis.getStatement();

        // Get relevant Session properties which may affect the resulting execution plan
        Map<String, Object> systemSessionProperties = new HashMap<>(); // Property to property value mapping
        SystemSessionProperties sessionProperties = new SystemSessionProperties();
        for (PropertyMetadata<?> property : sessionProperties.getSessionProperties()) {
            systemSessionProperties.put(property.getName(), session.getSystemProperty(property.getName(), property.getJavaType()));
        }

        // if the original statement before rewriting is CreateIndex, set session to let connector know that pageMetadata should be enabled
        if (analysis.getOriginalStatement() instanceof CreateIndex || analysis.getOriginalStatement() instanceof UpdateIndex) {
            session.setPageMetadataEnabled(true);
        }

        // build list of fully qualified table names
        List<String> tableNames = new ArrayList<>();
        Map<String, TableStatistics> tableStatistics = new HashMap<>();
        // Get column name to column type to detect column type changes between queries more easily
        Map<String, Type> columnTypes = new HashMap<>();
        // Cacheable conditions:
        // 1. Caching must be enabled globally
        // 2. Caching must be enabled in the session
        // 3. There must not be any parameters in the query
        //      TODO: remove requirement for empty params and implement parameter rewrite
        // 4. Methods in ConnectorTableHandle and ConnectorMetadata must be
        //     overwritten to allow access to fully qualified table names and column names
        // 5. Statement must be an instance of Query and not contain CurrentX functions
        boolean cacheable = this.cache.isPresent() &&
                isExecutionPlanCacheEnabled(session) &&
                analysis.getParameters().isEmpty() &&
                validateAndExtractTableAndColumns(analysis, metadata, session, tableNames, tableStatistics, columnTypes) &&
                isCacheable(statement) &&
                (!(analysis.getOriginalStatement() instanceof CreateIndex || analysis.getOriginalStatement() instanceof UpdateIndex)); // create index and update index should not be cached
        cacheable = cacheable && !tableNames.isEmpty();
        boolean isCteDataCacheable = this.dataCache.isDataCachedEnabled() && isCTEResultCacheEnabled(session) && isCTEReuseEnabled(session);
        CachedDataKey.Builder builder = CachedDataKey.builder();

        List<String> optimizers = new ArrayList<>();
        // build list of enabled optimizers and rules for cache key
        for (PlanOptimizer planOptimizer : planOptimizers) {
            if (planOptimizer instanceof IterativeOptimizer) {
                IterativeOptimizer iterativeOptimizer = (IterativeOptimizer) planOptimizer;
                Set<Rule<?>> rules = iterativeOptimizer.getRules();
                for (Rule rule : rules) {
                    if (OptimizerUtils.isEnabledRule(rule, session)) {
                        optimizers.add(rule.getClass().getSimpleName());
                        builder.addRule(rule.getClass().getSimpleName());
                    }
                }
            }
            else {
                if (OptimizerUtils.isEnabledLegacy(planOptimizer, session)) {
                    optimizers.add(planOptimizer.getClass().getSimpleName());
                    builder.addRule(planOptimizer.getClass().getSimpleName());
                }
            }
        }

        Set<String> connectors = tableNames.stream().map(table -> table.substring(0, table.indexOf("."))).collect(Collectors.toSet());
        connectors.stream().forEach(connector -> {
            for (Map.Entry<String, String> property : session.getConnectorProperties(new CatalogName(connector)).entrySet()) {
                systemSessionProperties.put(connector + "." + property.getKey(), property.getValue());
            }
        });

        Plan plan;
        int key = 0;

        if (cacheable) {
            if (isCteDataCacheable) {
                builder.addTableNames(tableNames.toArray(new String[0]));
                builder.setQuery((Query) statement);
                builder.addColumns(columnTypes);
            }

            // TODO: Traverse the statement to build the key then combine tables/optimizers.. etc
            SqlQueryExecutionCacheKeyGenerator.buildKey((Query) statement, tableNames, optimizers, columnTypes, session.getTimeZoneKey(), systemSessionProperties);
        }

        boolean finalCacheable = cacheable;
        CachedDataStorageProvider cachedDataStorageProvider = new CachedDataStorageProvider()
        {
            CachedDataKey dataKey = builder.build();

            @Override
            public CachedDataKey.Builder getCachedDataKeyBuilder(String cteName)
            {
                Table cteRef = new Table(QualifiedName.of(cteName));
                CachedDataKey.Builder keyBuilder = super.getCachedDataKeyBuilder(cteName)
                        .addRules(optimizers.toArray(new String[0]))
                        .setQuery(analysis.getNamedQueryByRef(cteRef)); // Check how can identify and assign TableName and Column Names here!
                validateAndExtractTableAndColumnsByCTE(analysis, metadata, session, new CTEReference(QualifiedName.of(cteName)), keyBuilder);
                return keyBuilder;
            }

            @Override
            public CachedDataStorage getOrCreateCachedDataKey(CachedDataKey cachedDataKey)
            {
                CachedDataKey createKey = cachedDataKey;
                if (cachedDataKey == null) {
                    createKey = dataKey;
                }

                CachedDataStorage cds = dataCache.validateAndGet(createKey, session);
                if (cds == null) {
                    cds = new CachedDataStorage(createKey, new CatalogSchemaTableName(getDataCacheCatalogName(session), getDataCacheSchemaName(session), UUID.randomUUID().toString().replaceAll("-", "_")),
                            new Function<Void, Void>()
                            {
                                @Override
                                public Void apply(Void unused)
                                {
                                    if (finalCacheable) {
                                        cache.get().invalidate(key);
                                    }
                                    return null;
                                }
                            },
                            null);
                    cds.updateTableReferences(metadata, session);
                    dataCache.put(createKey, cds);
                    CachedDataStorage finalCds = cds;
                    CachedDataKey finalCachedDataKey = createKey;
                    addStateChangeListener(newState -> {
                        if (newState == QueryState.FINISHED && finalCds.isNonCachable() && finalCacheable) {
                            cache.get().invalidate(key);
                        }
                        if (newState == QueryState.FAILED) {
                            /* In case some CTEs got committed even on failed query is useful */
                            if (!finalCds.isCommitted()) {
                                finalCds.abort();
                                dataCache.invalidate(ImmutableSet.of(finalCachedDataKey));
                            }
                            cache.get().invalidate(key);
                        }
                    });
                    return cds;
                }
                if (cds.inProgress() || cds.isNonCachable()) {
                    return null;
                }
                return cds;
            }
        };

        if (!cacheable) {
            return super.createPlan(analysis, session, planOptimizers, idAllocator, metadata, typeAnalyzer,
                    statsCalculator, costCalculator, warningCollector, cachedDataStorageProvider);
        }

        CachedSqlQueryExecutionPlan cachedPlan = this.cache.get().getIfPresent(key);
        HetuLogicalPlanner logicalPlanner = new HetuLogicalPlanner(session, planOptimizers, idAllocator,
                metadata, typeAnalyzer, statsCalculator, costCalculator, warningCollector, cachedDataStorageProvider);

        PlanNode root;
        plan = cachedPlan != null ? cachedPlan.getPlan() : null;
        // To handle the chance of cache key collision, the timezone and the statement between
        // the cached plan and the session are verified for greater confidence.
        // Timezone must be matched in order to preserve the correctness for queries containing functions
        // that rely on system time
        if (plan != null && cachedPlan.getTimeZoneKey().equals(session.getTimeZoneKey()) &&
                cachedPlan.getStatement().equals(statement) && session.getTransactionId().isPresent() && cachedPlan.getIdentity().getUser().equals(session.getIdentity().getUser())) { // TODO: traverse the statement and accept partial match
            root = plan.getRoot();
            boolean isValidCachePlan = tablesMatch(root, analysis.getTables());
            try {
                if (!isEqualBasicStatistics(cachedPlan.getTableStatistics(), tableStatistics, tableNames) || !isValidCachePlan) {
                    for (TableHandle tableHandle : analysis.getTables()) {
                        tableStatistics.replace(tableHandle.getFullyQualifiedName(), metadata.getTableStatistics(session, tableHandle, Constraint.alwaysTrue(), true));
                    }
                    if (!cachedPlan.getTableStatistics().equals(tableStatistics) || !isValidCachePlan) {
                        // TableStatistics have changed, therefore the cached plan may no longer be applicable
                        // Table have changed, therefore the cached plan may no longer be applicable
                        throw new NoSuchElementException();
                    }
                }
                // TableScanNode may contain the old transaction id.
                // The following logic rewrites the logical plan by replacing the TableScanNode with a new TableScanNode which
                // contains the new transaction id from session.
                root = SimplePlanRewriter.rewriteWith(new TableHandleRewriter(session, analysis, metadata), root);
            }
            catch (NoSuchElementException e) {
                // Cached plan is outdated
                // invalidate cache
                this.cache.get().invalidateAll();
                // Build a new plan
                plan = createAndCachePlan(key, logicalPlanner, statement, tableNames, tableStatistics, optimizers, analysis, columnTypes, systemSessionProperties);
                root = plan.getRoot();
            }
        }
        else {
            // Build a new plan
            for (TableHandle tableHandle : analysis.getTables()) {
                tableStatistics.replace(tableHandle.getFullyQualifiedName(), metadata.getTableStatistics(session, tableHandle, Constraint.alwaysTrue(), true));
            }
            plan = createAndCachePlan(key, logicalPlanner, statement, tableNames, tableStatistics, optimizers, analysis, columnTypes, systemSessionProperties);
            root = plan.getRoot();
        }
        // BeginTableWrite optimizer must be run at the end as the last optimization
        // due to a hack Hetu community added which also serves to updates
        // metadata in the nodes

        if (isCTEResultCacheEnabled(session) && analysis.getStatement() instanceof Query) {
            try {
                root = this.resultCacheTableRead.optimize(root, session, null, new PlanSymbolAllocator(), new PlanNodeIdAllocator(), null, cachedDataStorageProvider);
            }
            catch (NoSuchElementException e) {
                // Cached plan is outdated
                // invalidate cache
                this.cache.get().invalidateAll();
                // Build a new plan
                plan = createAndCachePlan(key, logicalPlanner, statement, tableNames, tableStatistics, optimizers, analysis, columnTypes, systemSessionProperties);
                root = plan.getRoot();
            }
        }
        root = this.beginTableWrite.optimize(root, session, null, null, null, null);
        plan = update(plan, root);

        return plan;
    }

    private Plan createAndCachePlan(
            int key,
            LogicalPlanner logicalPlanner,
            Statement statement,
            List<String> tableNames,
            Map<String, TableStatistics> tableStatistics,
            List<String> planOptimizers,
            Analysis analysis,
            Map<String, Type> columnTypes,
            Map<String, Object> systemSessionProperties)
    {
        // build a new plan
        Plan plan = logicalPlanner.plan(analysis, !isQueryResourceTrackingEnabled(getSession()));
        // Cache the plan
        CachedSqlQueryExecutionPlan newCachedPlan = new CachedSqlQueryExecutionPlan(statement, tableNames, tableStatistics, planOptimizers, plan,
                analysis.getParameters(), columnTypes, getSession().getTimeZoneKey(), getSession().getIdentity(), systemSessionProperties);
        this.cache.get().put(key, newCachedPlan);
        return plan;
    }

    private boolean validateAndExtractTableAndColumnsByCTE(
            Analysis analysis,
            Metadata metadata,
            Session session,
            CTEReference namedQuery,
            CachedDataKey.Builder builder)
    {
        for (TableHandle tableHandle : analysis.getTablesByNamedQuery(namedQuery)) {
            builder.addTableName(tableHandle.getFullyQualifiedName());
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
            for (ColumnHandle columnHandle : columnHandles.values()) {
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, columnHandle);
                builder.addColumn(tableHandle.getFullyQualifiedName() + "." + columnHandle.getColumnName(), columnMetadata.getType());
            }
        }
        return true;
    }

    private boolean validateAndExtractTableAndColumns(
            Analysis analysis,
            Metadata metadata,
            Session session,
            List<String> tables,
            Map<String, TableStatistics> tableStatistics,
            Map<String, Type> columns)
    {
        for (TableHandle tableHandle : analysis.getTables()) {
            // read metadata to see if plan caching is supported by the connector
            try {
                if (metadata.isExecutionPlanCacheSupported(session, tableHandle)) {
                    tables.add(tableHandle.getFullyQualifiedName());
                    // includeColumnStatistics is passed as false, so that calculation of columnStatistics are skipped
                    tableStatistics.put(tableHandle.getFullyQualifiedName(), metadata.getTableStatistics(session, tableHandle, Constraint.alwaysTrue(), false)); // TODO: Find a way to get constraints instead of reading all table statistics

                    Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
                    for (ColumnHandle columnHandle : columnHandles.values()) {
                        ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, columnHandle);
                        columns.put(tableHandle.getFullyQualifiedName() + "." + columnHandle.getColumnName(), columnMetadata.getType());
                    }
                }
                else {
                    return false;
                }
            }
            catch (PrestoException e) {
                // TableStatistics constraint -- cannot query more than 1000 hive partitions
                return false;
            }
        }
        return true;
    }

    private boolean isEqualBasicStatistics(Map<String, TableStatistics> cacheTableStatistics, Map<String, TableStatistics> tableStatistics, List<String> tableNames)
    {
        for (String tableName : tableNames) {
            TableStatistics cacheTableStatisticsTemp = cacheTableStatistics.get(tableName);
            TableStatistics tableStatisticsTemp = tableStatistics.get(tableName);
            if (cacheTableStatisticsTemp == null ||
                    tableStatisticsTemp == null ||
                    cacheTableStatisticsTemp.getFileCount() != tableStatisticsTemp.getFileCount() ||
                    !cacheTableStatisticsTemp.getRowCount().equals(tableStatisticsTemp.getRowCount()) ||
                    cacheTableStatisticsTemp.getOnDiskDataSizeInBytes() != tableStatisticsTemp.getOnDiskDataSizeInBytes()) {
                return false;
            }
        }
        return true;
    }

    private boolean isCacheable(Statement statement)
    {
        // Skip cache when creating tables, hack for outdated metadata
        if (!(statement instanceof Query)) {
            return false;
        }

        try {
            // filter out create table statements and statements which contain CurrentX functions
            new StatementChecker().process(statement, null);
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
        return true;
    }

    private static Plan update(Plan currentPlan, PlanNode root)
    {
        // Rebuild Plan object to get a new plan ID
        return new Plan(root, currentPlan.getTypes(), currentPlan.getStatsAndCosts()); // TODO: need to update Types for parameter rewrite
    }

    private static class StatementChecker
            extends DefaultTraversalVisitor<Void, Void>
    {
        @Override
        protected Void visitCurrentPath(CurrentPath node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Void visitCurrentTime(CurrentTime node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Void visitCurrentUser(CurrentUser node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Void visitCreateTable(CreateTable node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Void visitCreateTableAsSelect(CreateTableAsSelect node, Void context)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TableHandleRewriter
            extends SimplePlanRewriter<Void>
    {
        private final TransactionId transactionId;

        private final Session session;
        private final Analysis analysis;
        private final Metadata metadata;
        private final Map<String, TableHandle> tables;
        private final Map<ConnectorTransactionHandle, ConnectorTransactionHandle> connectorTransactionHandleMap; // Old ConnectorTransactionHandle from cached plan to new ConnectorTransactionHandle from metadata
        private HashMap<UUID, UUID> reuseTableScanNewMappingIdMap;

        TableHandleRewriter(Session session, Analysis analysis, Metadata metadata)
        {
            this.transactionId = session.getTransactionId().get();
            this.session = session;
            this.analysis = analysis;
            this.metadata = metadata;
            connectorTransactionHandleMap = new HashMap<>();

            // A map of String fully qualified names to TableHandles for ease of access
            Map<String, TableHandle> tableHandleHashMap = new HashMap<>();
            for (TableHandle handle : analysis.getTables()) {
                tableHandleHashMap.put(handle.getFullyQualifiedName(), handle);
                analysis.getCubes(handle).forEach(cubeHandle -> {
                    tableHandleHashMap.putIfAbsent(cubeHandle.getFullyQualifiedName(), cubeHandle);
                });
            }
            this.tables = tableHandleHashMap;
            this.reuseTableScanNewMappingIdMap = new HashMap();
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            TableHandle tableHandle = node.getTable(); // old table handle
            TableHandle newTableHandle = toNewTableHandle(tableHandle, tables, transactionId, session, metadata);
            // Build mapping of cached ConnectorTransactionHandle to new ConnectorTransactionHandle so it can be replaced
            // in other nodes if necessary
            connectorTransactionHandleMap.put(tableHandle.getTransaction(), newTableHandle.getTransaction());

            UUID newMappingID = node.getReuseTableScanMappingId();
            if (node.getStrategy() != ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT) {
                newMappingID = reuseTableScanNewMappingIdMap.computeIfAbsent(newMappingID, k -> UUID.randomUUID());
            }

            // Return a new table handle with the ID, output symbols, assignments, and enforced constraints of the cached table handle
            return new TableScanNode(node.getId(), newTableHandle, node.getOutputSymbols(), node.getAssignments(), node.getEnforcedConstraint(),
                    node.getPredicate(), node.getStrategy(), newMappingID, node.getConsumerTableScanNodeCount(), false);
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context)
        {
            PartitioningHandle oldPartitioningHandle = node.getPartitioningScheme().getPartitioning().getHandle();
            if (oldPartitioningHandle.getTransactionHandle().isPresent()) {
                // Visit children nodes first to update their transaction handles
                List<PlanNode> children = node.getSources().stream()
                        .map(x -> context.defaultRewrite(x))
                        .collect(Collectors.toList());

                PartitioningHandle partitioningHandle = new PartitioningHandle(oldPartitioningHandle.getConnectorId(),
                        Optional.of(connectorTransactionHandleMap.get(oldPartitioningHandle.getTransactionHandle().get())),
                        oldPartitioningHandle.getConnectorHandle());

                List<Symbol> columns = new ArrayList<>();
                columns.addAll(node.getPartitioningScheme().getPartitioning().getColumns());

                Partitioning partitioning = Partitioning.create(partitioningHandle, columns);
                PartitioningScheme partitioningScheme = new PartitioningScheme(partitioning, node.getPartitioningScheme().getOutputLayout());
                System.out.println("New partitioning handle ID: " + partitioningHandle.getTransactionHandle().get().toString());
                return new ExchangeNode(node.getId(),
                        node.getType(),
                        node.getScope(),
                        partitioningScheme,
                        children,
                        node.getInputs(),
                        node.getOrderingScheme(),
                        node.getAggregationType());
            }
            else {
                return super.visitExchange(node, context);
            }
        }

        private static TableHandle toNewTableHandle(TableHandle oldTableHandle, Map<String, TableHandle> tables, TransactionId transactionId, Session session, Metadata metadata)
        {
            // Look up old table handle in the current session
            TableHandle newTableHandle = tables.get(oldTableHandle.getFullyQualifiedName());
            if (newTableHandle == null) {
                Optional<TableHandle> targetTable = metadata.getTableHandle(session, QualifiedObjectName.valueOf(oldTableHandle.getFullyQualifiedName()));
                if (!targetTable.isPresent()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Cached table is not found");
                }
                newTableHandle = targetTable.get();
            }

            // New connector transaction handle may not have the correct transaction ID so it is explicitly rewritten here
            return new TableHandle(oldTableHandle.getCatalogName(),
                    newTableHandle.getConnectorHandle().createFrom(oldTableHandle.getConnectorHandle()),
                    toNewConnectorTransactionHandle(newTableHandle, transactionId),
                    newTableHandle.getLayout());
        }

        private static ConnectorTransactionHandle toNewConnectorTransactionHandle(TableHandle tableHandle, TransactionId transactionId)
        {
            ConnectorTransactionHandle transactionHandle = tableHandle.getTransaction();
            if (transactionHandle instanceof GlobalSystemTransactionHandle) {
                return new GlobalSystemTransactionHandle(transactionId);
            }
            else if (transactionHandle instanceof SystemTransactionHandle) {
                return new SystemTransactionHandle(transactionId, toNewConnectorTransactionHandle(tableHandle, transactionId));
            }
            else if (transactionHandle instanceof InformationSchemaTransactionHandle) {
                return new InformationSchemaTransactionHandle(transactionId);
            }
            else {
                // By default this method returns the original transaction handle unless overwritten
                // Some connector transaction handles (such as hive) cannot be reused between queries
                return tableHandle.getTransaction();
            }
        }
    }

    private static List<PlanNode> getTableScanNodes(PlanNode planNode)
    {
        List<PlanNode> result = new LinkedList<>();

        Queue<PlanNode> queue = new LinkedList<>();
        queue.add(planNode);

        while (!queue.isEmpty()) {
            PlanNode node = queue.poll();
            if (node instanceof TableScanNode) {
                result.add(node);
            }

            queue.addAll(node.getSources());
        }

        return result;
    }

    private static boolean tablesMatch(PlanNode planNode, Collection<TableHandle> tableHandles)
    {
        List<PlanNode> planNodes = getTableScanNodes(planNode);
        Map<String, ConnectorTableHandle> cachedTableHandleMap = new HashMap<>();
        for (PlanNode root : planNodes) {
            TableScanNode tableScanNode = (TableScanNode) root;
            cachedTableHandleMap.put(tableScanNode.getTable().getConnectorHandle().getSchemaPrefixedTableName(), tableScanNode.getTable().getConnectorHandle());
        }
        for (TableHandle tableHandle : tableHandles) {
            if (!tableHandle.getConnectorHandle().basicEquals(cachedTableHandleMap.get(tableHandle.getConnectorHandle().getSchemaPrefixedTableName()))) {
                return false;
            }
        }
        return true;
    }
}
