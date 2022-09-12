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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.NodeInfo;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.cube.CubeManager;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.event.SplitMonitor;
import io.prestosql.eventlistener.EventListenerManager;
import io.prestosql.exchange.ExchangeHandleResolver;
import io.prestosql.exchange.ExchangeManagerRegistry;
import io.prestosql.execution.TestSqlTaskManager.MockExchangeClientSupplier;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.scheduler.LegacyNetworkTopology;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.index.IndexManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Split;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.operator.LookupJoinOperators;
import io.prestosql.operator.PagesIndex;
import io.prestosql.operator.index.IndexJoinLookupStats;
import io.prestosql.seedstore.SeedStoreManager;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spiller.GenericSpillerFactory;
import io.prestosql.split.PageSinkManager;
import io.prestosql.split.PageSourceManager;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.LocalExecutionPlanner;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.statestore.LocalStateStoreProvider;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.statestore.listener.StateStoreListenerManager;
import io.prestosql.testing.TestingMetadata.TestingColumnHandle;
import io.prestosql.testing.TestingSplit;
import io.prestosql.util.FinalizerService;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.prestosql.testing.TestingHandles.TEST_TABLE_HANDLE;

public final class TaskTestUtils
{
    private TaskTestUtils()
    {
    }

    public static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("tableScan");

    private static final CatalogName CONNECTOR_ID = TEST_TABLE_HANDLE.getCatalogName();

    public static final ScheduledSplit SPLIT = new ScheduledSplit(0, TABLE_SCAN_NODE_ID, new Split(CONNECTOR_ID, TestingSplit.createLocalSplit(), Lifespan.taskWide()));

    public static final ImmutableList<TaskSource> EMPTY_SOURCES = ImmutableList.of();

    public static final Symbol SYMBOL = new Symbol("column");

    public static final PlanFragment PLAN_FRAGMENT = new PlanFragment(
            new PlanFragmentId("fragment"),
            TableScanNode.newInstance(
                    TABLE_SCAN_NODE_ID,
                    TEST_TABLE_HANDLE,
                    ImmutableList.of(SYMBOL),
                    ImmutableMap.of(SYMBOL, new TestingColumnHandle("column", 0, BIGINT)),
                    ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                    new UUID(0, 0),
                    0,
                    false),
            ImmutableMap.of(SYMBOL, VARCHAR),
            SOURCE_DISTRIBUTION,
            ImmutableList.of(TABLE_SCAN_NODE_ID),
            new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(SYMBOL))
                    .withBucketToPartition(Optional.of(new int[1])),
            ungroupedExecution(),
            StatsAndCosts.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    public static LocalExecutionPlanner createTestingPlanner()
    {
        Metadata metadata = createTestMetadataManager();

        PageSourceManager pageSourceManager = new PageSourceManager();
        HetuMetaStoreManager hetuMetaStoreManager = new HetuMetaStoreManager();
        FeaturesConfig featuresConfig = new FeaturesConfig();
        CubeManager cubeManager = new CubeManager(featuresConfig, hetuMetaStoreManager);
        pageSourceManager.addConnectorPageSourceProvider(CONNECTOR_ID, new TestingPageSourceProvider());

        // we don't start the finalizer so nothing will be collected, which is ok for a test
        FinalizerService finalizerService = new FinalizerService();

        NodeScheduler nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                new InMemoryNodeManager(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService));
        NodePartitioningManager nodePartitioningManager = new NodePartitioningManager(nodeScheduler);

        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(metadata, 0);
        NodeInfo nodeInfo = new NodeInfo("test");

        FileSystemClientManager fileSystemClientManager = new FileSystemClientManager();
        SeedStoreManager seedStoreManager = new SeedStoreManager(fileSystemClientManager);
        StateStoreProvider stateStoreProvider = new LocalStateStoreProvider(seedStoreManager);
        HeuristicIndexerManager heuristicIndexerManager = new HeuristicIndexerManager(new FileSystemClientManager(), new HetuMetaStoreManager());

        return new LocalExecutionPlanner(
                metadata,
                new TypeAnalyzer(new SqlParser(), metadata),
                Optional.empty(),
                pageSourceManager,
                new IndexManager(),
                nodePartitioningManager,
                new PageSinkManager(),
                new MockExchangeClientSupplier(),
                new ExpressionCompiler(metadata, pageFunctionCompiler),
                pageFunctionCompiler,
                new JoinFilterFunctionCompiler(metadata),
                new IndexJoinLookupStats(),
                new TaskManagerConfig(),
                new GenericSpillerFactory((types, spillContext, memoryContext, isSingleSessionSpiller, isSnapshotEnabled, queryId, isSpillToHdfs) -> {
                    throw new UnsupportedOperationException();
                }),
                (types, spillContext, memoryContext, isSingleSessionSpiller, isSnapshotEnabled, queryId, isSpillToHdfs) -> {
                    throw new UnsupportedOperationException();
                },
                (types, partitionFunction, spillContext, memoryContext, isSingleSessionSpiller, isSnapshotEnabled, queryId) -> {
                    throw new UnsupportedOperationException();
                },
                new PagesIndex.TestingFactory(false),
                new JoinCompiler(metadata),
                new LookupJoinOperators(),
                new OrderingCompiler(),
                nodeInfo,
                stateStoreProvider,
                new StateStoreListenerManager(stateStoreProvider),
                new DynamicFilterCacheManager(),
                heuristicIndexerManager,
                cubeManager,
                new ExchangeManagerRegistry(new ExchangeHandleResolver()));
    }

    public static TaskInfo updateTask(SqlTask sqlTask, List<TaskSource> taskSources, OutputBuffers outputBuffers)
    {
        return sqlTask.updateTask(TEST_SESSION, Optional.of(PLAN_FRAGMENT), taskSources, outputBuffers, OptionalInt.empty(), Optional.empty(), null, 1);
    }

    public static SplitMonitor createTestSplitMonitor()
    {
        return new SplitMonitor(
                new EventListenerManager(),
                new ObjectMapperProvider().get());
    }
}
