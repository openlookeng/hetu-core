/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.utils;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.execution.MockRemoteTaskFactory;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.execution.StageId;
import io.prestosql.execution.TestSqlTaskManager;
import io.prestosql.execution.scheduler.SplitSchedulerStats;
import io.prestosql.failuredetector.NoOpFailureDetector;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.seedstore.SeedStoreManager;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.statestore.LocalStateStoreProvider;
import io.prestosql.testing.TestingMetadata;
import io.prestosql.testing.TestingTransactionHandle;
import io.prestosql.util.FinalizerService;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.SqlStageExecution.createSqlStageExecution;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.ARBITRARY;
import static io.prestosql.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.prestosql.metadata.AbstractMockMetadata.dummyMetadata;
import static io.prestosql.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class TestUtil
{
    private static NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());
    private static ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
    private static ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

    private TestUtil()
    {
    }

    public static SqlStageExecution getTestStage(RowExpression expression)
    {
        StageId stageId = new StageId(new QueryId("query"), 0);

        SqlStageExecution stage = createSqlStageExecution(
                stageId,
                new TestSqlTaskManager.MockLocationFactory().createStageLocation(stageId),
                createExchangePlanFragment(expression),
                new HashMap<>(),
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                executor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new DynamicFilterService(new LocalStateStoreProvider(
                        new SeedStoreManager(new FileSystemClientManager()))));
        stage.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY));

        return stage;
    }

    private static PlanFragment createExchangePlanFragment(RowExpression expr)
    {
        Symbol testSymbol = new Symbol("a");
        Map<Symbol, ColumnHandle> scanAssignments = ImmutableMap.<Symbol, ColumnHandle>builder()
                .put(testSymbol, new TestingMetadata.TestingColumnHandle("a"))
                .build();

        Map<Symbol, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(testSymbol)));
        TableScanNode tableScanNode = new TableScanNode(
                new PlanNodeId(UUID.randomUUID().toString()),
                makeTableHandle(TupleDomain.none()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.none(),
                Optional.empty(),
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                new UUID(0, 0),
                0,
                false);
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata());
        FilterNode filterNode = planBuilder.filter(expr, tableScanNode);

        PlanNode planNode = new LimitNode(
                new PlanNodeId("limit"),
                filterNode,
                1,
                false);

        ImmutableMap.Builder<Symbol, Type> types = ImmutableMap.builder();
        for (Symbol symbol : planNode.getOutputSymbols()) {
            types.put(symbol, VARCHAR);
        }
        return new PlanFragment(
                new PlanFragmentId("limit_fragment_id"),
                planNode,
                types.build(),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputSymbols()),
                ungroupedExecution(),
                StatsAndCosts.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static TableHandle makeTableHandle(TupleDomain<ColumnHandle> predicate)
    {
        return new TableHandle(
                new CatalogName("test"),
                new PredicatedTableHandle(predicate),
                TestingTransactionHandle.create(),
                Optional.empty());
    }

    private static class PredicatedTableHandle
            implements ConnectorTableHandle
    {
        private final TupleDomain<ColumnHandle> predicate;

        public PredicatedTableHandle(TupleDomain<ColumnHandle> predicate)
        {
            this.predicate = predicate;
        }

        public TupleDomain<ColumnHandle> getPredicate()
        {
            return predicate;
        }

        @Override
        public String getSchemaPrefixedTableName()
        {
            return null;
        }

        @Override
        public boolean isFilterSupported()
        {
            return true;
        }
    }
}
