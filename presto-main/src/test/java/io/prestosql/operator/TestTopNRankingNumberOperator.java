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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.prestosql.RowPagesBuilder;
import io.prestosql.operator.window.RankingFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.GroupByHashYieldAssertion.createPagesWithDistinctHashKeys;
import static io.prestosql.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEqualsWithStateComparison;
import static io.prestosql.operator.TopNRankingNumberOperator.TopNRankingNumberOperatorFactory;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestTopNRankingNumberOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;
    private JoinCompiler joinCompiler;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        joinCompiler = new JoinCompiler(createTestMetadataManager());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @DataProvider(name = "hashEnabledValues")
    public static Object[][] hashEnabledValuesProvider()
    {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider
    public Object[][] partial()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testPartitioned(boolean hashEnabled)
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT, DOUBLE);
        List<Page> input = rowPagesBuilder
                .row(1L, 0.1)
                .row(2L, 0.1)
                .row(3L, 0.1)
                .row(3L, 0.1)
                .pageBreak()
                .row(1L, 0.2)
                .pageBreak()
                .row(1L, 0.2)
                .row(1L, 0.2)
                .row(2L, 0.3)
                .row(2L, 0.4)
                .pageBreak()
                .row(2L, 0.3)
                .build();
        // row_number() over(partition by 0 order by 1) top 3
        TopNRankingNumberOperatorFactory operatorFactory = new TopNRankingNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(0),
                ImmutableList.of(BIGINT),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                false,
                Optional.empty(),
                10,
                joinCompiler,
                Optional.of(RankingFunction.ROW_NUMBER));
        MaterializedResult rowNumberExpected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(0.1, 1L, 1L)
                .row(0.2, 1L, 2L)
                .row(0.2, 1L, 3L)
                .row(0.1, 2L, 1L)
                .row(0.3, 2L, 2L)
                .row(0.3, 2L, 3L)
                .row(0.1, 3L, 1L)
                .row(0.1, 3L, 2L)
                .build();
        assertOperatorEquals(operatorFactory, driverContext, input, rowNumberExpected);
        // rank() over(partition by 0 order by 1) top 3
        operatorFactory = new TopNRankingNumberOperatorFactory(
                1,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(0),
                ImmutableList.of(BIGINT),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                false,
                Optional.empty(),
                10,
                joinCompiler,
                Optional.of(RankingFunction.RANK));
        MaterializedResult rankNumberExpected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(0.1, 1L, 1L)
                .row(0.2, 1L, 2L)
                .row(0.2, 1L, 2L)
                .row(0.2, 1L, 2L)
                .row(0.1, 2L, 1L)
                .row(0.3, 2L, 2L)
                .row(0.3, 2L, 2L)
                .row(0.1, 3L, 1L)
                .row(0.1, 3L, 1L)
                .build();
        assertOperatorEquals(operatorFactory, driverContext, input, rankNumberExpected);
        // dense_rank() over(partition by 0 order by 1) top 3
        operatorFactory = new TopNRankingNumberOperatorFactory(
                2,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(0),
                ImmutableList.of(BIGINT),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                false,
                Optional.empty(),
                10,
                joinCompiler,
                Optional.of(RankingFunction.DENSE_RANK));
        MaterializedResult denseRankNumberExpected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(0.1, 1L, 1L)
                .row(0.2, 1L, 2L)
                .row(0.2, 1L, 2L)
                .row(0.2, 1L, 2L)
                .row(0.1, 2L, 1L)
                .row(0.3, 2L, 2L)
                .row(0.3, 2L, 2L)
                .row(0.4, 2L, 3L)
                .row(0.1, 3L, 1L)
                .row(0.1, 3L, 1L)
                .build();
        assertOperatorEquals(operatorFactory, driverContext, input, denseRankNumberExpected);
    }

    @Test
    public void testPartitionedSnapshot()
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(true, Ints.asList(0), BIGINT, DOUBLE);
        List<Page> input = rowPagesBuilder
                .row(1L, 0.1)
                .row(2L, 0.1)
                .row(3L, 0.1)
                .row(3L, 0.1)
                .pageBreak()
                .row(1L, 0.2)
                .pageBreak()
                .row(1L, 0.2)
                .row(1L, 0.2)
                .row(2L, 0.3)
                .row(2L, 0.4)
                .pageBreak()
                .row(2L, 0.3)
                .build();
        // row_number() over(partition by 0 order by 1) top 3
        TopNRankingNumberOperatorFactory operatorFactory = new TopNRankingNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(0),
                ImmutableList.of(BIGINT),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                false,
                Optional.empty(),
                10,
                joinCompiler,
                Optional.of(RankingFunction.ROW_NUMBER));
        MaterializedResult rowNumberExpected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(0.1, 1L, 1L)
                .row(0.2, 1L, 2L)
                .row(0.2, 1L, 3L)
                .row(0.1, 2L, 1L)
                .row(0.3, 2L, 2L)
                .row(0.3, 2L, 3L)
                .row(0.1, 3L, 1L)
                .row(0.1, 3L, 2L)
                .build();
        assertOperatorEqualsWithStateComparison(operatorFactory, driverContext, input, rowNumberExpected, createExpectedMappingRestore());
        // rank() over(partition by 0 order by 1) top 3
        operatorFactory = new TopNRankingNumberOperatorFactory(
                1,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(0),
                ImmutableList.of(BIGINT),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                false,
                Optional.empty(),
                10,
                joinCompiler,
                Optional.of(RankingFunction.RANK));
        MaterializedResult rankNumberExpected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(0.1, 1L, 1L)
                .row(0.2, 1L, 2L)
                .row(0.2, 1L, 2L)
                .row(0.2, 1L, 2L)
                .row(0.1, 2L, 1L)
                .row(0.3, 2L, 2L)
                .row(0.3, 2L, 2L)
                .row(0.1, 3L, 1L)
                .row(0.1, 3L, 1L)
                .build();
        assertOperatorEqualsWithStateComparison(operatorFactory, driverContext, input, rankNumberExpected, createExpectedMappingRestore());
        // dense_rank() over(partition by 0 order by 1) top 3
        operatorFactory = new TopNRankingNumberOperatorFactory(
                2,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(0),
                ImmutableList.of(BIGINT),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                false,
                Optional.empty(),
                10,
                joinCompiler,
                Optional.of(RankingFunction.DENSE_RANK));
        MaterializedResult denseRankNumberExpected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(0.1, 1L, 1L)
                .row(0.2, 1L, 2L)
                .row(0.2, 1L, 2L)
                .row(0.2, 1L, 2L)
                .row(0.1, 2L, 1L)
                .row(0.3, 2L, 2L)
                .row(0.3, 2L, 2L)
                .row(0.4, 2L, 3L)
                .row(0.1, 3L, 1L)
                .row(0.1, 3L, 1L)
                .build();
        assertOperatorEqualsWithStateComparison(operatorFactory, driverContext, input, denseRankNumberExpected, createExpectedMappingRestoreDenseRank());
    }

    //For Rank and RowNumber RowHeap
    private Map<String, Object> createExpectedMappingRestore()
    {
        Map<String, Object> operatorSnapshotMapping = new HashMap<>();

        operatorSnapshotMapping.put("operatorContext", 0);
        operatorSnapshotMapping.put("localUserMemoryContext", 51816L);

        //TopNRankingNumberOperator.groupByHash
        Map<String, Object> groupByHashMapping = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        Map<String, Object> groupIds = new HashMap<>();
        Map<String, Object> valuesByGroupId = new HashMap<>();
        operatorSnapshotMapping.put("groupByHash", groupByHashMapping);
        groupByHashMapping.put("hashCapacity", 16);
        groupByHashMapping.put("maxFill", 12);
        groupByHashMapping.put("mask", 15);
        groupByHashMapping.put("values", values);
        groupByHashMapping.put("groupIds", groupIds);
        groupByHashMapping.put("nullGroupId", -1);
        groupByHashMapping.put("valuesByGroupId", valuesByGroupId);
        groupByHashMapping.put("nextGroupId", 3);
        groupByHashMapping.put("hashCollisions", 0L);
        groupByHashMapping.put("expectedHashCollisions", 0.0);
        groupByHashMapping.put("preallocatedMemoryInBytes", 0L);
        groupByHashMapping.put("currentPageSizeInBytes", 356L);
        values.put("array", long[][].class);
        values.put("capacity", 1024);
        values.put("segments", 1);
        groupIds.put("array", int[][].class);
        groupIds.put("capacity", 1024);
        groupIds.put("segments", 1);
        valuesByGroupId.put("array", long[][].class);
        valuesByGroupId.put("capacity", 1024);
        valuesByGroupId.put("segments", 1);

        //TopNRankingNumberOperator.groupedTopNBuilder
        Map<String, Object> groupedTopNBuilderMapping = new HashMap<>();
        Map<String, Object> groupedRowsMapping = new HashMap<>();
        Map<String, Object> pageReferencesMapping = new HashMap<>();
        List<Integer> emptyPageReferenceSlots = new ArrayList<>();
        operatorSnapshotMapping.put("groupedTopNBuilder", groupedTopNBuilderMapping);
        groupedTopNBuilderMapping.put("groupByHash", groupByHashMapping);
        groupedTopNBuilderMapping.put("groupedRows", groupedRowsMapping);
        groupedTopNBuilderMapping.put("pageReferences", pageReferencesMapping);
        groupedTopNBuilderMapping.put("emptyPageReferenceSlots", emptyPageReferenceSlots);
        groupedTopNBuilderMapping.put("memorySizeInBytes", 2128L);
        groupedTopNBuilderMapping.put("currentPageCount", 2);
        groupedRowsMapping.put("array", Object[][].class);
        groupedRowsMapping.put("capacity", 1024);
        groupedRowsMapping.put("segments", 1);
        pageReferencesMapping.put("array", Object[][].class);
        pageReferencesMapping.put("capacity", 1024);
        pageReferencesMapping.put("segments", 1);

        operatorSnapshotMapping.put("finishing", false);

        return operatorSnapshotMapping;
    }

    //For DenseRankRowHeap
    private Map<String, Object> createExpectedMappingRestoreDenseRank()
    {
        Map<String, Object> operatorSnapshotMapping = new HashMap<>();

        operatorSnapshotMapping.put("operatorContext", 0);
        operatorSnapshotMapping.put("localUserMemoryContext", 51812L);

        //TopNRankingNumberOperator.groupByHash
        Map<String, Object> groupByHashMapping = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        Map<String, Object> groupIds = new HashMap<>();
        Map<String, Object> valuesByGroupId = new HashMap<>();
        operatorSnapshotMapping.put("groupByHash", groupByHashMapping);
        groupByHashMapping.put("hashCapacity", 16);
        groupByHashMapping.put("maxFill", 12);
        groupByHashMapping.put("mask", 15);
        groupByHashMapping.put("values", values);
        groupByHashMapping.put("groupIds", groupIds);
        groupByHashMapping.put("nullGroupId", -1);
        groupByHashMapping.put("valuesByGroupId", valuesByGroupId);
        groupByHashMapping.put("nextGroupId", 3);
        groupByHashMapping.put("hashCollisions", 0L);
        groupByHashMapping.put("expectedHashCollisions", 0.0);
        groupByHashMapping.put("preallocatedMemoryInBytes", 0L);
        groupByHashMapping.put("currentPageSizeInBytes", 356L);
        values.put("array", long[][].class);
        values.put("capacity", 1024);
        values.put("segments", 1);
        groupIds.put("array", int[][].class);
        groupIds.put("capacity", 1024);
        groupIds.put("segments", 1);
        valuesByGroupId.put("array", long[][].class);
        valuesByGroupId.put("capacity", 1024);
        valuesByGroupId.put("segments", 1);

        //TopNRankingNumberOperator.groupedTopNBuilder
        Map<String, Object> groupedTopNBuilderMapping = new HashMap<>();
        Map<String, Object> groupedRowsMapping = new HashMap<>();
        Map<String, Object> pageReferencesMapping = new HashMap<>();
        List<Integer> emptyPageReferenceSlots = new ArrayList<>();
        operatorSnapshotMapping.put("groupedTopNBuilder", groupedTopNBuilderMapping);
        groupedTopNBuilderMapping.put("groupByHash", groupByHashMapping);
        groupedTopNBuilderMapping.put("groupedRows", groupedRowsMapping);
        groupedTopNBuilderMapping.put("pageReferences", pageReferencesMapping);
        groupedTopNBuilderMapping.put("emptyPageReferenceSlots", emptyPageReferenceSlots);
        groupedTopNBuilderMapping.put("memorySizeInBytes", 2124L);
        groupedTopNBuilderMapping.put("currentPageCount", 2);
        groupedRowsMapping.put("array", Object[][].class);
        groupedRowsMapping.put("capacity", 1024);
        groupedRowsMapping.put("segments", 1);
        pageReferencesMapping.put("array", Object[][].class);
        pageReferencesMapping.put("capacity", 1024);
        pageReferencesMapping.put("segments", 1);

        operatorSnapshotMapping.put("finishing", false);

        return operatorSnapshotMapping;
    }

    @Test(dataProvider = "partial")
    public void testUnPartitioned(boolean partial)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.1)
                .pageBreak()
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.1)
                .pageBreak()
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.1)
                .pageBreak()
                .row(2L, 0.9)
                .build();
        MaterializedResult expected;
        TopNRankingNumberOperatorFactory operatorFactory;
        //row_number() over(order by 1) Top4
        if (partial) {
            expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT)
                    .row(0.1, 3L)
                    .row(0.1, 3L)
                    .row(0.1, 3L)
                    .row(0.2, 2L)
                    .build();
        }
        else {
            expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                    .row(0.1, 3L, 1L)
                    .row(0.1, 3L, 2L)
                    .row(0.1, 3L, 3L)
                    .row(0.2, 2L, 4L)
                    .build();
        }
        operatorFactory = new TopNRankingNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(),
                ImmutableList.of(),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                4,
                partial,
                Optional.empty(),
                10,
                joinCompiler,
                Optional.of(RankingFunction.ROW_NUMBER));

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
        //rank() over(order by 1) Top4
        if (partial) {
            expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT)
                    .row(0.1, 3L)
                    .row(0.1, 3L)
                    .row(0.1, 3L)
                    .row(0.2, 2L)
                    .row(0.2, 2L)
                    .row(0.2, 2L)
                    .build();
        }
        else {
            expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                    .row(0.1, 3L, 1L)
                    .row(0.1, 3L, 1L)
                    .row(0.1, 3L, 1L)
                    .row(0.2, 2L, 4L)
                    .row(0.2, 2L, 4L)
                    .row(0.2, 2L, 4L)
                    .build();
        }
        operatorFactory = new TopNRankingNumberOperatorFactory(
                1,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(),
                ImmutableList.of(),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                4,
                partial,
                Optional.empty(),
                10,
                joinCompiler,
                Optional.of(RankingFunction.RANK));

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
        //dense_rank() over(order by 1) Top4
        if (partial) {
            expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT)
                    .row(0.1, 3L)
                    .row(0.1, 3L)
                    .row(0.1, 3L)
                    .row(0.2, 2L)
                    .row(0.2, 2L)
                    .row(0.2, 2L)
                    .row(0.3, 1L)
                    .row(0.3, 1L)
                    .row(0.3, 1L)
                    .row(0.9, 2L)
                    .build();
        }
        else {
            expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                    .row(0.1, 3L, 1L)
                    .row(0.1, 3L, 1L)
                    .row(0.1, 3L, 1L)
                    .row(0.2, 2L, 2L)
                    .row(0.2, 2L, 2L)
                    .row(0.2, 2L, 2L)
                    .row(0.3, 1L, 3L)
                    .row(0.3, 1L, 3L)
                    .row(0.3, 1L, 3L)
                    .row(0.9, 2L, 4L)
                    .build();
        }
        operatorFactory = new TopNRankingNumberOperatorFactory(
                2,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(),
                ImmutableList.of(),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                4,
                partial,
                Optional.empty(),
                10,
                joinCompiler,
                Optional.of(RankingFunction.DENSE_RANK));

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testMemoryReservationYield()
    {
        Type type = BIGINT;
        List<Page> input = createPagesWithDistinctHashKeys(type, 6_000, 600);

        OperatorFactory operatorFactory = new TopNRankingNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(type),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(type),
                Ints.asList(0),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                false,
                Optional.empty(),
                10,
                joinCompiler,
                Optional.of(RankingFunction.ROW_NUMBER));

        // get result with yield; pick a relatively small buffer for heaps
        GroupByHashYieldAssertion.GroupByHashYieldResult result = finishOperatorWithYieldingGroupByHash(
                input,
                type,
                operatorFactory,
                operator -> ((TopNRankingNumberOperator) operator).getCapacity(),
                1_000_000);
        assertGreaterThan(result.getYieldCount(), 3);
        assertGreaterThan(result.getMaxReservedBytes(), 5L << 20);

        int count = 0;
        for (Page page : result.getOutput()) {
            assertEquals(page.getChannelCount(), 2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertEquals(page.getBlock(1).getByte(i, 0), 1);
                count++;
            }
        }
        assertEquals(count, 6_000 * 600);
    }
}
