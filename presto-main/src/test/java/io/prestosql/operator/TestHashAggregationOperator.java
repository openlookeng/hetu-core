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
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.prestosql.ExceededMemoryLimitException;
import io.prestosql.RowPagesBuilder;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.aggregation.builder.AggregationBuilder;
import io.prestosql.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.prestosql.operator.aggregation.partial.PartialAggregationController;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.plan.AggregationNode.Step;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.Spiller;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingTaskContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.block.BlockAssertions.createRLEBlock;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.GroupByHashYieldAssertion.GroupByHashYieldResult;
import static io.prestosql.operator.GroupByHashYieldAssertion.createPagesWithDistinctHashKeys;
import static io.prestosql.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static io.prestosql.operator.OperatorAssertion.assertPagesEqualIgnoreOrder;
import static io.prestosql.operator.OperatorAssertion.dropChannel;
import static io.prestosql.operator.OperatorAssertion.toMaterializedResult;
import static io.prestosql.operator.OperatorAssertion.toPages;
import static io.prestosql.operator.OperatorAssertion.toPagesCompareStateSimple;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.plan.AggregationNode.Step.PARTIAL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestHashAggregationOperator
{
    private static final Metadata metadata = createTestMetadataManager();

    private static final InternalAggregationFunction LONG_AVERAGE = metadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("avg"), AGGREGATE, DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction LONG_SUM = metadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("sum"), AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction COUNT = metadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE, BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction LONG_MIN = metadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("min"), AGGREGATE, BIGINT.getTypeSignature()));

    private static final int MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024;

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private final JoinCompiler joinCompiler = new JoinCompiler(createTestMetadataManager());
    private DummySpillerFactory spillerFactory;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        spillerFactory = new DummySpillerFactory();
    }

    @DataProvider(name = "hashEnabled")
    public static Object[][] hashEnabled()
    {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider(name = "hashEnabledAndMemoryLimitForMergeValues")
    public static Object[][] hashEnabledAndMemoryLimitForMergeValuesProvider()
    {
        return new Object[][] {
                {true, true, true, 8, Integer.MAX_VALUE},
                {true, true, false, 8, Integer.MAX_VALUE},
                {false, false, false, 0, 0},
                {false, true, true, 0, 0},
                {false, true, false, 0, 0},
                {false, true, true, 8, 0},
                {false, true, false, 8, 0},
                {false, true, true, 8, Integer.MAX_VALUE},
                {false, true, false, 8, Integer.MAX_VALUE}};
    }

    @DataProvider
    public Object[][] dataType()
    {
        return new Object[][] {{VARCHAR}, {BIGINT}};
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        spillerFactory = null;
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test(dataProvider = "hashEnabledAndMemoryLimitForMergeValues")
    public void testHashAggregation(boolean hashEnabled, boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        // make operator produce multiple pages during finish phase
        int numberOfRows = 40_000;
        Metadata localMetadata = createTestMetadataManager();
        InternalAggregationFunction countVarcharColumn = localMetadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.VARCHAR)));
        InternalAggregationFunction countBooleanColumn = localMetadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BOOLEAN)));
        InternalAggregationFunction maxVarcharColumn = localMetadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)));
        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN);
        List<Page> input = rowPagesBuilder
                .addSequencePage(numberOfRows, 100, 0, 100_000, 0, 500)
                .addSequencePage(numberOfRows, 100, 0, 200_000, 0, 500)
                .addSequencePage(numberOfRows, 100, 0, 300_000, 0, 500)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR),
                hashChannels,
                ImmutableList.of(),
                Step.SINGLE,
                false,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                        LONG_SUM.bind(ImmutableList.of(3), Optional.empty()),
                        LONG_AVERAGE.bind(ImmutableList.of(3), Optional.empty()),
                        maxVarcharColumn.bind(ImmutableList.of(2), Optional.empty()),
                        countVarcharColumn.bind(ImmutableList.of(0), Optional.empty()),
                        countBooleanColumn.bind(ImmutableList.of(4), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                spillEnabled,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                false,
                Optional.empty());

        DriverContext driverContext = createDriverContext(memoryLimitForMerge);

        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row(Integer.toString(i), 3L, 3L * i, (double) i, Integer.toString(300_000 + i), 3L, 3L);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
        assertGreaterThan(pages.size(), 1, "Expected more than one output page");
        assertPagesEqualIgnoreOrder(driverContext, pages, expected, hashEnabled, Optional.of(hashChannels.size()));

        assertTrue(spillEnabled == (spillerFactory.getSpillsCount() > 0), format("Spill state mismatch. Expected spill: %s, spill count: %s", spillEnabled, spillerFactory.getSpillsCount()));
    }

    @Test
    public void testHashAggregationSnapshot()
    {
        // make operator produce multiple pages during finish phase
        long memoryLimitForMerge = 0;
        long memoryLimitForMergeWithMemory = 0;
        int numberOfRows = 40_000;
        Metadata localMetadata = createTestMetadataManager();
        InternalAggregationFunction countVarcharColumn = localMetadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.VARCHAR)));
        InternalAggregationFunction countBooleanColumn = localMetadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BOOLEAN)));
        InternalAggregationFunction maxVarcharColumn = localMetadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)));
        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(false, hashChannels, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN);
        List<Page> input = rowPagesBuilder
                .addSequencePage(numberOfRows, 100, 0, 100_000, 0, 500)
                .addSequencePage(numberOfRows, 100, 0, 200_000, 0, 500)
                .addSequencePage(numberOfRows, 100, 0, 300_000, 0, 500)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR),
                hashChannels,
                ImmutableList.of(),
                Step.SINGLE,
                false,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                        LONG_SUM.bind(ImmutableList.of(3), Optional.empty()),
                        LONG_AVERAGE.bind(ImmutableList.of(3), Optional.empty()),
                        maxVarcharColumn.bind(ImmutableList.of(2), Optional.empty()),
                        countVarcharColumn.bind(ImmutableList.of(0), Optional.empty()),
                        countBooleanColumn.bind(ImmutableList.of(4), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                false,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                false,
                Optional.empty());

        DriverContext driverContext = createDriverContext(memoryLimitForMerge);

        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row(Integer.toString(i), 3L, 3L * i, (double) i, Integer.toString(300_000 + i), 3L, 3L);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPagesCompareStateSimple(operatorFactory, driverContext, input, false, createExpectedMapping());

        assertGreaterThan(pages.size(), 1, "Expected more than one output page");
        assertPagesEqualIgnoreOrder(driverContext, pages, expected, false, Optional.of(hashChannels.size()));

        assertFalse(spillerFactory.getSpillsCount() > 0, format("Spill state mismatch. Expected spill: %s, spill count: %s", false, spillerFactory.getSpillsCount()));
    }

    private Map<String, Object> createExpectedMapping()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        Map<String, Object> hashCollisionsCounterMapping = new HashMap<>();
        Map<String, Object> aggregationBuilderMapping = new HashMap<>();
        Map<String, Object> groupByHashMapping = new HashMap<>();
        List<Map<String, Object>> aggregatorsMapping = new ArrayList<>();
        Map<String, Object> currentPageBuilderMapping = new HashMap<>();
        Map<String, Object> groupAddressByGroupIdMapping = new HashMap<>();

        Map<String, Object> aggregator0Mapping = new HashMap<>();
        Map<String, Object> aggregator1Mapping = new HashMap<>();
        Map<String, Object> aggregator2Mapping = new HashMap<>();
        Map<String, Object> aggregator3Mapping = new HashMap<>();
        Map<String, Object> aggregator4Mapping = new HashMap<>();
        Map<String, Object> aggregator5Mapping = new HashMap<>();

        List<Map<String, Object>> blockBuildersMapping = new ArrayList<>();

        List<List<Object>> aggregator0AggregationMapping = new ArrayList<>();
        List<List<Object>> aggregator1AggregationMapping = new ArrayList<>();
        List<List<Object>> aggregator2AggregationMapping = new ArrayList<>();
        List<List<Object>> aggregator3AggregationMapping = new ArrayList<>();
        List<List<Object>> aggregator4AggregationMapping = new ArrayList<>();
        List<List<Object>> aggregator5AggregationMapping = new ArrayList<>();

        Map<String, Object> variableWidthBlockBuilderMapping = new HashMap<>();

        List<Object> channelBuildersMapping = new ArrayList<>();
        channelBuildersMapping.add(new ArrayList<>());

        //TODO-cp-I2DSGQ: change expectedMapping after implementation of operatorContext capture
        expectedMapping.put("operatorContext", 0);
        expectedMapping.put("aggregationBuilder", aggregationBuilderMapping);
        expectedMapping.put("memoryContext", 10675419L);
        expectedMapping.put("inputProcessed", true);
        expectedMapping.put("finishing", false);
        expectedMapping.put("finished", false);

        hashCollisionsCounterMapping.put("hashCollisions", 0L);
        hashCollisionsCounterMapping.put("expectedHashCollisions", 0.0);

        aggregationBuilderMapping.put("aggregators", aggregatorsMapping);
        aggregationBuilderMapping.put("full", false);

        aggregatorsMapping.add(aggregator0Mapping);
        aggregatorsMapping.add(aggregator1Mapping);
        aggregatorsMapping.add(aggregator2Mapping);
        aggregatorsMapping.add(aggregator3Mapping);
        aggregatorsMapping.add(aggregator4Mapping);
        aggregatorsMapping.add(aggregator5Mapping);

        currentPageBuilderMapping.put("blockBuilders", blockBuildersMapping);
        currentPageBuilderMapping.put("pageBuilderStatus", 388890L);
        currentPageBuilderMapping.put("declaredPositions", 40000);

        groupAddressByGroupIdMapping.put("array", long[][].class);
        groupAddressByGroupIdMapping.put("capacity", 197632);
        groupAddressByGroupIdMapping.put("segments", 193);

        aggregator0Mapping.put("step", "SINGLE");
        aggregator0Mapping.put("aggregation", aggregator0AggregationMapping);
        aggregator1Mapping.put("step", "SINGLE");
        aggregator1Mapping.put("aggregation", aggregator1AggregationMapping);
        aggregator2Mapping.put("step", "SINGLE");
        aggregator2Mapping.put("aggregation", aggregator2AggregationMapping);
        aggregator3Mapping.put("step", "SINGLE");
        aggregator3Mapping.put("aggregation", aggregator3AggregationMapping);
        aggregator4Mapping.put("step", "SINGLE");
        aggregator4Mapping.put("aggregation", aggregator4AggregationMapping);
        aggregator5Mapping.put("step", "SINGLE");
        aggregator5Mapping.put("aggregation", aggregator5AggregationMapping);

        blockBuildersMapping.add(variableWidthBlockBuilderMapping);

        Map<String, Object> aggregation0Array0 = new HashMap<>();
        List<Object> aggregation0StateList = new ArrayList<>();
        aggregation0StateList.add(aggregation0Array0);
        aggregation0Array0.put("array", long[][].class);
        aggregation0Array0.put("capacity", 40960);
        aggregation0Array0.put("segments", 40);
        aggregation0StateList.add(39999L);
        aggregator0AggregationMapping.add(aggregation0StateList);

        Map<String, Object> aggregation1Array0 = new HashMap<>();
        Map<String, Object> aggregation1Array1 = new HashMap<>();
        List<Object> aggregation1StateList = new ArrayList<>();
        aggregation1StateList.add(aggregation1Array0);
        aggregation1StateList.add(aggregation1Array1);
        aggregation1Array0.put("array", long[][].class);
        aggregation1Array0.put("capacity", 40960);
        aggregation1Array0.put("segments", 40);
        aggregation1Array1.put("array", boolean[][].class);
        aggregation1Array1.put("capacity", 40960);
        aggregation1Array1.put("segments", 40);
        aggregation1StateList.add(39999L);
        aggregator1AggregationMapping.add(aggregation1StateList);

        Map<String, Object> aggregation2Array0 = new HashMap<>();
        Map<String, Object> aggregation2Array1 = new HashMap<>();
        List<Object> aggregation2StateList = new ArrayList<>();
        aggregation2StateList.add(aggregation2Array0);
        aggregation2StateList.add(aggregation2Array1);
        aggregation2Array0.put("array", double[][].class);
        aggregation2Array0.put("capacity", 40960);
        aggregation2Array0.put("segments", 40);
        aggregation2Array1.put("array", long[][].class);
        aggregation2Array1.put("capacity", 40960);
        aggregation2Array1.put("segments", 40);
        aggregation2StateList.add(39999L);
        aggregator2AggregationMapping.add(aggregation2StateList);

        Map<String, Object> aggregation3Array0 = new HashMap<>();
        Map<String, Object> aggregation3Array1 = new HashMap<>();
        Map<String, Object> blockObjectBigArrayState = new HashMap<>();
        List<Object> aggregation3StateList = new ArrayList<>();
        aggregation3StateList.add(aggregation3Array0);
        aggregation3StateList.add(aggregation3Array1);
        aggregation3Array0.put("array", int[][].class);
        aggregation3Array0.put("capacity", 40960);
        aggregation3Array0.put("segments", 40);
        aggregation3Array1.put("array", blockObjectBigArrayState);
        aggregation3Array1.put("sizeOfBlocks", 2824108L);
        blockObjectBigArrayState.put("array", 1);
        blockObjectBigArrayState.put("capacity", 40960);
        aggregation3StateList.add(39999L);
        aggregator3AggregationMapping.add(aggregation3StateList);

        Map<String, Object> aggregation4Array0 = new HashMap<>();
        List<Object> aggregation4StateList = new ArrayList<>();
        aggregation4StateList.add(aggregation4Array0);
        aggregation4Array0.put("array", long[][].class);
        aggregation4Array0.put("capacity", 40960);
        aggregation4Array0.put("segments", 40);
        aggregation4StateList.add(39999L);
        aggregator4AggregationMapping.add(aggregation4StateList);

        Map<String, Object> aggregation5Array0 = new HashMap<>();
        List<Object> aggregation5StateList = new ArrayList<>();
        aggregation5StateList.add(aggregation5Array0);
        aggregation5Array0.put("array", long[][].class);
        aggregation5Array0.put("capacity", 40960);
        aggregation5Array0.put("segments", 40);
        aggregation5StateList.add(39999L);
        aggregator5AggregationMapping.add(aggregation5StateList);

        Map<String, Object> variableWidthBlockBuilderStatusMapping = new HashMap<>();
        variableWidthBlockBuilderMapping.put("blockBuilderStatus", variableWidthBlockBuilderStatusMapping);
        variableWidthBlockBuilderMapping.put("initialized", true);
        variableWidthBlockBuilderMapping.put("initialEntryCount", 8);
        variableWidthBlockBuilderMapping.put("initialSliceOutputSize", 256);
        variableWidthBlockBuilderMapping.put("sliceOutput", byte[].class);
        variableWidthBlockBuilderMapping.put("hasNullValue", false);
        variableWidthBlockBuilderMapping.put("valueIsNull", boolean[].class);
        variableWidthBlockBuilderMapping.put("offsets", int[].class);
        variableWidthBlockBuilderMapping.put("positions", 40000);
        variableWidthBlockBuilderMapping.put("currentEntrySize", 0);
        variableWidthBlockBuilderMapping.put("arraysRetainedSizeInBytes", 209991L);

        variableWidthBlockBuilderStatusMapping.put("pageBuilderStatus", 388890L);
        variableWidthBlockBuilderStatusMapping.put("currentSize", 388890);

        return expectedMapping;
    }

    @Test(dataProvider = "hashEnabledAndMemoryLimitForMergeValues")
    public void testHashAggregationWithGlobals(boolean hashEnabled, boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        Metadata localMetadata = createTestMetadataManager();
        InternalAggregationFunction countVarcharColumn = localMetadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.VARCHAR)));
        InternalAggregationFunction countBooleanColumn = localMetadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BOOLEAN)));
        InternalAggregationFunction maxVarcharColumn = localMetadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)));

        Optional<Integer> groupIdChannel = Optional.of(1);
        List<Integer> groupByChannels = Ints.asList(1, 2);
        List<Integer> globalAggregationGroupIds = Ints.asList(42, 49);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, groupByChannels, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN);
        List<Page> input = rowPagesBuilder.build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                groupByChannels,
                globalAggregationGroupIds,
                Step.SINGLE,
                true,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                        LONG_SUM.bind(ImmutableList.of(4), Optional.empty()),
                        LONG_AVERAGE.bind(ImmutableList.of(4), Optional.empty()),
                        maxVarcharColumn.bind(ImmutableList.of(2), Optional.empty()),
                        countVarcharColumn.bind(ImmutableList.of(0), Optional.empty()),
                        countBooleanColumn.bind(ImmutableList.of(5), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                groupIdChannel,
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                spillEnabled,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                false,
                Optional.empty());

        DriverContext driverContext = createDriverContext(memoryLimitForMerge);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT)
                .row(null, 42L, 0L, null, null, null, 0L, 0L)
                .row(null, 49L, 0L, null, null, null, 0L, 0L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, hashEnabled, Optional.of(groupByChannels.size()), revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "hashEnabledAndMemoryLimitForMergeValues")
    public void testHashAggregationMemoryReservation(boolean hashEnabled, boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        Metadata localMetadata = createTestMetadataManager();
        InternalAggregationFunction arrayAggColumn = localMetadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("array_agg"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BIGINT)));

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, BIGINT, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100, 0)
                .addSequencePage(10, 200, 0)
                .addSequencePage(10, 300, 0)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(10, Unit.MEGABYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                Step.SINGLE,
                true,
                ImmutableList.of(arrayAggColumn.bind(ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                spillEnabled,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                false,
                Optional.empty());

        Operator operator = operatorFactory.createOperator(driverContext);
        toPages(operator, input.iterator(), revokeMemoryWhenAddingPages);
        assertEquals(operator.getOperatorContext().getOperatorStats().getUserMemoryReservation().toBytes(), 0);
    }

    @Test(dataProvider = "hashEnabled", expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded per-node user memory limit of 10B.*")
    public void testMemoryLimit(boolean hashEnabled)
    {
        Metadata localMetadata = createTestMetadataManager();
        InternalAggregationFunction maxVarcharColumn = localMetadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)));

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR, BIGINT, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100, 0, 100, 0)
                .addSequencePage(10, 100, 0, 200, 0)
                .addSequencePage(10, 100, 0, 300, 0)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(10, Unit.BYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                        LONG_SUM.bind(ImmutableList.of(3), Optional.empty()),
                        LONG_AVERAGE.bind(ImmutableList.of(3), Optional.empty()),
                        maxVarcharColumn.bind(ImmutableList.of(2), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                joinCompiler,
                false,
                Optional.empty());

        toPages(operatorFactory, driverContext, input);
    }

    @Test(dataProvider = "hashEnabledAndMemoryLimitForMergeValues")
    public void testHashBuilderResize(boolean hashEnabled, boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1, MAX_BLOCK_SIZE_IN_BYTES);
        VARCHAR.writeSlice(builder, Slices.allocate(200_000)); // this must be larger than MAX_BLOCK_SIZE_IN_BYTES, 64K
        builder.build();

        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100)
                .addBlocksPage(builder.build())
                .addSequencePage(10, 100)
                .build();

        DriverContext driverContext = createDriverContext(memoryLimitForMerge);

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR),
                hashChannels,
                ImmutableList.of(),
                Step.SINGLE,
                false,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                spillEnabled,
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                false,
                Optional.empty());

        toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "dataType")
    public void testMemoryReservationYield(Type type)
    {
        List<Page> input = createPagesWithDistinctHashKeys(type, 6_000, 600);
        OperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(type),
                ImmutableList.of(0),
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty())),
                Optional.of(1),
                Optional.empty(),
                1,
                Optional.of(new DataSize(16, MEGABYTE)),
                joinCompiler,
                false,
                Optional.empty());

        // get result with yield; pick a relatively small buffer for aggregator's memory usage
        GroupByHashYieldResult result;
        result = finishOperatorWithYieldingGroupByHash(input, type, operatorFactory, this::getHashCapacity, 1_400_000);
        assertGreaterThan(result.getYieldCount(), 5);
        assertGreaterThan(result.getMaxReservedBytes(), 20L << 20);

        int count = 0;
        for (Page page : result.getOutput()) {
            // value + hash + aggregation result
            assertEquals(page.getChannelCount(), 3);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertEquals(page.getBlock(2).getLong(i, 0), 1);
                count++;
            }
        }
        assertEquals(count, 6_000 * 600);
    }

    @Test(dataProvider = "hashEnabled", expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded per-node user memory limit of 3MB.*")
    public void testHashBuilderResizeLimit(boolean hashEnabled)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1, MAX_BLOCK_SIZE_IN_BYTES);
        VARCHAR.writeSlice(builder, Slices.allocate(5_000_000)); // this must be larger than MAX_BLOCK_SIZE_IN_BYTES, 64K
        builder.build();

        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100)
                .addBlocksPage(builder.build())
                .addSequencePage(10, 100)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(3, MEGABYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR),
                hashChannels,
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                joinCompiler,
                false,
                Optional.empty());

        toPages(operatorFactory, driverContext, input);
    }

    @Test(dataProvider = "hashEnabled")
    public void testMultiSliceAggregationOutput(boolean hashEnabled)
    {
        // estimate the number of entries required to create 1.5 pages of results
        // See InMemoryHashAggregationBuilder.buildTypes()
        int fixedWidthSize = SIZE_OF_LONG + SIZE_OF_LONG +  // Used by BigintGroupByHash, see BigintGroupByHash.TYPES_WITH_RAW_HASH
                SIZE_OF_LONG + SIZE_OF_DOUBLE;              // Used by COUNT and LONG_AVERAGE aggregators;
        int multiSlicePositionCount = (int) (1.5 * PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES / fixedWidthSize);

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, BIGINT, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(multiSlicePositionCount, 0, 0)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                        LONG_AVERAGE.bind(ImmutableList.of(1), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                joinCompiler,
                false,
                Optional.empty());

        assertEquals(toPages(operatorFactory, createDriverContext(), input).size(), 2);
    }

    @Test(dataProvider = "hashEnabled")
    public void testMultiplePartialFlushes(boolean hashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(500, 0)
                .addSequencePage(500, 500)
                .addSequencePage(500, 1000)
                .addSequencePage(500, 1500)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                PARTIAL,
                ImmutableList.of(LONG_SUM.bind(ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(1, KILOBYTE)),
                joinCompiler,
                true,
                Optional.empty());

        DriverContext driverContext = createDriverContext(1024);

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            List<Page> expectedPages = rowPagesBuilder(BIGINT, BIGINT)
                    .addSequencePage(2000, 0, 0)
                    .build();
            MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT)
                    .pages(expectedPages)
                    .build();

            Iterator<Page> inputIterator = input.iterator();

            // Fill up the aggregation
            while (operator.needsInput() && inputIterator.hasNext()) {
                operator.addInput(inputIterator.next());
            }

            assertThat(driverContext.getSystemMemoryUsage()).isGreaterThan(0);
            assertEquals(driverContext.getMemoryUsage(), 0);

            // Drain the output (partial flush)
            List<Page> outputPages = new ArrayList<>();
            while (true) {
                Page output = operator.getOutput();
                if (output == null) {
                    break;
                }
                outputPages.add(output);
            }

            // There should be some pages that were drained
            assertTrue(!outputPages.isEmpty());

            // The operator need input again since this was a partial flush
            assertTrue(operator.needsInput());

            // Now, drive the operator to completion
            outputPages.addAll(toPages(operator, inputIterator));

            MaterializedResult actual;
            if (hashEnabled) {
                // Drop the hashChannel for all pages
                outputPages = dropChannel(outputPages, ImmutableList.of(1));
            }
            actual = toMaterializedResult(operator.getOperatorContext().getSession(), expected.getTypes(), outputPages);

            assertEquals(actual.getTypes(), expected.getTypes());
            assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
        }

        assertEquals(driverContext.getSystemMemoryUsage(), 0);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }

    @Test
    public void testMergeWithMemorySpill()
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(BIGINT);

        int smallPagesSpillThresholdSize = 150000;

        List<Page> input = rowPagesBuilder
                .addSequencePage(smallPagesSpillThresholdSize, 0)
                .addSequencePage(10, smallPagesSpillThresholdSize)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(),
                Step.SINGLE,
                false,
                ImmutableList.of(LONG_SUM.bind(ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                1,
                Optional.of(new DataSize(16, MEGABYTE)),
                true,
                new DataSize(smallPagesSpillThresholdSize, Unit.BYTE),
                succinctBytes(Integer.MAX_VALUE),
                spillerFactory,
                joinCompiler,
                false,
                Optional.empty());

        DriverContext driverContext = createDriverContext(smallPagesSpillThresholdSize);

        MaterializedResult.Builder resultBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT);
        for (int i = 0; i < smallPagesSpillThresholdSize + 10; ++i) {
            resultBuilder.row((long) i, (long) i);
        }

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, resultBuilder.build());
    }

    @Test
    public void testSpillerFailure()
    {
        Metadata localMetadata = createTestMetadataManager();
        InternalAggregationFunction maxVarcharColumn = localMetadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)));
        List<Integer> hashChannels = Ints.asList(1);
        ImmutableList<Type> types = ImmutableList.of(VARCHAR, BIGINT, VARCHAR, BIGINT);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(false, hashChannels, types);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100, 0, 100, 0)
                .addSequencePage(10, 100, 0, 200, 0)
                .addSequencePage(10, 100, 0, 300, 0)
                .build();

        DriverContext driverContext = TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setQueryMaxMemory(DataSize.valueOf("7MB"))
                .setMemoryPoolSize(DataSize.valueOf("1GB"))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                Step.SINGLE,
                false,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                        LONG_SUM.bind(ImmutableList.of(3), Optional.empty()),
                        LONG_AVERAGE.bind(ImmutableList.of(3), Optional.empty()),
                        maxVarcharColumn.bind(ImmutableList.of(2), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                true,
                succinctBytes(8),
                succinctBytes(Integer.MAX_VALUE),
                new FailingSpillerFactory(),
                joinCompiler,
                false,
                Optional.empty());

        try {
            toPages(operatorFactory, driverContext, input);
            fail("An exception was expected");
        }
        catch (RuntimeException expected) {
            if (!nullToEmpty(expected.getMessage()).matches(".* Failed to spill")) {
                fail("Exception other than expected was thrown", expected);
            }
        }
    }

    @Test
    public void testMemoryTracking()
            throws Exception
    {
        testMemoryTracking(false);
        testMemoryTracking(true);
    }

    private void testMemoryTracking(boolean useSystemMemory)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(false, hashChannels, BIGINT);
        Page input = getOnlyElement(rowPagesBuilder.addSequencePage(500, 0).build());

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(LONG_SUM.bind(ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                joinCompiler,
                useSystemMemory,
                Optional.empty());

        DriverContext driverContext = createDriverContext(1024);

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            assertTrue(operator.needsInput());
            operator.addInput(input);

            if (useSystemMemory) {
                assertThat(driverContext.getSystemMemoryUsage()).isGreaterThan(0);
                assertEquals(driverContext.getMemoryUsage(), 0);
            }
            else {
                assertEquals(driverContext.getSystemMemoryUsage(), 0);
                assertThat(driverContext.getMemoryUsage()).isGreaterThan(0);
            }

            toPages(operator, emptyIterator());
        }

        assertEquals(driverContext.getSystemMemoryUsage(), 0);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }

    @Test(dataProvider = "hashEnabled")
    public void testAdaptivePartialAggregation()
    {
        List<Integer> hashChannels = Ints.asList(0);

        PartialAggregationController partialAggregationController = new PartialAggregationController(5, 0.8);
        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                PARTIAL,
                ImmutableList.of(LONG_MIN.bind(ImmutableList.of(0), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                100,
                Optional.of(new DataSize(1, BYTE)), // this setting makes operator to flush after each page
                joinCompiler,
                false,
                // use 5 rows threshold to trigger adaptive partial aggregation after each page flush
                Optional.of(partialAggregationController));

        // at the start partial aggregation is enabled
        assertFalse(partialAggregationController.isPartialAggregationDisabled());
        // First operator will trigger adaptive partial aggregation after the first page
        List<Page> operator1Input = rowPagesBuilder(false, hashChannels, BIGINT)
                .addBlocksPage(createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 8, 8)) // first page will be hashed but the values are almost unique, so it will trigger adaptation
                .addBlocksPage(createRLEBlock(1, 10)) // second page would be hashed to existing value 1. but if adaptive PA kicks in, the raw values will be passed on
                .build();
        List<Page> operator1Expected = rowPagesBuilder(BIGINT, BIGINT)
                .addBlocksPage(createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 8), createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 8)) // the last position was aggregated
                .addBlocksPage(createRLEBlock(1, 10), createRLEBlock(1, 10)) // we are expecting second page with raw values
                .build();
        OperatorAssertion.assertOperatorEquals(operatorFactory, ImmutableList.of(BIGINT), createDriverContext(), operator1Input, operator1Expected);

        // the first operator flush disables partial aggregation
        assertTrue(partialAggregationController.isPartialAggregationDisabled());
        // second operator using the same factory, reuses PartialAggregationControl, so it will only produce raw pages (partial aggregation is disabled at this point)
        List<Page> operator2Input = rowPagesBuilder(false, hashChannels, BIGINT)
                .addBlocksPage(createRLEBlock(1, 10))
                .addBlocksPage(createRLEBlock(2, 10))
                .build();
        List<Page> operator2Expected = rowPagesBuilder(BIGINT, BIGINT)
                .addBlocksPage(createRLEBlock(1, 10), createRLEBlock(1, 10))
                .addBlocksPage(createRLEBlock(2, 10), createRLEBlock(2, 10))
                .build();

        OperatorAssertion.assertOperatorEquals(operatorFactory, ImmutableList.of(BIGINT), createDriverContext(), operator2Input, operator2Expected);
    }

    @Test
    public void testAdaptivePartialAggregationTriggeredOnlyOnFlush()
    {
        List<Integer> hashChannels = Ints.asList(0);

        PartialAggregationController partialAggregationController = new PartialAggregationController(5, 0.8);
        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                PARTIAL,
                ImmutableList.of(LONG_MIN.bind(ImmutableList.of(0), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                10,
                Optional.of(new DataSize(16, MEGABYTE)), // this setting makes operator to flush only after all pages
                joinCompiler,
                false,
                // use 5 rows threshold to trigger adaptive partial aggregation after each page flush
                Optional.of(partialAggregationController));

        List<Page> operator1Input = rowPagesBuilder(false, hashChannels, BIGINT)
                .addSequencePage(10, 0) // first page are unique values, so it would trigger adaptation, but it won't because flush is not called
                .addBlocksPage(createRLEBlock(1, 2)) // second page will be hashed to existing value 1
                .build();
        // the total unique ows ratio for the first operator will be 10/12 so > 0.8 (adaptive partial aggregation uniqueRowsRatioThreshold)
        List<Page> operator1Expected = rowPagesBuilder(BIGINT, BIGINT)
                .addSequencePage(10, 0, 0) // we are expecting second page to be squashed with the first
                .build();
        OperatorAssertion.assertOperatorEquals(operatorFactory, ImmutableList.of(BIGINT), createDriverContext(), operator1Input, operator1Expected);

        // the first operator flush disables partial aggregation
        assertTrue(partialAggregationController.isPartialAggregationDisabled());

        // second operator using the same factory, reuses PartialAggregationControl, so it will only produce raw pages (partial aggregation is disabled at this point)
        List<Page> operator2Input = rowPagesBuilder(false, hashChannels, BIGINT)
                .addBlocksPage(createRLEBlock(1, 10))
                .addBlocksPage(createRLEBlock(2, 10))
                .build();
        List<Page> operator2Expected = rowPagesBuilder(BIGINT, BIGINT)
                .addBlocksPage(createRLEBlock(1, 10), createRLEBlock(1, 10))
                .addBlocksPage(createRLEBlock(2, 10), createRLEBlock(2, 10))
                .build();

        OperatorAssertion.assertOperatorEquals(operatorFactory, ImmutableList.of(BIGINT), createDriverContext(), operator2Input, operator2Expected);
    }

    private DriverContext createDriverContext()
    {
        return createDriverContext(Integer.MAX_VALUE);
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private int getHashCapacity(Operator operator)
    {
        assertTrue(operator instanceof HashAggregationOperator);
        AggregationBuilder aggregationBuilder = ((HashAggregationOperator) operator).getAggregationBuilder();
        if (aggregationBuilder == null) {
            return 0;
        }
        assertTrue(aggregationBuilder instanceof InMemoryHashAggregationBuilder);
        return ((InMemoryHashAggregationBuilder) aggregationBuilder).getCapacity();
    }

    @RestorableConfig(unsupported = true)
    private static class FailingSpillerFactory
            implements SpillerFactory
    {
        @Override
        public Spiller create(List<Type> types, SpillContext spillContext, AggregatedMemoryContext memoryContext, boolean isSnapshotEnabled, String queryId, boolean isSpillToHdfs)
        {
            return new Spiller()
            {
                @RestorableConfig(unsupported = true)
                private final RestorableConfig restorableConfig = null;

                @Override
                public ListenableFuture<?> spill(Iterator<Page> pageIterator)
                {
                    return immediateFailedFuture(new IOException("Failed to spill"));
                }

                @Override
                public List<Iterator<Page>> getSpills()
                {
                    return ImmutableList.of();
                }

                @Override
                public void close()
                {
                }
            };
        }
    }
}
