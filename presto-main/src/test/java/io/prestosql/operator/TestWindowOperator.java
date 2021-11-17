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
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.ExceededMemoryLimitException;
import io.prestosql.Session;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.operator.WindowOperator.WindowOperatorFactory;
import io.prestosql.operator.window.FirstValueFunction;
import io.prestosql.operator.window.FrameInfo;
import io.prestosql.operator.window.LagFunction;
import io.prestosql.operator.window.LastValueFunction;
import io.prestosql.operator.window.LeadFunction;
import io.prestosql.operator.window.NthValueFunction;
import io.prestosql.operator.window.ReflectionWindowFunctionSupplier;
import io.prestosql.operator.window.RowNumberFunction;
import io.prestosql.snapshot.SnapshotConfig;
import io.prestosql.snapshot.SnapshotUtils;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.FileSingleStreamSpillerFactory;
import io.prestosql.spiller.GenericSpillerFactory;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.spiller.SpillerStats;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingTaskContext;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrderWithRestoreToNewOperator;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEqualsWithRestoreToNewOperator;
import static io.prestosql.operator.OperatorAssertion.toMaterializedResult;
import static io.prestosql.operator.OperatorAssertion.toPages;
import static io.prestosql.operator.OperatorAssertion.toPagesWithRestoreToNewOperator;
import static io.prestosql.operator.WindowFunctionDefinition.window;
import static io.prestosql.spi.sql.expression.Types.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.prestosql.spi.sql.expression.Types.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.prestosql.spi.sql.expression.Types.WindowFrameType.RANGE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingSnapshotUtils.NOOP_SNAPSHOT_UTILS;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestWindowOperator
{
    private static final FrameInfo UNBOUNDED_FRAME = new FrameInfo(RANGE, UNBOUNDED_PRECEDING, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty());

    public static final List<WindowFunctionDefinition> ROW_NUMBER = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("row_number", BIGINT, ImmutableList.of(), RowNumberFunction.class), BIGINT, UNBOUNDED_FRAME));

    private static final List<WindowFunctionDefinition> FIRST_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("first_value", VARCHAR, ImmutableList.<Type>of(VARCHAR), FirstValueFunction.class), VARCHAR, UNBOUNDED_FRAME, 1));

    private static final List<WindowFunctionDefinition> LAST_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("last_value", VARCHAR, ImmutableList.<Type>of(VARCHAR), LastValueFunction.class), VARCHAR, UNBOUNDED_FRAME, 1));

    private static final List<WindowFunctionDefinition> NTH_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("nth_value", VARCHAR, ImmutableList.of(VARCHAR, BIGINT), NthValueFunction.class), VARCHAR, UNBOUNDED_FRAME, 1, 3));

    private static final List<WindowFunctionDefinition> LAG = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("lag", VARCHAR, ImmutableList.of(VARCHAR, BIGINT, VARCHAR), LagFunction.class), VARCHAR, UNBOUNDED_FRAME, 1, 3, 4));

    private static final List<WindowFunctionDefinition> LEAD = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("lead", VARCHAR, ImmutableList.of(VARCHAR, BIGINT, VARCHAR), LeadFunction.class), VARCHAR, UNBOUNDED_FRAME, 1, 3, 4));

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DummySpillerFactory spillerFactory;
    private SnapshotUtils snapshotUtils = NOOP_SNAPSHOT_UTILS;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        spillerFactory = new DummySpillerFactory();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        spillerFactory = null;
    }

    @DataProvider
    public static Object[][] spillEnabled()
    {
        return new Object[][] {
                {false, false, 0},
                {true, false, 8},
                {true, true, 8},
                {true, false, 0},
                {true, true, 0}};
    }

    @DataProvider
    public static Object[][] spillEnabledSnapshot()
    {
        return new Object[][] {
                {false, false, 0}};
    }

    @Test(dataProvider = "spillEnabled")
    public void testMultipleOutputPages(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        // make operator produce multiple pages during finish phase
        int numberOfRows = 80_000;
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .addSequencePage(numberOfRows, 0, 0)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.DESC_NULLS_FIRST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row((double) numberOfRows - i - 1, (long) numberOfRows - i - 1, (long) i + 1);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
        assertGreaterThan(pages.size(), 1, "Expected more than one output page");

        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());

        assertTrue(spillEnabled == (spillerFactory.getSpillsCount() > 0), format("Spill state mismatch. Expected spill: %s, spill count: %s", spillEnabled, spillerFactory.getSpillsCount()));
    }

    @Test(dataProvider = "spillEnabledSnapshot")
    public void testMultipleOutputPagesSnapshot(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        // make operator produce multiple pages during finish phase
        int numberOfRows = 80_000;
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .addSequencePage(numberOfRows, 0, 0)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.DESC_NULLS_FIRST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row((double) numberOfRows - i - 1, (long) numberOfRows - i - 1, (long) i + 1);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPagesWithRestoreToNewOperator(operatorFactory, driverContext, () -> createDriverContext(memoryLimit), input, revokeMemoryWhenAddingPages);
        assertGreaterThan(pages.size(), 1, "Expected more than one output page");

        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());

        assertTrue(false == (spillerFactory.getSpillsCount() > 0), format("Spill state mismatch. Expected spill: %s, spill count: %s", false, spillerFactory.getSpillsCount()));
    }

    @Test(dataProvider = "spillEnabled")
    public void testRowNumber(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(2L, 0.3)
                .row(4L, 0.2)
                .row(6L, 0.1)
                .pageBreak()
                .row(-1L, -0.1)
                .row(5L, 0.4)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(-0.1, -1L, 1L)
                .row(0.3, 2L, 2L)
                .row(0.2, 4L, 3L)
                .row(0.4, 5L, 4L)
                .row(0.1, 6L, 5L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabledSnapshot")
    public void testRowNumberSnapshot(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(2L, 0.3)
                .row(4L, 0.2)
                .row(6L, 0.1)
                .pageBreak()
                .row(-1L, -0.1)
                .row(5L, 0.4)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(-0.1, -1L, 1L)
                .row(0.3, 2L, 2L)
                .row(0.2, 4L, 3L)
                .row(0.4, 5L, 4L)
                .row(0.1, 6L, 5L)
                .build();

        assertOperatorEqualsWithRestoreToNewOperator(operatorFactory, driverContext, () -> createDriverContext(memoryLimit), input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testRowNumberPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT, DOUBLE, BOOLEAN)
                .row("b", -1L, -0.1, true)
                .row("a", 2L, 0.3, false)
                .row("a", 4L, 0.2, true)
                .pageBreak()
                .row("b", 5L, 0.4, false)
                .row("a", 6L, 0.1, true)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, DOUBLE, BOOLEAN, BIGINT)
                .row("a", 2L, 0.3, false, 1L)
                .row("a", 4L, 0.2, true, 2L)
                .row("a", 6L, 0.1, true, 3L)
                .row("b", -1L, -0.1, true, 1L)
                .row("b", 5L, 0.4, false, 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabledSnapshot")
    public void testRowNumberPartitionSnapshot(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT, DOUBLE, BOOLEAN)
                .row("b", -1L, -0.1, true)
                .row("a", 2L, 0.3, false)
                .row("a", 4L, 0.2, true)
                .pageBreak()
                .row("b", 5L, 0.4, false)
                .row("a", 6L, 0.1, true)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, DOUBLE, BOOLEAN, BIGINT)
                .row("a", 2L, 0.3, false, 1L)
                .row("a", 4L, 0.2, true, 2L)
                .row("a", 6L, 0.1, true, 3L)
                .row("b", -1L, -0.1, true, 1L)
                .row("b", 5L, 0.4, false, 2L)
                .build();

        assertOperatorEqualsWithRestoreToNewOperator(operatorFactory, driverContext, () -> createDriverContext(memoryLimit), input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testRowNumberArbitrary()
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .row(3L)
                .row(5L)
                .row(7L)
                .pageBreak()
                .row(2L)
                .row(4L)
                .row(6L)
                .row(8L)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT),
                Ints.asList(0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(),
                ImmutableList.copyOf(new SortOrder[] {}),
                false);

        DriverContext driverContext = createDriverContext();
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(3L, 2L)
                .row(5L, 3L)
                .row(7L, 4L)
                .row(2L, 5L)
                .row(4L, 6L)
                .row(6L, 7L)
                .row(8L, 8L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testRowNumberArbitraryWithSpill()
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .row(3L)
                .row(5L)
                .row(7L)
                .pageBreak()
                .row(2L)
                .row(4L)
                .row(6L)
                .row(8L)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT),
                Ints.asList(0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(),
                ImmutableList.copyOf(new SortOrder[] {}),
                true);

        DriverContext driverContext = createDriverContext();
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(3L, 3L)
                .row(4L, 4L)
                .row(5L, 5L)
                .row(6L, 6L)
                .row(7L, 7L)
                .row(8L, 8L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded per-node user memory limit of 10B.*")
    public void testMemoryLimit()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(10, Unit.BYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                false);

        toPages(operatorFactory, driverContext, input);
    }

    @Test(dataProvider = "spillEnabled")
    public void testFirstValuePartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, true, "")
                .row("a", "A2", 1L, false, "")
                .row("a", "B1", 2L, true, "")
                .pageBreak()
                .row("b", "C1", 2L, false, "")
                .row("a", "C2", 3L, true, "")
                .row("c", "A3", 1L, true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                FIRST_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "A2")
                .row("a", "B1", 2L, true, "A2")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "A1")
                .row("b", "C1", 2L, false, "A1")
                .row("c", "A3", 1L, true, "A3")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabledSnapshot")
    public void testFirstValuePartitionSnapshot(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, true, "")
                .row("a", "A2", 1L, false, "")
                .row("a", "B1", 2L, true, "")
                .pageBreak()
                .row("b", "C1", 2L, false, "")
                .row("a", "C2", 3L, true, "")
                .row("c", "A3", 1L, true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                FIRST_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "A2")
                .row("a", "B1", 2L, true, "A2")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "A1")
                .row("b", "C1", 2L, false, "A1")
                .row("c", "A3", 1L, true, "A3")
                .build();

        assertOperatorEqualsWithRestoreToNewOperator(operatorFactory, driverContext, () -> createDriverContext(memoryLimit), input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testLastValuePartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, true, "")
                .row("a", "A2", 1L, false, "")
                .row("a", "B1", 2L, true, "")
                .pageBreak()
                .row("b", "C1", 2L, false, "")
                .row("a", "C2", 3L, true, "")
                .row("c", "A3", 1L, true, "")
                .build();

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                LAST_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "C2")
                .row("a", "C2", 3L, true, "C2")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, "C1")
                .row("c", "A3", 1L, true, "A3")
                .build();
        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testNthValuePartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, 2L, true, "")
                .row("a", "A2", 1L, 3L, false, "")
                .row("a", "B1", 2L, 2L, true, "")
                .pageBreak()
                .row("b", "C1", 2L, 3L, false, "")
                .row("a", "C2", 3L, 1L, true, "")
                .row("c", "A3", 1L, null, true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 4),
                NTH_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "B1")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, null)
                .row("c", "A3", 1L, true, null)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabledSnapshot")
    public void testNthValuePartitionSnapshot(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, 2L, true, "")
                .row("a", "A2", 1L, 3L, false, "")
                .row("a", "B1", 2L, 2L, true, "")
                .pageBreak()
                .row("b", "C1", 2L, 3L, false, "")
                .row("a", "C2", 3L, 1L, true, "")
                .row("c", "A3", 1L, null, true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 4),
                NTH_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "B1")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, null)
                .row("c", "A3", 1L, true, null)
                .build();

        assertOperatorEqualsWithRestoreToNewOperator(operatorFactory, driverContext, () -> createDriverContext(memoryLimit), input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testLagPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, 1L, "D", true, "")
                .row("a", "A2", 1L, 2L, "D", false, "")
                .row("a", "B1", 2L, 2L, "D", true, "")
                .pageBreak()
                .row("b", "C1", 2L, 1L, "D", false, "")
                .row("a", "C2", 3L, 2L, "D", true, "")
                .row("c", "A3", 1L, 1L, "D", true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 5),
                LAG,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "D")
                .row("a", "B1", 2L, true, "D")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "D")
                .row("b", "C1", 2L, false, "A1")
                .row("c", "A3", 1L, true, "D")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testLeadPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, 1L, "D", true, "")
                .row("a", "A2", 1L, 2L, "D", false, "")
                .row("a", "B1", 2L, 2L, "D", true, "")
                .pageBreak()
                .row("b", "C1", 2L, 1L, "D", false, "")
                .row("a", "C2", 3L, 2L, "D", true, "")
                .row("c", "A3", 1L, 1L, "D", true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 5),
                LEAD,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "D")
                .row("a", "C2", 3L, true, "D")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, "D")
                .row("c", "A3", 1L, true, "D")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testPartiallyPreGroupedPartitionWithEmptyInput(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .pageBreak()
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0, 1),
                Ints.asList(1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testPartiallyPreGroupedPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .row(2L, "a", 101L, "B")
                .pageBreak()
                .row(3L, "b", 102L, "E")
                .row(1L, "b", 103L, "D")
                .pageBreak()
                .row(3L, "b", 104L, "C")
                .row(1L, "c", 105L, "F")
                .pageBreak()
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0, 1),
                Ints.asList(1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "B", 1L)
                .row(3L, "b", 104L, "C", 1L)
                .row(3L, "b", 102L, "E", 2L)
                .row(1L, "b", 103L, "D", 1L)
                .row(1L, "c", 105L, "F", 1L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabledSnapshot")
    public void testPartiallyPreGroupedPartitionSnapshot(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .row(2L, "a", 101L, "B")
                .pageBreak()
                .row(3L, "b", 102L, "E")
                .row(1L, "b", 103L, "D")
                .pageBreak()
                .row(3L, "b", 104L, "C")
                .row(1L, "c", 105L, "F")
                .pageBreak()
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0, 1),
                Ints.asList(1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "B", 1L)
                .row(3L, "b", 104L, "C", 1L)
                .row(3L, "b", 102L, "E", 2L)
                .row(1L, "b", 103L, "D", 1L)
                .row(1L, "c", 105L, "F", 1L)
                .build();

        assertOperatorEqualsIgnoreOrderWithRestoreToNewOperator(operatorFactory, driverContext, () -> createDriverContext(memoryLimit), input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testFullyPreGroupedPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "a", 101L, "B")
                .pageBreak()
                .row(2L, "b", 102L, "D")
                .row(2L, "b", 103L, "C")
                .row(1L, "b", 104L, "E")
                .pageBreak()
                .row(1L, "b", 105L, "F")
                .row(3L, "c", 106L, "G")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "B", 1L)
                .row(2L, "b", 103L, "C", 1L)
                .row(2L, "b", 102L, "D", 2L)
                .row(1L, "b", 104L, "E", 1L)
                .row(1L, "b", 105L, "F", 2L)
                .row(3L, "c", 106L, "G", 1L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabledSnapshot")
    public void testFullyPreGroupedPartitionSnapshot(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "a", 101L, "B")
                .pageBreak()
                .row(2L, "b", 102L, "D")
                .row(2L, "b", 103L, "C")
                .row(1L, "b", 104L, "E")
                .pageBreak()
                .row(1L, "b", 105L, "F")
                .row(3L, "c", 106L, "G")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "B", 1L)
                .row(2L, "b", 103L, "C", 1L)
                .row(2L, "b", 102L, "D", 2L)
                .row(1L, "b", 104L, "E", 1L)
                .row(1L, "b", 105L, "F", 2L)
                .row(3L, "c", 106L, "G", 1L)
                .build();

        assertOperatorEqualsIgnoreOrderWithRestoreToNewOperator(operatorFactory, driverContext, () -> createDriverContext(memoryLimit), input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testFullyPreGroupedAndPartiallySortedPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "b", 102L, "A")
                .row(2L, "b", 101L, "A")
                .row(2L, "b", 100L, "B")
                .row(1L, "b", 101L, "A")
                .pageBreak()
                .row(1L, "b", 100L, "A")
                .row(3L, "c", 100L, "A")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3, 2),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST, SortOrder.ASC_NULLS_LAST),
                1,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 100L, "A", 1L)
                .row(2L, "b", 101L, "A", 1L)
                .row(2L, "b", 102L, "A", 2L)
                .row(2L, "b", 100L, "B", 3L)
                .row(1L, "b", 100L, "A", 1L)
                .row(1L, "b", 101L, "A", 2L)
                .row(3L, "c", 100L, "A", 1L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testFullyPreGroupedAndFullySortedPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "a", 101L, "A")
                .pageBreak()
                .row(2L, "b", 102L, "A")
                .row(2L, "b", 103L, "A")
                .row(2L, "b", 104L, "B")
                .row(1L, "b", 105L, "A")
                .pageBreak()
                .row(1L, "b", 106L, "A")
                .row(3L, "c", 107L, "A")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                1,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "A", 1L)
                .row(2L, "b", 102L, "A", 1L)
                .row(2L, "b", 103L, "A", 2L)
                .row(2L, "b", 104L, "B", 3L)
                .row(1L, "b", 105L, "A", 1L)
                .row(1L, "b", 106L, "A", 2L)
                .row(3L, "c", 107L, "A", 1L)
                .build();

        // Since fully grouped and sorted already, should respect original input order
        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabledSnapshot")
    public void testFullyPreGroupedAndFullySortedPartitionSnapshot(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "a", 101L, "A")
                .pageBreak()
                .row(2L, "b", 102L, "A")
                .row(2L, "b", 103L, "A")
                .row(2L, "b", 104L, "B")
                .row(1L, "b", 105L, "A")
                .pageBreak()
                .row(1L, "b", 106L, "A")
                .row(3L, "c", 107L, "A")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                1,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "A", 1L)
                .row(2L, "b", 102L, "A", 1L)
                .row(2L, "b", 103L, "A", 2L)
                .row(2L, "b", 104L, "B", 3L)
                .row(1L, "b", 105L, "A", 1L)
                .row(1L, "b", 106L, "A", 2L)
                .row(3L, "c", 107L, "A", 1L)
                .build();

        // Since fully grouped and sorted already, should respect original input order
        assertOperatorEqualsWithRestoreToNewOperator(operatorFactory, driverContext, () -> createDriverContext(memoryLimit), input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testFindEndPosition()
    {
        assertFindEndPosition("0", 1);
        assertFindEndPosition("11", 2);
        assertFindEndPosition("1111111111", 10);

        assertFindEndPosition("01", 1);
        assertFindEndPosition("011", 1);
        assertFindEndPosition("0111", 1);
        assertFindEndPosition("0111111111", 1);

        assertFindEndPosition("012", 1);
        assertFindEndPosition("01234", 1);
        assertFindEndPosition("0123456789", 1);

        assertFindEndPosition("001", 2);
        assertFindEndPosition("0001", 3);
        assertFindEndPosition("0000000001", 9);

        assertFindEndPosition("000111", 3);
        assertFindEndPosition("0001111", 3);
        assertFindEndPosition("0000111", 4);
        assertFindEndPosition("000000000000001111111111", 14);
    }

    private static void assertFindEndPosition(String values, int expected)
    {
        char[] array = values.toCharArray();
        assertEquals(WindowOperator.findEndPosition(0, array.length, (first, second) -> array[first] == array[second]), expected);
    }

    private WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            boolean spillEnabled)
    {
        return createFactoryUnbounded(
                sourceTypes,
                outputChannels,
                functions,
                partitionChannels,
                ImmutableList.of(),
                sortChannels,
                sortOrder,
                0,
                spillEnabled);
    }

    private WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> preGroupedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int preSortedChannelPrefix,
            boolean spillEnabled)
    {
        return new WindowOperatorFactory(
                0,
                new PlanNodeId("test"),
                sourceTypes,
                outputChannels,
                functions,
                partitionChannels,
                preGroupedChannels,
                sortChannels,
                sortOrder,
                preSortedChannelPrefix,
                10,
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                spillerFactory,
                new OrderingCompiler());
    }

    public static WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> preGroupedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int preSortedChannelPrefix,
            SpillerFactory spillerFactory,
            boolean spillEnabled)
    {
        return new WindowOperatorFactory(
                0,
                new PlanNodeId("test"),
                sourceTypes,
                outputChannels,
                functions,
                partitionChannels,
                preGroupedChannels,
                sortChannels,
                sortOrder,
                preSortedChannelPrefix,
                10,
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                spillerFactory,
                new OrderingCompiler());
    }

    private DriverContext createDriverContext()
    {
        return createDriverContext(Long.MAX_VALUE);
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return createDriverContext(memoryLimit, TEST_SESSION);
    }

    private DriverContext createDriverContext(long memoryLimit, Session session)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, session)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .setSnapshotUtils(snapshotUtils)
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private static GenericSpillerFactory createGenericSpillerFactory(Path spillPath)
    {
        FileSingleStreamSpillerFactory streamSpillerFactory = new FileSingleStreamSpillerFactory(
                listeningDecorator(newCachedThreadPool()),
                createTestMetadataManager().getFunctionAndTypeManager().getBlockEncodingSerde(),
                new SpillerStats(),
                ImmutableList.of(spillPath),
                1.0, false, false, false);
        return new GenericSpillerFactory(streamSpillerFactory);
    }

    @Test
    public void testCaptureRestoreWithSpill()
            throws Exception
    {
        // Initialization
        Path spillPath = Files.createTempDir().toPath();
        GenericSpillerFactory spillerFactory = createGenericSpillerFactory(spillPath);
        FileSystemClientManager fileSystemClientManager = mock(FileSystemClientManager.class);
        when(fileSystemClientManager.getFileSystemClient(any(Path.class))).thenReturn(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get("/tmp/hetu/snapshot/")));
        SnapshotConfig snapshotConfig = new SnapshotConfig();
        snapshotUtils = new SnapshotUtils(fileSystemClientManager, snapshotConfig, new InMemoryNodeManager());
        snapshotUtils.initialize();
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        List<Page> input1 = rowPagesBuilder(VARCHAR, BIGINT, DOUBLE, BOOLEAN)
                .row("b", -1L, -0.1, true)
                .row("a", 2L, 0.3, false)
                .row("a", 4L, 0.2, true)
                .pageBreak()
                .row("b", 5L, 0.4, false)
                .row("a", 6L, 0.1, true)
                .build();

        List<Page> input2 = rowPagesBuilder(VARCHAR, BIGINT, DOUBLE, BOOLEAN)
                .row("c", -1L, -0.1, true)
                .row("d", 2L, 0.3, false)
                .row("c", 4L, 0.2, true)
                .pageBreak()
                .row("d", 5L, 0.4, false)
                .build();

        WindowOperatorFactory operatorFactory = new WindowOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0),
                ImmutableList.of(),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                0,
                10,
                new PagesIndex.TestingFactory(false),
                true,
                spillerFactory,
                new OrderingCompiler());

        DriverContext driverContext = createDriverContext(0, TEST_SNAPSHOT_SESSION);
        WindowOperator windowOperator = (WindowOperator) operatorFactory.createOperator(driverContext);

        // Step1: add the first 2 pages
        for (Page page : input1) {
            windowOperator.addInput(page);
            windowOperator.getOutput();
        }
        // Step2: spilling happened here
        getFutureValue(windowOperator.startMemoryRevoke());
        windowOperator.finishMemoryRevoke();

        // Step3: add a marker page to make 'capture1' happened
        MarkerPage marker = MarkerPage.snapshotPage(1);
        windowOperator.addInput(marker);
        windowOperator.getOutput();

        // Step4: add another 2 pages
        for (Page page : input2) {
            windowOperator.addInput(page);
            windowOperator.getOutput();
        }

        // Step5: assume the task is rescheduled due to failure and everything is re-constructed

        driverContext = createDriverContext(8, TEST_SNAPSHOT_SESSION);
        operatorFactory = new WindowOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0),
                ImmutableList.of(),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                0,
                10,
                new PagesIndex.TestingFactory(false),
                true,
                spillerFactory,
                new OrderingCompiler());
        windowOperator = (WindowOperator) operatorFactory.createOperator(driverContext);

        // Step6: restore to 'capture1', the spiller should contains the reference of the first 2 pages for now.
        MarkerPage resumeMarker = MarkerPage.resumePage(1);
        windowOperator.addInput(resumeMarker);
        windowOperator.getOutput();

        // Step7: continue to add another 2 pages
        for (Page page : input2) {
            windowOperator.addInput(page);
            windowOperator.getOutput();
        }
        windowOperator.finish();

        // Compare the results
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, DOUBLE, BOOLEAN, BIGINT)
                .row("a", 2L, 0.3, false, 1L)
                .row("a", 4L, 0.2, true, 2L)
                .row("a", 6L, 0.1, true, 3L)
                .row("b", -1L, -0.1, true, 1L)
                .row("b", 5L, 0.4, false, 2L)
                .row("c", -1L, -0.1, true, 1L)
                .row("c", 4L, 0.2, true, 2L)
                .row("d", 2L, 0.3, false, 1L)
                .row("d", 5L, 0.4, false, 2L)
                .build();

        Page p = windowOperator.getOutput();
        while (p == null) {
            p = windowOperator.getOutput();
        }

        outputPages.add(p);
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), outputPages.build());

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testCaptureRestoreWithoutSpill()
            throws Exception
    {
        FileSystemClientManager fileSystemClientManager = mock(FileSystemClientManager.class);
        when(fileSystemClientManager.getFileSystemClient(any(Path.class))).thenReturn(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get("/tmp/hetu/snapshot/")));
        SnapshotConfig snapshotConfig = new SnapshotConfig();
        snapshotUtils = new SnapshotUtils(fileSystemClientManager, snapshotConfig, new InMemoryNodeManager());
        snapshotUtils.initialize();
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        List<Page> input1 = rowPagesBuilder(VARCHAR, BIGINT, DOUBLE, BOOLEAN)
                .row("b", -1L, -0.1, true)
                .row("a", 2L, 0.3, false)
                .row("a", 4L, 0.2, true)
                .pageBreak()
                .row("b", 5L, 0.4, false)
                .row("a", 6L, 0.1, true)
                .build();

        List<Page> input2 = rowPagesBuilder(VARCHAR, BIGINT, DOUBLE, BOOLEAN)
                .row("c", -1L, -0.1, true)
                .row("d", 2L, 0.3, false)
                .row("c", 4L, 0.2, true)
                .pageBreak()
                .row("d", 5L, 0.4, false)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                false);

        DriverContext driverContext = createDriverContext(0, TEST_SNAPSHOT_SESSION);
        WindowOperator windowOperator = (WindowOperator) operatorFactory.createOperator(driverContext);

        // Step1: add the first 2 pages
        for (Page page : input1) {
            windowOperator.addInput(page);
            windowOperator.getOutput();
        }

        // Step2: add a marker page to make 'capture1' happened
        MarkerPage marker = MarkerPage.snapshotPage(1);
        windowOperator.addInput(marker);
        windowOperator.getOutput();

        // Step3: add another 2 pages
        for (Page page : input2) {
            windowOperator.addInput(page);
            windowOperator.getOutput();
        }

        // Step4: assume the task is rescheduled due to failure and everything is re-constructed
        driverContext = createDriverContext(8, TEST_SNAPSHOT_SESSION);
        operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                false);
        windowOperator = (WindowOperator) operatorFactory.createOperator(driverContext);

        // Step5: restore to 'capture1'
        MarkerPage resumeMarker = MarkerPage.resumePage(1);
        windowOperator.addInput(resumeMarker);
        windowOperator.getOutput();

        // Step6: continue to add another 2 pages
        for (Page page : input2) {
            windowOperator.addInput(page);
            windowOperator.getOutput();
        }
        windowOperator.finish();

        // Compare the results
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, DOUBLE, BOOLEAN, BIGINT)
                .row("a", 2L, 0.3, false, 1L)
                .row("a", 4L, 0.2, true, 2L)
                .row("a", 6L, 0.1, true, 3L)
                .row("b", -1L, -0.1, true, 1L)
                .row("b", 5L, 0.4, false, 2L)
                .row("c", -1L, -0.1, true, 1L)
                .row("c", 4L, 0.2, true, 2L)
                .row("d", 2L, 0.3, false, 1L)
                .row("d", 5L, 0.4, false, 2L)
                .build();

        Page p = windowOperator.getOutput();
        while (p == null) {
            p = windowOperator.getOutput();
        }

        outputPages.add(p);
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), outputPages.build());

        Assert.assertEquals(actual, expected);
    }
}
