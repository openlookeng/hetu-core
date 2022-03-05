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
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.hetu.core.filesystem.HdfsConfig;
import io.hetu.core.filesystem.HetuHdfsFileSystemClient;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.ExceededMemoryLimitException;
import io.prestosql.Session;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.operator.OrderByOperator.OrderByOperatorFactory;
import io.prestosql.snapshot.SnapshotConfig;
import io.prestosql.snapshot.SnapshotUtils;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spiller.FileSingleStreamSpillerFactory;
import io.prestosql.spiller.GenericSpillerFactory;
import io.prestosql.spiller.SpillerStats;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingTaskContext;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static io.prestosql.operator.OperatorAssertion.assertOperatorEqualsWithSimpleSelfStateComparison;
import static io.prestosql.operator.OperatorAssertion.toMaterializedResult;
import static io.prestosql.operator.OperatorAssertion.toPages;
import static io.prestosql.spi.block.SortOrder.ASC_NULLS_LAST;
import static io.prestosql.spi.block.SortOrder.DESC_NULLS_LAST;
import static io.prestosql.spi.type.BigintType.BIGINT;
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
public class TestOrderByOperator
{
    private static final long defaultMemoryLimit = 1L << 28;

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DummySpillerFactory spillerFactory;
    private SnapshotUtils snapshotUtils = NOOP_SNAPSHOT_UTILS;
    private FileSystemClientManager fileSystemClientManager = mock(FileSystemClientManager.class);

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

    @BeforeMethod
    public void setUp() throws IOException
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        spillerFactory = new DummySpillerFactory();
        when(fileSystemClientManager.getFileSystemClient(any(Path.class))).thenReturn(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get("/tmp/hetu/snapshot/")));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        spillerFactory = null;
    }

    @Test(dataProvider = "spillEnabled")
    public void testMultipleOutputPages(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        // make operator produce multiple pages during finish phase
        int numberOfRows = 80_000;
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .addSequencePage(numberOfRows, 0, 0)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(spillerFactory),
                new OrderingCompiler(), false);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), DOUBLE);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row((double) numberOfRows - i - 1);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
        assertGreaterThan(pages.size(), 1, "Expected more than one output page");

        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());

        assertTrue(spillEnabled == (spillerFactory.getSpillsCount() > 0), format("Spill state mismatch. Expected spill: %s, spill count: %s", spillEnabled, spillerFactory.getSpillsCount()));
    }

    @Test(dataProvider = "spillEnabled")
    public void testSingleFieldKey(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(spillerFactory),
                new OrderingCompiler(), false);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE)
                .row(-0.1)
                .row(0.1)
                .row(0.2)
                .row(0.4)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testMultiFieldKey(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("b", 2L)
                .pageBreak()
                .row("b", 3L)
                .row("a", 4L)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(0, 1),
                10,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(spillerFactory),
                new OrderingCompiler(), false);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("a", 4L)
                .row("a", 1L)
                .row("b", 3L)
                .row("b", 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testMultiFieldKeySnapshot()
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("b", 2L)
                .pageBreak()
                .row("b", 3L)
                .row("a", 4L)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(0, 1),
                10,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                false,
                Optional.of(spillerFactory),
                new OrderingCompiler(),
                false);

        DriverContext driverContext = createDriverContext(0, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("a", 4L)
                .row("a", 1L)
                .row("b", 3L)
                .row("b", 2L)
                .build();

        assertOperatorEqualsWithSimpleSelfStateComparison(operatorFactory, driverContext, input, expected, false, createExpectedMapping());
    }

    private Map<String, Object> createExpectedMapping()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("operatorContext", 0);
        expectedMapping.put("revocableMemoryContext", 0L);
        expectedMapping.put("localUserMemoryContext", 8844L);
        expectedMapping.put("secondaryMemoryContext", 8844L);
        expectedMapping.put("secondarySpillRunning", false);
        expectedMapping.put("primarySpillRunning", false);
        return expectedMapping;
    }

    @Test(dataProvider = "spillEnabled")
    public void testReverseOrder(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(0),
                10,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(spillerFactory),
                new OrderingCompiler(), false);

        DriverContext driverContext = createDriverContext(memoryLimit, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(4L)
                .row(2L)
                .row(1L)
                .row(-1L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testReverseOrderWithSnapshot()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(0),
                10,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                true,
                Optional.of(spillerFactory),
                new OrderingCompiler(),
                true);

        DriverContext driverContext = createDriverContext(8, TEST_SESSION);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(4L)
                .row(2L)
                .row(1L)
                .row(-1L)
                .build();

        assertOperatorEqualsWithSimpleSelfStateComparison(operatorFactory, driverContext, input, expected, true, createExpectedMappingRevoke());
    }

    private Map<String, Object> createExpectedMappingRevoke()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("operatorContext", 0);
        expectedMapping.put("revocableMemoryContext", 1288L);
        expectedMapping.put("localUserMemoryContext", 0L);
        expectedMapping.put("secondaryMemoryContext", 0L);
        expectedMapping.put("secondarySpillRunning", false);
        expectedMapping.put("primarySpillRunning", false);
        return expectedMapping;
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

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                false,
                Optional.of(spillerFactory),
                new OrderingCompiler(), false);

        toPages(operatorFactory, driverContext, input);
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

    private static GenericSpillerFactory createGenericSpillerFactory(Path spillPath, FileSystemClientManager fileSystemClientManager, boolean spillToHdfs, String spillProfile)
    {
        FileSingleStreamSpillerFactory streamSpillerFactory = new FileSingleStreamSpillerFactory(
                listeningDecorator(newCachedThreadPool()),
                createTestMetadataManager().getFunctionAndTypeManager().getBlockEncodingSerde(),
                new SpillerStats(),
                ImmutableList.of(spillPath),
                1.0, false, false, false, 1, spillToHdfs, spillProfile, fileSystemClientManager);
        return new GenericSpillerFactory(streamSpillerFactory);
    }

    /**
     * This test is supposed to consume 4 pages and produce the output page with sorted ordering.
     * The spilling and capturing('capture1') happened after the first 2 pages added into the operator.
     * The operator is rescheduled after 4 pages added (but before finish() is called).
     *
     * @throws Exception
     */
    @Test
    public void testCaptureRestoreWithSpill()
            throws Exception
    {
        // Initialization
        Path spillPath = Paths.get("/tmp/hetu/snapshot/");
        GenericSpillerFactory genericSpillerFactory = createGenericSpillerFactory(spillPath, fileSystemClientManager, false, null);
        SnapshotConfig snapshotConfig = new SnapshotConfig();
        snapshotUtils = new SnapshotUtils(fileSystemClientManager, snapshotConfig, new InMemoryNodeManager());
        snapshotUtils.initialize();

        List<Page> input1 = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("b", 2L)
                .pageBreak()
                .row("b", 3L)
                .row("a", 4L)
                .build();

        List<Page> input2 = rowPagesBuilder(VARCHAR, BIGINT)
                .row("c", 4L)
                .row("d", 6L)
                .pageBreak()
                .row("c", 2L)
                .row("d", 3L)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(0, 1),
                10,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                true,
                Optional.of(genericSpillerFactory),
                new OrderingCompiler(),
                false);

        DriverContext driverContext = createDriverContext(defaultMemoryLimit, TEST_SNAPSHOT_SESSION);
        driverContext.getPipelineContext().getTaskContext().getSnapshotManager().setTotalComponents(1);
        OrderByOperator orderByOperator = (OrderByOperator) operatorFactory.createOperator(driverContext);

        // Step1: add the first 2 pages
        for (Page page : input1) {
            orderByOperator.addInput(page);
        }
        // Step2: spilling happened here
        getFutureValue(orderByOperator.startMemoryRevoke());
        orderByOperator.finishMemoryRevoke();

        // Step3: add a marker page to make 'capture1' happened
        MarkerPage marker = MarkerPage.snapshotPage(1);
        orderByOperator.addInput(marker);

        // Step4: add another 2 pages
        for (Page page : input2) {
            orderByOperator.addInput(page);
        }

        // Step5: assume the task is rescheduled due to failure and everything is re-constructed
        driverContext = createDriverContext(defaultMemoryLimit, TEST_SNAPSHOT_SESSION);
        operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(0, 1),
                10,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                true,
                Optional.of(genericSpillerFactory),
                new OrderingCompiler(),
                false);
        orderByOperator = (OrderByOperator) operatorFactory.createOperator(driverContext);

        // Step6: restore to 'capture1', the spiller should contains the reference of the first 2 pages for now.
        MarkerPage resumeMarker = MarkerPage.resumePage(1);
        orderByOperator.addInput(resumeMarker);

        // Step7: continue to add another 2 pages
        for (Page page : input2) {
            orderByOperator.addInput(page);
        }
        orderByOperator.finish();

        // Compare the results
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("a", 4L)
                .row("a", 1L)
                .row("b", 3L)
                .row("b", 2L)
                .row("c", 4L)
                .row("c", 2L)
                .row("d", 6L)
                .row("d", 3L)
                .build();

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        Page p = orderByOperator.getOutput();
        while (p instanceof MarkerPage) {
            p = orderByOperator.getOutput();
        }
        outputPages.add(p);
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), outputPages.build());

        Assert.assertEquals(actual, expected);
    }

    private HetuHdfsFileSystemClient getLocalHdfs()
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("fs.client.type", "hdfs");
        properties.setProperty("hdfs.config.resources", "");
        properties.setProperty("hdfs.authentication.type", "NONE");
        return new HetuHdfsFileSystemClient(new HdfsConfig(properties), Paths.get("/tmp/hetu/snapshot/"));
    }

    /**
     * This test is supposed to consume 4 pages and produce the output page with sorted ordering.
     * The spilling and capturing('capture1') happened after the first 2 pages added into the operator.
     * The operator is rescheduled after 4 pages added (but before finish() is called).
     *
     * @throws Exception
     */
    @Test
    public void testCaptureRestoreWithSpillToHdfsEnabled()
            throws Exception
    {
        // Initialization
        Path spillPath = Paths.get("/tmp/hetu/snapshot/");
        HetuHdfsFileSystemClient fs = getLocalHdfs();
        when(fileSystemClientManager.getFileSystemClient(any(String.class), any(Path.class))).thenReturn(fs);
        GenericSpillerFactory genericSpillerFactory = createGenericSpillerFactory(spillPath, fileSystemClientManager, true, "hdfs");
        SnapshotConfig snapshotConfig = new SnapshotConfig();
        snapshotConfig.setSpillProfile("hdfs");
        snapshotConfig.setSpillToHdfs(true);
        snapshotUtils = new SnapshotUtils(fileSystemClientManager, snapshotConfig, new InMemoryNodeManager());
        snapshotUtils.initialize();

        List<Page> input1 = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("b", 2L)
                .pageBreak()
                .row("b", 3L)
                .row("a", 4L)
                .build();

        List<Page> input2 = rowPagesBuilder(VARCHAR, BIGINT)
                .row("c", 4L)
                .row("d", 6L)
                .pageBreak()
                .row("c", 2L)
                .row("d", 3L)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(0, 1),
                10,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                true,
                Optional.of(genericSpillerFactory),
                new OrderingCompiler(), false);

        DriverContext driverContext = createDriverContext(defaultMemoryLimit, TEST_SNAPSHOT_SESSION);
        driverContext.getPipelineContext().getTaskContext().getSnapshotManager().setTotalComponents(1);
        OrderByOperator orderByOperator = (OrderByOperator) operatorFactory.createOperator(driverContext);

        // Step1: add the first 2 pages
        for (Page page : input1) {
            orderByOperator.addInput(page);
        }
        // Step2: spilling happened here
        getFutureValue(orderByOperator.startMemoryRevoke());
        orderByOperator.finishMemoryRevoke();

        // Step3: add a marker page to make 'capture1' happened
        MarkerPage marker = MarkerPage.snapshotPage(1);
        orderByOperator.addInput(marker);

        // Step4: add another 2 pages
        for (Page page : input2) {
            orderByOperator.addInput(page);
        }

        // Step5: assume the task is rescheduled due to failure and everything is re-constructed
        driverContext = createDriverContext(defaultMemoryLimit, TEST_SNAPSHOT_SESSION);
        operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(0, 1),
                10,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                true,
                Optional.of(genericSpillerFactory),
                new OrderingCompiler(), false);
        orderByOperator = (OrderByOperator) operatorFactory.createOperator(driverContext);

        // Step6: restore to 'capture1', the spiller should contains the reference of the first 2 pages for now.
        MarkerPage resumeMarker = MarkerPage.resumePage(1);
        orderByOperator.addInput(resumeMarker);

        // Step7: continue to add another 2 pages
        for (Page page : input2) {
            orderByOperator.addInput(page);
        }
        orderByOperator.finish();

        // Compare the results
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("a", 4L)
                .row("a", 1L)
                .row("b", 3L)
                .row("b", 2L)
                .row("c", 4L)
                .row("c", 2L)
                .row("d", 6L)
                .row("d", 3L)
                .build();

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        Page p = orderByOperator.getOutput();
        while (p instanceof MarkerPage) {
            p = orderByOperator.getOutput();
        }
        outputPages.add(p);
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), outputPages.build());

        Assert.assertEquals(actual, expected);
    }
}
