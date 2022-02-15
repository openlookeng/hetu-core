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
import io.prestosql.SequencePageBuilder;
import io.prestosql.block.BlockAssertions;
import io.prestosql.execution.Lifespan;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Split;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.operator.index.PageRecordSet;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.operator.project.TestPageProcessor.LazyPagePageProjection;
import io.prestosql.operator.project.TestPageProcessor.SelectAllFilter;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingSplit;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.block.BlockAssertions.toValues;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.OperatorAssertion.toMaterializedResult;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.Signature.internalScalarFunction;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestScanFilterAndProjectOperator
        extends AbstractTestFunctions
{
    private final Metadata metadata = createTestMetadataManager();
    private final ExpressionCompiler expressionCompiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    public TestScanFilterAndProjectOperator()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @Test
    public void testPageSource()
    {
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();

        List<RowExpression> projections = ImmutableList.of(field(0, VARCHAR));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(Optional.empty(), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(Optional.empty(), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns, dynamicFilter) -> new FixedPageSource(ImmutableList.of(input)),
                cursorProcessor,
                pageProcessor,
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                null,
                ImmutableList.of(VARCHAR),
                new DataSize(0, BYTE),
                0,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                new UUID(0, 0),
                false,
                Optional.empty(),
                0,
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new CatalogName("test"), TestingSplit.createLocalSplit(), Lifespan.taskWide()));
        operator.noMoreSplits();

        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operator));

        assertEquals(actual.getRowCount(), expected.getRowCount());
        assertEquals(actual, expected);
    }

    @Test
    public void testPageSourceMergeOutput()
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .build();

        RowExpression filter = call(EQUAL.getFunctionName().toString(),
                new BuiltInFunctionHandle(Signature.internalOperator(EQUAL, BOOLEAN.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()))),
                BOOLEAN,
                field(0, BIGINT),
                constant(10L, BIGINT));
        List<RowExpression> projections = ImmutableList.of(field(0, BIGINT));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(Optional.of(filter), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(Optional.of(filter), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns, dynamicFilter) -> new FixedPageSource(input),
                cursorProcessor,
                pageProcessor,
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                null,
                ImmutableList.of(BIGINT),
                new DataSize(64, KILOBYTE),
                2,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), false, Optional.empty(), 0, 0);

        SourceOperator operator = factory.createOperator(newDriverContext());
        operator.addSplit(new Split(new CatalogName("test"), TestingSplit.createLocalSplit(), Lifespan.taskWide()));
        operator.noMoreSplits();

        List<Page> actual = toPages(operator);
        assertEquals(actual.size(), 1);

        List<Page> expected = rowPagesBuilder(BIGINT)
                .row(10L)
                .row(10L)
                .row(10L)
                .row(10L)
                .build();

        assertPageEquals(ImmutableList.of(BIGINT), actual.get(0), expected.get(0));
    }

    @Test
    public void testPageSourceLazyLoad()
    {
        Block inputBlock = BlockAssertions.createLongSequenceBlock(0, 100);
        // If column 1 is loaded, test will fail
        Page input = new Page(100, inputBlock, new LazyBlock(100, lazyBlock -> {
            throw new AssertionError("Lazy block should not be loaded");
        }));
        DriverContext driverContext = newDriverContext();

        List<RowExpression> projections = ImmutableList.of(field(0, VARCHAR));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(Optional.empty(), projections, "key");
        PageProcessor pageProcessor = new PageProcessor(Optional.of(new SelectAllFilter()), ImmutableList.of(new LazyPagePageProjection()));

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns, dynamicFilter) -> new SinglePagePageSource(input),
                cursorProcessor,
                () -> pageProcessor,
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                null,
                ImmutableList.of(BIGINT),
                new DataSize(0, BYTE),
                0,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                new UUID(0, 0),
                false,
                Optional.empty(),
                0,
                0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new CatalogName("test"), TestingSplit.createLocalSplit(), Lifespan.taskWide()));
        operator.noMoreSplits();

        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(BIGINT), ImmutableList.of(new Page(inputBlock)));
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(BIGINT), toPages(operator));

        assertEquals(actual.getRowCount(), expected.getRowCount());
        assertEquals(actual, expected);
    }

    @Test
    public void testRecordCursorSource()
    {
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();

        List<RowExpression> projections = ImmutableList.of(field(0, VARCHAR));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(Optional.empty(), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(Optional.empty(), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns, dynamicFilter) -> new RecordPageSource(new PageRecordSet(ImmutableList.of(VARCHAR), input)),
                cursorProcessor,
                pageProcessor,
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                null,
                ImmutableList.of(VARCHAR),
                new DataSize(0, BYTE),
                0,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), false, Optional.empty(), 0, 0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new CatalogName("test"), TestingSplit.createLocalSplit(), Lifespan.taskWide()));
        operator.noMoreSplits();

        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operator));

        assertEquals(actual.getRowCount(), expected.getRowCount());
        assertEquals(actual, expected);
    }

    @Test
    public void testPageYield()
    {
        int totalRows = 1000;
        Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(BIGINT), totalRows, 1);
        DriverContext driverContext = newDriverContext();

        // 20 columns; each column is associated with a function that will force yield per projection
        int totalColumns = 20;
        ImmutableList.Builder<SqlScalarFunction> functions = ImmutableList.builder();
        for (int i = 0; i < totalColumns; i++) {
            functions.add(new GenericLongFunction("page_col" + i, value -> {
                driverContext.getYieldSignal().forceYieldForTesting();
                return value;
            }));
        }
        Metadata localMetadata = functionAssertions.getMetadata();
        localMetadata.getFunctionAndTypeManager().registerBuiltInFunctions(functions.build());

        // match each column with a projection
        ExpressionCompiler compiler = new ExpressionCompiler(localMetadata, new PageFunctionCompiler(localMetadata, 0));
        ImmutableList.Builder<RowExpression> projections = ImmutableList.builder();
        for (int i = 0; i < totalColumns; i++) {
            projections.add(call(QualifiedObjectName.valueOfDefaultFunction("generic_long_page_col" + i).toString(), new BuiltInFunctionHandle(internalScalarFunction(QualifiedObjectName.valueOfDefaultFunction("generic_long_page_col" + i), BIGINT.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature()))), BIGINT, field(0, BIGINT)));
        }
        Supplier<CursorProcessor> cursorProcessor = compiler.compileCursorProcessor(Optional.empty(), projections.build(), "key");
        Supplier<PageProcessor> pageProcessor = compiler.compilePageProcessor(Optional.empty(), projections.build(), MAX_BATCH_SIZE);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns, dynamicFilter) -> new FixedPageSource(ImmutableList.of(input)),
                cursorProcessor,
                pageProcessor,
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                null,
                ImmutableList.of(BIGINT),
                new DataSize(0, BYTE),
                0,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), false, Optional.empty(), 0, 0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new CatalogName("test"), TestingSplit.createLocalSplit(), Lifespan.taskWide()));
        operator.noMoreSplits();

        // In the below loop we yield for every cell: 20 X 1000 times
        // Currently we don't check for the yield signal in the generated projection loop, we only check for the yield signal
        // in the PageProcessor.PositionsPageProcessorIterator::computeNext() method. Therefore, after 20 calls we will have
        // exactly 20 blocks (one for each column) and the PageProcessor will be able to create a Page out of it.
        for (int i = 1; i <= totalRows * totalColumns; i++) {
            driverContext.getYieldSignal().setWithDelay(SECONDS.toNanos(1000), driverContext.getYieldExecutor());
            Page page = operator.getOutput();
            if (i == totalColumns) {
                assertNotNull(page);
                assertEquals(page.getPositionCount(), totalRows);
                assertEquals(page.getChannelCount(), totalColumns);
                for (int j = 0; j < totalColumns; j++) {
                    assertEquals(toValues(BIGINT, page.getBlock(j)), toValues(BIGINT, input.getBlock(0)));
                }
            }
            else {
                assertNull(page);
            }
            driverContext.getYieldSignal().reset();
        }
    }

    @Test
    public void testRecordCursorYield()
    {
        // create a generic long function that yields for projection on every row
        // verify we will yield #row times totally

        // create a table with 15 rows
        int length = 15;
        Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(BIGINT), length, 0);
        DriverContext driverContext = newDriverContext();

        // set up generic long function with a callback to force yield
        Metadata localMetadata = functionAssertions.getMetadata();
        localMetadata.getFunctionAndTypeManager().registerBuiltInFunctions(ImmutableList.of(new GenericLongFunction("record_cursor", value -> {
            driverContext.getYieldSignal().forceYieldForTesting();
            return value;
        })));
        ExpressionCompiler compiler = new ExpressionCompiler(localMetadata, new PageFunctionCompiler(localMetadata, 0));

        List<RowExpression> projections = ImmutableList.of(call(QualifiedObjectName.valueOfDefaultFunction("generic_long_record_cursor").toString(),
                new BuiltInFunctionHandle(internalScalarFunction(QualifiedObjectName.valueOfDefaultFunction("generic_long_record_cursor"), BIGINT.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature()))),
                BIGINT,
                field(0, BIGINT)));
        Supplier<CursorProcessor> cursorProcessor = compiler.compileCursorProcessor(Optional.empty(), projections, "key");
        Supplier<PageProcessor> pageProcessor = compiler.compilePageProcessor(Optional.empty(), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (session, split, table, columns, dynamicFilter) -> new RecordPageSource(new PageRecordSet(ImmutableList.of(BIGINT), input)),
                cursorProcessor,
                pageProcessor,
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                null,
                ImmutableList.of(BIGINT),
                new DataSize(0, BYTE),
                0,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), false, Optional.empty(), 0, 0);

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new CatalogName("test"), TestingSplit.createLocalSplit(), Lifespan.taskWide()));
        operator.noMoreSplits();

        // start driver; get null value due to yield for the first 15 times
        for (int i = 0; i < length; i++) {
            driverContext.getYieldSignal().setWithDelay(SECONDS.toNanos(1000), driverContext.getYieldExecutor());
            assertNull(operator.getOutput());
            driverContext.getYieldSignal().reset();
        }

        // the 16th yield is not going to prevent the operator from producing a page
        driverContext.getYieldSignal().setWithDelay(SECONDS.toNanos(1000), driverContext.getYieldExecutor());
        Page output = operator.getOutput();
        driverContext.getYieldSignal().reset();
        assertNotNull(output);
        assertEquals(toValues(BIGINT, output.getBlock(0)), toValues(BIGINT, input.getBlock(0)));
    }

    @Test
    public void testReusePageSource()
    {
        UUID uuid = UUID.randomUUID();
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();
        List<Page> producerPages;

        SourceOperator operatorProducer = createScanFilterAndProjectOperator("0", uuid, 0, input, driverContext, ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER, false, 10, 1);

        producerPages = toPages(operatorProducer);
        MaterializedResult producerExpected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult producerActual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), producerPages);
        assertEquals(producerActual.getRowCount(), producerExpected.getRowCount());
        assertEquals(producerActual, producerExpected);

        //Consumer
        SourceOperator operatorConsumer = createScanFilterAndProjectOperator("0", uuid, 1, input, driverContext, ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER, false, 10, 1);
        MaterializedResult consumerExpected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), producerPages);
        MaterializedResult consumerActual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operatorConsumer));
        assertEquals(consumerActual.getRowCount(), consumerExpected.getRowCount());
        assertEquals(consumerActual, consumerExpected);
    }

    @Test
    public void testReuseExchangeSpill()
    {
        UUID uuid = UUID.randomUUID();
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();
        List<Page> producerPages;

        SourceOperator operatorProducer = createScanFilterAndProjectOperator("0", uuid, 0, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER, true, 10, 1);
        WorkProcessorSourceOperatorAdapter workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorProducer;
        producerPages = toPages(operatorProducer);

        // check spilling is done
        boolean notSpilled = workProcessorSourceOperatorAdapter.isNotSpilled();
        assertEquals(false, notSpilled);
        MaterializedResult producerExpected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult producerActual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), producerPages);
        assertEquals(producerActual.getRowCount(), producerExpected.getRowCount());
        assertEquals(producerActual, producerExpected);

        //Consumer
        SourceOperator operatorConsumer = createScanFilterAndProjectOperator("0", uuid, 1, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER, true, 10, 1);
        MaterializedResult consumerExpected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), producerPages);
        MaterializedResult consumerActual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operatorConsumer));
        assertEquals(consumerActual.getRowCount(), consumerExpected.getRowCount());
        assertEquals(consumerActual, consumerExpected);
    }

    @Test
    public void testReuseExchangeInMemorySpill()
    {
        UUID uuid = UUID.randomUUID();
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();
        List<Page> producerPages;

        SourceOperator operatorProducer = createScanFilterAndProjectOperator("0", uuid, 0, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER, true, 75000, 1);
        WorkProcessorSourceOperatorAdapter workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorProducer;
        producerPages = toPages(operatorProducer);

        // check spilling is done
        boolean notSpilled = workProcessorSourceOperatorAdapter.isNotSpilled();
        assertEquals(true, notSpilled);
        MaterializedResult producerExpected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult producerActual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), producerPages);
        assertEquals(producerActual.getRowCount(), producerExpected.getRowCount());
        assertEquals(producerActual, producerExpected);

        //Consumer
        SourceOperator operatorConsumer = createScanFilterAndProjectOperator("0", uuid, 1, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER, true, 75000, 0);
        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer;
        boolean flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(false, flag);
        MaterializedResult consumerExpected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), producerPages);
        MaterializedResult consumerActual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operatorConsumer));
        assertEquals(consumerActual.getRowCount(), consumerExpected.getRowCount());
        assertEquals(consumerActual, consumerExpected);

        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer;
        flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(true, flag);
    }

    @Test
    public void testReuseExchangeMultipleConsumer()
    {
        UUID uuid = UUID.randomUUID();
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();
        List<Page> producerPages;

        SourceOperator operatorProducer = createScanFilterAndProjectOperator("0", uuid, 0, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER, true, 800000, 2);
        WorkProcessorSourceOperatorAdapter workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorProducer;
        producerPages = toPages(operatorProducer);

        // check spilling is done
        boolean notSpilled = workProcessorSourceOperatorAdapter.isNotSpilled();
        assertEquals(true, notSpilled);
        MaterializedResult producerExpected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult producerActual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), producerPages);
        assertEquals(producerActual.getRowCount(), producerExpected.getRowCount());
        assertEquals(producerActual, producerExpected);

        //Consumer 1
        SourceOperator operatorConsumer = createScanFilterAndProjectOperator("1", uuid, 1, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER, true, 800000, 0);
        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer;
        boolean flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(false, flag);
        MaterializedResult consumerExpected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), producerPages);
        MaterializedResult consumerActual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operatorConsumer));
        assertEquals(consumerActual.getRowCount(), consumerExpected.getRowCount());
        assertEquals(consumerActual, consumerExpected);
        flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(true, flag);

        //Consumer 2
        SourceOperator operatorConsumer1 = createScanFilterAndProjectOperator("2", uuid, 2, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER, true, 80000, 0);
        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer1;
        flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(false, flag);
        MaterializedResult consumerExpected1 = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), producerPages);
        MaterializedResult consumerActual1 = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operatorConsumer1));
        assertEquals(consumerActual1.getRowCount(), consumerExpected1.getRowCount());
        assertEquals(consumerActual1, consumerExpected1);

        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer1;
        flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(true, flag);
    }

    @Test
    public void testReuseExchangeInMemorySpillMultipleConsumer()
    {
        UUID uuid = UUID.randomUUID();
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();
        List<Page> producerPages;

        SourceOperator operatorProducer = createScanFilterAndProjectOperator("0", uuid, 0, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER, true, 75000, 2);
        WorkProcessorSourceOperatorAdapter workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorProducer;
        producerPages = toPages(operatorProducer);

        // check spilling is done
        boolean notSpilled = workProcessorSourceOperatorAdapter.isNotSpilled();
        assertEquals(true, notSpilled);
        MaterializedResult producerExpected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult producerActual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), producerPages);
        assertEquals(producerActual.getRowCount(), producerExpected.getRowCount());
        assertEquals(producerActual, producerExpected);

        //Consumer 1
        SourceOperator operatorConsumer = createScanFilterAndProjectOperator("1", uuid, 1, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER, true, 75000, 0);
        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer;
        boolean flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(false, flag);
        List<Page> consumer1Pages = new ArrayList<>();
        // read pages 1st time from pageCaches
        consumer1Pages.addAll(toPages(operatorConsumer, true));
        flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(false, flag);

        //Consumer 2
        SourceOperator operatorConsumer2 = createScanFilterAndProjectOperator("2", uuid, 2, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER, true, 75000, 0);
        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer2;
        flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(false, flag);
        List<Page> consumer2Pages = new ArrayList<>();
        // read pages 1st time from pageCaches
        consumer2Pages.addAll(toPages(operatorConsumer2));
        assertEquals(producerPages.size(), consumer2Pages.size());

        // read pages 2nd time after pages are moved form pagesToSpill to pageCaches
        consumer1Pages.addAll(toPages(operatorConsumer));
        assertEquals(producerPages.size(), consumer1Pages.size());

        //consumer1 should be finished after reading from pagesToSpill & pageCaches
        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer;
        flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(true, flag);

        //consumer2 should be finished after reading from pagesToSpill & pageCaches
        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer2;
        flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(true, flag);
    }

    @Test
    public void testReuseExchangeSpillToDiskMultipleConsumer()
    {
        UUID uuid = UUID.randomUUID();
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();
        List<Page> producerPages;

        SourceOperator operatorProducer = createScanFilterAndProjectOperator("0", uuid, 0, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER, true, 4000, 2);
        WorkProcessorSourceOperatorAdapter workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorProducer;
        producerPages = toPages(operatorProducer);

        // check spilling is done
        boolean notSpilled = workProcessorSourceOperatorAdapter.isNotSpilled();
        assertEquals(false, notSpilled);
        MaterializedResult producerExpected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult producerActual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), producerPages);
        assertEquals(producerActual.getRowCount(), producerExpected.getRowCount());
        assertEquals(producerActual, producerExpected);

        //Consumer 1
        SourceOperator operatorConsumer = createScanFilterAndProjectOperator("1", uuid, 1, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER, true, 4000, 0);
        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer;
        boolean flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(false, flag);
        List<Page> consumer1Pages = new ArrayList<>();
        // read pages 1st time from pageCaches
        consumer1Pages.addAll(toPages(operatorConsumer, true));
        flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(false, flag);

        //Consumer 2
        SourceOperator operatorConsumer2 = createScanFilterAndProjectOperator("2", uuid, 2, input, driverContext,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER, true, 4000, 0);
        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer2;
        flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(false, flag);
        List<Page> consumer2Pages = new ArrayList<>();
        //since its last consumer it will read pages  pageCaches & pagesToSpill at a time
        consumer2Pages.addAll(toPages(operatorConsumer2));
        assertEquals(producerPages.size(), consumer2Pages.size());

        //consumer2 should be finished after reading from pagesToSpill & pageCaches
        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer2;
        flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(true, flag);

        // read pages 2nd time after pages are moved form pagesToSpill to pageCaches
        consumer1Pages.addAll(toPages(operatorConsumer));
        assertEquals(producerPages.size(), consumer1Pages.size());

        //consumer1 should be finished after reading from pagesToSpill & pageCaches
        workProcessorSourceOperatorAdapter = (WorkProcessorSourceOperatorAdapter) operatorConsumer;
        flag = getWorkProcessorSourceOperatorAdapterCheckFinished(workProcessorSourceOperatorAdapter);
        assertEquals(true, flag);
    }

    private boolean getWorkProcessorSourceOperatorAdapterCheckFinished(WorkProcessorSourceOperatorAdapter workProcessorSourceOperatorAdapter)
    {
        boolean returnValue = false;
        try {
            Method privateStringMethod = WorkProcessorSourceOperatorAdapter.class.getDeclaredMethod("checkFinished", null);
            privateStringMethod.setAccessible(true);
            returnValue = (boolean)
                    privateStringMethod.invoke(workProcessorSourceOperatorAdapter, null);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            // the exception could be ignored
        }
        return returnValue;
    }

    private SourceOperator createScanFilterAndProjectOperator(String sourceId, UUID uuid, int operatorId, Page input, DriverContext driverContext,
                                                              ReuseExchangeOperator.STRATEGY strategy, boolean spillEnabled, Integer spillerThreshold,
                                                              Integer consumerTableScanNodeCount)
    {
        List<RowExpression> projections = ImmutableList.of(field(0, VARCHAR));
        Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(Optional.empty(), projections, "key");
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(Optional.empty(), projections);

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                operatorId,
                new PlanNodeId("test"),
                new PlanNodeId(sourceId),
                (session, split, table, columns, dynamicFilter) -> new FixedPageSource(ImmutableList.of(input)),
                cursorProcessor,
                pageProcessor,
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                null,
                ImmutableList.of(VARCHAR),
                new DataSize(0, BYTE),
                0,
                strategy,
                uuid,
                spillEnabled,
                Optional.of(new DummySpillerFactory()),
                spillerThreshold,
                consumerTableScanNodeCount);
        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new CatalogName("test"), TestingSplit.createLocalSplit(), Lifespan.taskWide()));
        operator.noMoreSplits();
        return operator;
    }

    private static List<Page> toPages(Operator operator)
    {
        return toPages(operator, false);
    }

    private static List<Page> toPages(Operator operator, boolean retNotFinished)
    {
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        // read output until input is needed or operator is finished
        int nullPages = 0;
        while (!operator.isFinished()) {
            Page outputPage = operator.getOutput();
            if (outputPage == null) {
                if (retNotFinished) {
                    return outputPages.build();
                }
                // break infinite loop due to null pages
                assertTrue(nullPages < 1_000_000, "Too many null pages; infinite loop?");
                nullPages++;
            }
            else {
                outputPages.add(outputPage);
                nullPages = 0;
            }
        }

        return outputPages.build();
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    public static class SinglePagePageSource
            implements ConnectorPageSource
    {
        private Page page;

        public SinglePagePageSource(Page page)
        {
            this.page = page;
        }

        @Override
        public void close()
        {
            page = null;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public long getSystemMemoryUsage()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return page == null;
        }

        @Override
        public Page getNextPage()
        {
            Page tmpPage = this.page;
            this.page = null;
            return tmpPage;
        }
    }
}
