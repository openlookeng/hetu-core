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
import io.prestosql.RowPagesBuilder;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.prestosql.operator.StreamingAggregationOperator.StreamingAggregationOperatorFactory;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.testing.TestingTaskContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.block.BlockAssertions.createLongSequenceBlock;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.BenchmarkHashAndStreamingAggregationOperators.Context.ROWS_PER_PAGE;
import static io.prestosql.operator.BenchmarkHashAndStreamingAggregationOperators.Context.TOTAL_PAGES;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.testng.Assert.assertEquals;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 5)
@Measurement(iterations = 10, time = 2, timeUnit = SECONDS)
public class BenchmarkHashAndStreamingAggregationOperators
{
    private static final Metadata metadata = createTestMetadataManager();

    private static final InternalAggregationFunction LONG_SUM = metadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("sum"), AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction COUNT = metadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE, BIGINT.getTypeSignature()));

    @State(Thread)
    public static class Context
    {
        public static final int TOTAL_PAGES = 140;
        public static final int ROWS_PER_PAGE = 10_000;

        @Param({"1", "10", "1000"})
        public int rowsPerGroup;

        @Param({"streaming", "hash"})
        public String operatorType;

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;
        private List<Page> pages;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

            int groupsPerPage = ROWS_PER_PAGE / rowsPerGroup;

            boolean hashAggregation = operatorType.equalsIgnoreCase("hash");

            RowPagesBuilder pagesBuilder = RowPagesBuilder.rowPagesBuilder(hashAggregation, ImmutableList.of(0), VARCHAR, BIGINT);
            for (int i = 0; i < TOTAL_PAGES; i++) {
                BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, ROWS_PER_PAGE);
                for (int j = 0; j < groupsPerPage; j++) {
                    String groupKey = format("%s", i * groupsPerPage + j);
                    repeatToStringBlock(groupKey, rowsPerGroup, blockBuilder);
                }
                pagesBuilder.addBlocksPage(blockBuilder.build(), createLongSequenceBlock(0, ROWS_PER_PAGE));
            }

            pages = pagesBuilder.build();

            if (hashAggregation) {
                operatorFactory = createHashAggregationOperatorFactory(pagesBuilder.getHashChannel());
            }
            else {
                operatorFactory = createStreamingAggregationOperatorFactory();
            }
        }

        @TearDown
        public void cleanup()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        private OperatorFactory createStreamingAggregationOperatorFactory()
        {
            return new StreamingAggregationOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    ImmutableList.of(VARCHAR),
                    ImmutableList.of(VARCHAR),
                    ImmutableList.of(0),
                    AggregationNode.Step.SINGLE,
                    ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                            LONG_SUM.bind(ImmutableList.of(1), Optional.empty())),
                    new JoinCompiler(createTestMetadataManager()));
        }

        private OperatorFactory createHashAggregationOperatorFactory(Optional<Integer> hashChannel)
        {
            JoinCompiler joinCompiler = new JoinCompiler(createTestMetadataManager());
            SpillerFactory spillerFactory = (types, localSpillContext, aggregatedMemoryContext, isSnapshotEnabled, queryId, isSpillToHdfs) -> null;

            return new HashAggregationOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    ImmutableList.of(VARCHAR),
                    ImmutableList.of(0),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    false,
                    ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                            LONG_SUM.bind(ImmutableList.of(1), Optional.empty())),
                    hashChannel,
                    Optional.empty(),
                    100_000,
                    Optional.of(new DataSize(16, MEGABYTE)),
                    false,
                    succinctBytes(8),
                    succinctBytes(Integer.MAX_VALUE),
                    spillerFactory,
                    joinCompiler,
                    false,
                    Optional.empty());
        }

        private static void repeatToStringBlock(String value, int count, BlockBuilder blockBuilder)
        {
            for (int i = 0; i < count; i++) {
                VARCHAR.writeString(blockBuilder, value);
            }
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(2, GIGABYTE));
        }

        public OperatorFactory getOperatorFactory()
        {
            return operatorFactory;
        }

        public List<Page> getPages()
        {
            return pages;
        }
    }

    @Benchmark
    public List<Page> benchmark(Context context)
    {
        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        Operator operator = context.getOperatorFactory().createOperator(driverContext);

        Iterator<Page> input = context.getPages().iterator();
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        boolean finishing = false;
        for (int loops = 0; !operator.isFinished() && loops < 1_000_000; loops++) {
            if (operator.needsInput()) {
                if (input.hasNext()) {
                    Page inputPage = input.next();
                    operator.addInput(inputPage);
                }
                else if (!finishing) {
                    operator.finish();
                    finishing = true;
                }
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        return outputPages.build();
    }

    @Test
    public void verifyStreaming()
    {
        verify(1, "streaming");
        verify(10, "streaming");
        verify(1000, "streaming");
    }

    @Test
    public void verifyHash()
    {
        verify(1, "hash");
        verify(10, "hash");
        verify(1000, "hash");
    }

    private void verify(int rowsPerGroup, String operatorType)
    {
        Context context = new Context();
        context.operatorType = operatorType;
        context.rowsPerGroup = rowsPerGroup;
        context.setup();

        assertEquals(TOTAL_PAGES, context.getPages().size());
        for (int i = 0; i < TOTAL_PAGES; i++) {
            assertEquals(ROWS_PER_PAGE, context.getPages().get(i).getPositionCount());
        }

        List<Page> outputPages = benchmark(context);
        assertEquals(TOTAL_PAGES * ROWS_PER_PAGE / rowsPerGroup, outputPages.stream().mapToInt(Page::getPositionCount).sum());

        context.cleanup();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkHashAndStreamingAggregationOperators.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
