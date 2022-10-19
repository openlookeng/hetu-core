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
import io.prestosql.execution.buffer.OutputBufferStateMachine;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.PartitionedOutputBuffer;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.operator.exchange.LocalPartitionGenerator;
import io.prestosql.operator.output.PartitionedOutputOperator;
import io.prestosql.operator.output.PartitionedOutputOperator.PartitionedOutputFactory;
import io.prestosql.operator.output.PositionsAppenderFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.TestingTaskContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.prestosql.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(2)
@Warmup(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPartitionedOutputOperator
{
    private static final PositionsAppenderFactory POSITIONS_APPENDER_FACTORY = new PositionsAppenderFactory();

    @Benchmark
    public void addPage(BenchmarkData data)
    {
        PartitionedOutputOperator operator = data.createPartitionedOutputOperator();
        for (int i = 0; i < data.getPageCount(); i++) {
            operator.addInput(data.getDataPage());
        }
        operator.finish();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int PAGE_COUNT = 5000;
        private static final int PARTITION_COUNT = 512;
        private static final int ENTRIES_PER_PAGE = 256;
        private static final DataSize MAX_MEMORY = new DataSize(1, GIGABYTE);
        private static final RowType rowType = RowType.anonymous(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR));
        private static final List<Type> TYPES = ImmutableList.of(BIGINT, rowType, rowType, rowType);
        private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-EXECUTOR-%s"));
        private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("test-%s"));

        private final Page dataPage = createPage();

        private int getPageCount()
        {
            return PAGE_COUNT;
        }

        public Page getDataPage()
        {
            return dataPage;
        }

        private PartitionedOutputOperator createPartitionedOutputOperator()
        {
            PartitionFunction partitionFunction = new LocalPartitionGenerator(new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0}), PARTITION_COUNT);
            OutputBuffers buffers = createInitialEmptyOutputBuffers(PARTITIONED);
            for (int partition = 0; partition < PARTITION_COUNT; partition++) {
                buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
            }
            PartitionedOutputBuffer buffer = createPartitionedBuffer(
                    buffers.withNoMoreBufferIds(),
                    new DataSize(Long.MAX_VALUE, BYTE)); // don't let output buffer block
            PartitionedOutputFactory operatorFactory = new PartitionedOutputFactory(
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty()),
                    false,
                    OptionalInt.empty(),
                    buffer,
                    new DataSize(1, GIGABYTE),
                    POSITIONS_APPENDER_FACTORY);
            TaskContext taskContext = createTaskContext();
            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), TYPES, Function.identity(), taskContext)
                    .createOperator(createDriverContext(taskContext));
        }

        private Page createPage()
        {
            List<Object>[] testRows = generateTestRows(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR), ENTRIES_PER_PAGE);
            PageBuilder pageBuilder = new PageBuilder(TYPES);
            BlockBuilder bigintBlockBuilder = pageBuilder.getBlockBuilder(0);
            BlockBuilder rowBlockBuilder = pageBuilder.getBlockBuilder(1);
            BlockBuilder rowBlockBuilder2 = pageBuilder.getBlockBuilder(2);
            BlockBuilder rowBlockBuilder3 = pageBuilder.getBlockBuilder(3);
            for (int i = 0; i < ENTRIES_PER_PAGE; i++) {
                BIGINT.writeLong(bigintBlockBuilder, i);
                writeRow(testRows[i], rowBlockBuilder);
                writeRow(testRows[i], rowBlockBuilder2);
                writeRow(testRows[i], rowBlockBuilder3);
            }
            pageBuilder.declarePositions(ENTRIES_PER_PAGE);
            return pageBuilder.build();
        }

        private void writeRow(List<Object> testRow, BlockBuilder rowBlockBuilder)
        {
            BlockBuilder singleRowBlockWriter = rowBlockBuilder.beginBlockEntry();
            for (Object fieldValue : testRow) {
                if (fieldValue instanceof String) {
                    VARCHAR.writeSlice(singleRowBlockWriter, utf8Slice((String) fieldValue));
                }
                else {
                    throw new UnsupportedOperationException();
                }
            }
            rowBlockBuilder.closeEntry();
        }

        // copied & modifed from TestRowBlock
        private List<Object>[] generateTestRows(List<Type> fieldTypes, int numRows)
        {
            List<Object>[] testRows = new List[numRows];
            for (int i = 0; i < numRows; i++) {
                List<Object> testRow = new ArrayList<>(fieldTypes.size());
                for (int j = 0; j < fieldTypes.size(); j++) {
                    if (fieldTypes.get(j) == VARCHAR) {
                        byte[] data = new byte[ThreadLocalRandom.current().nextInt(128)];
                        ThreadLocalRandom.current().nextBytes(data);
                        testRow.add(new String(data, StandardCharsets.UTF_8));
                    }
                    else {
                        throw new UnsupportedOperationException();
                    }
                }
                testRows[i] = testRow;
            }
            return testRows;
        }

        private TaskContext createTaskContext()
        {
            return TestingTaskContext.builder(EXECUTOR, SCHEDULER, TEST_SESSION)
                    .setMemoryPoolSize(MAX_MEMORY)
                    .build();
        }

        private DriverContext createDriverContext(TaskContext taskContext)
        {
            return taskContext
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();
        }

        private PartitionedOutputBuffer createPartitionedBuffer(OutputBuffers buffers, DataSize dataSize)
        {
            return new PartitionedOutputBuffer(
                    new OutputBufferStateMachine("bufferState", SCHEDULER),
                    buffers,
                    dataSize,
                    () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                    SCHEDULER);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        new BenchmarkPartitionedOutputOperator().addPage(data);
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .jvmArgs("-Xmx10g")
                .include(".*" + BenchmarkPartitionedOutputOperator.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
