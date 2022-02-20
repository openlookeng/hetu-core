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
package io.prestosql.operator.spiller;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.FileSingleStreamSpillerFactory;
import io.prestosql.spiller.GenericSpillerFactory;
import io.prestosql.spiller.Spiller;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.spiller.SpillerStats;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkBinaryFileSpiller
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, BIGINT, DOUBLE, createUnboundedVarcharType(), DOUBLE);
    private static final BlockEncodingSerde BLOCK_ENCODING_SERDE = createTestMetadataManager().getFunctionAndTypeManager().getBlockEncodingSerde();
    private static final Path SPILL_PATH = Paths.get(System.getProperty("java.io.tmpdir"), "spills");

    @Benchmark
    public void writeReadSpill(BenchmarkData data)
            throws ExecutionException, InterruptedException
    {
        try (Spiller spiller = data.createSpiller()) {
            spiller.spill(data.getPages().iterator()).get();

            List<Iterator<Page>> spills = spiller.getSpills();
            for (Iterator<Page> spill : spills) {
                while (spill.hasNext()) {
                    Page next = spill.next();
                    next.getPositionCount();
                }
            }
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private final SpillerStats spillerStats = new SpillerStats();

        @Param("10000")
        private int rowsPerPage = 10000;

        @Param("10")
        private int pagesCount = 10;

        @Param({"false", "true"})
        private boolean compressionEnabled;

        @Param({"false", "true"})
        private boolean encryptionEnabled;

        @Param({"false", "true"})
        private boolean directSerdeEnabled;

        @Param({"1", "25"})
        private int spillPrefetchReadPages = 1;

        private List<Page> pages;

        private FileSingleStreamSpillerFactory singleStreamSpillerFactory;
        private SpillerFactory spillerFactory;

        @Setup
        public void setup()
                throws ExecutionException, InterruptedException, IOException
        {
            FileSystemClientManager fileSystemClientManager = mock(FileSystemClientManager.class);
            when(fileSystemClientManager.getFileSystemClient(SPILL_PATH)).thenReturn(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), SPILL_PATH));
            singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(
                    MoreExecutors.newDirectExecutorService(),
                    BLOCK_ENCODING_SERDE,
                    spillerStats,
                    ImmutableList.of(SPILL_PATH),
                    1.0,
                    compressionEnabled,
                    encryptionEnabled,
                    directSerdeEnabled,
                    spillPrefetchReadPages,
                    false,
                    null,
                    fileSystemClientManager);
            spillerFactory = new GenericSpillerFactory(singleStreamSpillerFactory);
            pages = createInputPages();
        }

        @TearDown
        public void tearDown()
        {
            singleStreamSpillerFactory.destroy();
        }

        private List<Page> createInputPages()
        {
            ImmutableList.Builder<Page> builder = ImmutableList.builder();

            PageBuilder pageBuilder = new PageBuilder(TYPES);
            LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
            for (int j = 0; j < pagesCount; j++) {
                Iterator<LineItem> iterator = lineItemGenerator.iterator();
                for (int i = 0; i < rowsPerPage; i++) {
                    pageBuilder.declarePosition();

                    LineItem lineItem = iterator.next();
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(0), lineItem.getOrderKey());
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(1), lineItem.getDiscountPercent());
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(2), lineItem.getDiscount());
                    VARCHAR.writeString(pageBuilder.getBlockBuilder(3), lineItem.getReturnFlag());
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(4), lineItem.getExtendedPrice());
                }
                builder.add(pageBuilder.build());
                pageBuilder.reset();
            }

            return builder.build();
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public Spiller createSpiller()
        {
            return spillerFactory.create(TYPES, bytes -> {}, newSimpleAggregatedMemoryContext());
        }
    }

    @Test
    public void testBenchmark() throws ExecutionException, InterruptedException, IOException
    {
        BenchmarkData ctx = new BenchmarkData();
        ctx.setup();
        writeReadSpill(ctx);
        ctx.tearDown();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkBinaryFileSpiller.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
