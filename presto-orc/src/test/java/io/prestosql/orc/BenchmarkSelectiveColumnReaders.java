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
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.orc.OrcTester.writeOrcColumnPresto;
import static io.prestosql.orc.metadata.CompressionKind.NONE;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.nio.file.Files.createTempDirectory;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSelectiveColumnReaders
{
    public static final int ROWS = 10_000_000;
    public static final List<?> NULL_VALUES = Collections.nCopies(ROWS, null);

    @Benchmark
    public Object readAllNull(AllNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(Optional.empty()));
    }

    @Benchmark
    public Object readBooleanNoNull(BooleanNoNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(Optional.empty()));
    }

    @Benchmark
    public Object readBooleanNoNullWithFilter(BooleanNoNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(Optional.of(TupleDomainFilter.BooleanValue.of(true, true))));
    }

    @Benchmark
    public Object readBooleanWithNull(BooleanWithNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(Optional.empty()));
    }

    @Benchmark
    public Object readBooleanWithNullWithFilter(BooleanWithNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(Optional.of(TupleDomainFilter.BooleanValue.of(true, true))));
    }

    private static List<Block> readAllBlocks(OrcSelectiveRecordReader recordReader)
            throws IOException
    {
        List<Block> blocks = new ArrayList<>();
        while (true) {
            Page page = recordReader.getNextPage();
            if (page == null) {
                break;
            }

            blocks.add(page.getBlock(0));
        }
        return blocks;
    }

    private abstract static class BenchmarkData
    {
        protected final Random random = new Random(0);
        private Type type;
        private File temporaryDirectory;
        private File orcFile;

        public void setup(Type type)
                throws Exception
        {
            this.type = type;
            temporaryDirectory = createTempDirectory(getClass().getName()).toFile();
            orcFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnPresto(orcFile, NONE, type, createValues(), new OrcWriterStats());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        public Type getType()
        {
            return type;
        }

        protected abstract Iterator<?> createValues();

        public OrcSelectiveRecordReader createRecordReader(Optional<TupleDomainFilter> filter)
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(orcFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true, orcFile.lastModified());
            OrcReader orcReader = new OrcReader(dataSource, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));

            return orcReader.createSelectiveRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    orcReader.getRootColumn().getNestedColumns(),
                    ImmutableList.of(type),
                    ImmutableList.of(0),
                    ImmutableMap.of(),
                    filter.map(f -> ImmutableMap.of(0, f)).orElse(ImmutableMap.of()),
                    null,
                    OrcPredicate.TRUE,
                    0,
                    dataSource.getSize(),
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE,
                    RuntimeException::new,
                    Optional.empty(),
                    Collections.emptyMap(),
                    OrcCacheStore.CACHE_NOTHING,
                    new OrcCacheProperties(),
                    Optional.empty(),
                    new HashMap<>(),
                    new ArrayList<>(),
                    false,
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    new HashSet<>());
        }
    }

    @State(Scope.Thread)
    public static class AllNullBenchmarkData
            extends BenchmarkData
    {
        @SuppressWarnings("unused")
        @Param("boolean")
        private String typeSignature;

        @Setup
        public void setup()
                throws Exception
        {
            Type type = createTestMetadataManager().getType(TypeSignature.parseTypeSignature(typeSignature));
            setup(type);
        }

        @Override
        protected final Iterator<?> createValues()
        {
            return NULL_VALUES.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BooleanNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BOOLEAN);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Boolean> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextBoolean());
            }
            return values.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BooleanWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BOOLEAN);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Boolean> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextBoolean() ? random.nextBoolean() : null);
            }
            return values.iterator();
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSelectiveColumnReaders.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
