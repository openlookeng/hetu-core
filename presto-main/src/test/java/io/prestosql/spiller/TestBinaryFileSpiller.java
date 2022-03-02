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
package io.prestosql.spiller;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.prestosql.RowPagesBuilder;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.testing.TestingPagesSerdeFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Double.doubleToLongBits;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestBinaryFileSpiller
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BIGINT);

    private final File spillPath = createTempDirectory(getClass().getName()).toFile();
    private final File spillUploadPath = createTempDirectory(getClass().getName()).toFile();
    private SpillerStats spillerStats;
    private FileSingleStreamSpillerFactory singleStreamSpillerFactory;
    private SpillerFactory factory;
    private PagesSerde pagesSerde;
    private AggregatedMemoryContext memoryContext;

    public TestBinaryFileSpiller()
            throws IOException
    {}

    @BeforeMethod
    public void setUp() throws IOException
    {
        Metadata metadata = createTestMetadataManager();
        FileSystemClientManager fileSystemClientManager = mock(FileSystemClientManager.class);
        when(fileSystemClientManager.getFileSystemClient(any(Path.class))).thenReturn(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(spillPath.getCanonicalPath())));
        spillerStats = new SpillerStats();
        FeaturesConfig featuresConfig = new FeaturesConfig();
        try {
            featuresConfig.setSpillerSpillPaths(spillPath.getCanonicalPath());
        }
        catch (IOException e) {
            System.out.println(e.getStackTrace());
        }
        featuresConfig.setSpillMaxUsedSpaceThreshold(1.0);
        NodeSpillConfig nodeSpillConfig = new NodeSpillConfig();
        singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(metadata, spillerStats, featuresConfig, nodeSpillConfig, fileSystemClientManager);
        factory = new GenericSpillerFactory(singleStreamSpillerFactory);
        PagesSerdeFactory pagesSerdeFactory = new PagesSerdeFactory(metadata.getFunctionAndTypeManager().getBlockEncodingSerde(), nodeSpillConfig.isSpillCompressionEnabled());
        pagesSerde = pagesSerdeFactory.createPagesSerde();
        memoryContext = newSimpleAggregatedMemoryContext();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        singleStreamSpillerFactory.destroy();
        deleteRecursively(spillPath.toPath(), ALLOW_INSECURE);
        if (spillUploadPath.exists()) {
            deleteRecursively(spillUploadPath.toPath(), ALLOW_INSECURE);
        }
    }

    @Test
    public void testFileSpiller()
            throws Exception
    {
        try (Spiller spiller = factory.create(TYPES, bytes -> {}, memoryContext)) {
            testSimpleSpiller(spiller);
        }
    }

    @Test
    public void testFileVarbinarySpiller()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARBINARY);

        BlockBuilder col1 = BIGINT.createBlockBuilder(null, 1);
        BlockBuilder col2 = DOUBLE.createBlockBuilder(null, 1);
        BlockBuilder col3 = VARBINARY.createBlockBuilder(null, 1);

        col1.writeLong(42).closeEntry();
        col2.writeLong(doubleToLongBits(43.0)).closeEntry();
        col3.writeLong(doubleToLongBits(43.0)).writeLong(1).closeEntry();

        Page page = new Page(col1.build(), col2.build(), col3.build());

        try (Spiller spiller = factory.create(TYPES, bytes -> {}, memoryContext)) {
            testSpiller(types, spiller, ImmutableList.of(page));
        }
    }

    private void testSimpleSpiller(Spiller spiller)
            throws ExecutionException, InterruptedException
    {
        RowPagesBuilder builder = RowPagesBuilder.rowPagesBuilder(TYPES);
        builder.addSequencePage(10, 0, 5, 10, 15);
        builder.pageBreak();
        builder.addSequencePage(10, 0, -5, -10, -15);
        List<Page> firstSpill = builder.build();

        builder = RowPagesBuilder.rowPagesBuilder(TYPES);
        builder.addSequencePage(10, 10, 15, 20, 25);
        builder.pageBreak();
        builder.addSequencePage(10, -10, -15, -20, -25);
        List<Page> secondSpill = builder.build();

        testSpiller(TYPES, spiller, firstSpill, secondSpill);
    }

    private void testSpiller(List<Type> types, Spiller spiller, List<Page>... spills)
            throws ExecutionException, InterruptedException
    {
        long spilledBytesBefore = spillerStats.getTotalSpilledBytes();
        long spilledBytes = 0;

        assertEquals(memoryContext.getBytes(), 0);
        for (List<Page> spill : spills) {
            spilledBytes += spill.stream()
                    .mapToLong(page -> pagesSerde.serialize(page).getSizeInBytes())
                    .sum();
            spiller.spill(spill.iterator()).get();
        }
        assertEquals(spillerStats.getTotalSpilledBytes() - spilledBytesBefore, spilledBytes);
        // At this point, the buffers should still be accounted for in the memory context, because
        // the spiller (FileSingleStreamSpiller) doesn't release its memory reservation until it's closed.
        assertEquals(memoryContext.getBytes(), spills.length * FileSingleStreamSpiller.BUFFER_SIZE);

        List<Iterator<Page>> actualSpills = spiller.getSpills();
        assertEquals(actualSpills.size(), spills.length);

        for (int i = 0; i < actualSpills.size(); i++) {
            List<Page> actualSpill = ImmutableList.copyOf(actualSpills.get(i));
            List<Page> expectedSpill = spills[i];

            assertEquals(actualSpill.size(), expectedSpill.size());
            for (int j = 0; j < actualSpill.size(); j++) {
                assertPageEquals(types, actualSpill.get(j), expectedSpill.get(j));
            }
        }
        spiller.close();
        assertEquals(memoryContext.getBytes(), 0);
    }

    @Test
    public void testFileVarbinaryComittableSpiller()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARBINARY);

        BlockBuilder col1 = BIGINT.createBlockBuilder(null, 1);
        BlockBuilder col2 = DOUBLE.createBlockBuilder(null, 1);
        BlockBuilder col3 = VARBINARY.createBlockBuilder(null, 1);

        col1.writeLong(42).closeEntry();
        col2.writeLong(doubleToLongBits(43.0)).closeEntry();
        col3.writeLong(doubleToLongBits(43.0)).writeLong(1).closeEntry();

        Page page = new Page(col1.build(), col2.build(), col3.build());

        testSpillerUnCommit(types,
                ImmutableList.of(page),
                ImmutableList.of(page, page),
                ImmutableList.of(page, page, page));
    }

    private void testSpillerUnCommit(List<Type> types, List<Page>... spills)
            throws ExecutionException, InterruptedException, IOException
    {
        long spilledBytesBefore = spillerStats.getTotalSpilledBytes();
        long spilledBytes = 0;

        assertEquals(memoryContext.getBytes(), 0);
        List<Runnable> runners = new ArrayList<>();
        PagesSerde serde = TestingPagesSerdeFactory.testingPagesSerde();

        Spiller spiller = factory.create(TYPES, bytes -> {}, memoryContext);
        spilledBytes = doSpill(spiller, spilledBytes, runners, spills, 0);
        spillUploadPath.mkdirs();

        int counter = 1;
        int runCount = runners.size();
        List<Path> uploadedFile = new ArrayList<>();
        for (counter = 1; counter <= runCount; counter++) {
            runners.remove(0).run();

            assertEquals(spiller.getSpilledFilePaths().size(), counter);
            assertEquals(runners.size(), runCount - counter);

            Object snapshot = spiller.capture(serde);
            spiller.getSpilledFilePaths().stream().forEach(path -> {
                try {
                    Files.copy(path, Paths.get(spillUploadPath.getPath(), path.getFileName().toString()), REPLACE_EXISTING);
                    uploadedFile.add(Paths.get(spillUploadPath.getPath(), path.getFileName().toString()));
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            });

            spiller.close();
            spiller = factory.create(TYPES, bytes -> {}, memoryContext);
            spiller.restore(snapshot, serde);
            uploadedFile.stream().forEach(path -> {
                try {
                    Files.move(path, Paths.get(spillPath.getPath(), path.getFileName().toString()), REPLACE_EXISTING);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            });

            uploadedFile.clear();
            doSpill(spiller, spilledBytes, runners, spills, counter);
        }
        //assertEquals(spillerStats.getTotalSpilledBytes() - spilledBytesBefore, spilledBytes);
        // At this point, the buffers should still be accounted for in the memory context, because
        // the spiller (FileSingleStreamSpiller) doesn't release its memory reservation until it's closed.
        //assertEquals(memoryContext.getBytes(), spills.length * FileSingleStreamSpiller.BUFFER_SIZE);

        List<Iterator<Page>> actualSpills = spiller.getSpills();
        assertEquals(actualSpills.size(), spills.length);

        for (int i = 0; i < actualSpills.size(); i++) {
            List<Page> actualSpill = ImmutableList.copyOf(actualSpills.get(i));
            List<Page> expectedSpill = spills[i];

            assertEquals(actualSpill.size(), expectedSpill.size());
            for (int j = 0; j < actualSpill.size(); j++) {
                assertPageEquals(types, actualSpill.get(j), expectedSpill.get(j));
            }
        }
        spiller.close();
        assertEquals(memoryContext.getBytes(), 0);
    }

    private long doSpill(Spiller spiller, long spilledBytes, List<Runnable> runners, List<Page>[] spills, int count) throws InterruptedException, ExecutionException
    {
        runners.clear();
        for (List<Page> spill : spills) {
            if (count > 0) {
                count--;
                continue;
            }
            spilledBytes += spill.stream()
                    .mapToLong(page -> pagesSerde.serialize(page).getSizeInBytes())
                    .sum();
            Pair<ListenableFuture<?>, Runnable> spillState = spiller.spillUnCommit(spill.iterator());
            runners.add(spillState.getRight());
            spillState.getLeft().get();
        }
        return spilledBytes;
    }
}
