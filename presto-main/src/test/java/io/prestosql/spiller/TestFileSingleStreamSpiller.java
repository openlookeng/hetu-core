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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.log.Logger;
import io.airlift.slice.InputStreamSliceInput;
import io.hetu.core.transport.execution.buffer.PageCodecMarker;
import io.hetu.core.transport.execution.buffer.PagesSerdeUtil;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.PageAssertions;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.MoreFiles.listFiles;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Double.doubleToLongBits;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.newInputStream;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestFileSingleStreamSpiller
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, DOUBLE, VARBINARY);
    private static final List<Type> TYPES_BENCHMARK = ImmutableList.of(BIGINT);

    private static final Logger log = Logger.get(TestFileSingleStreamSpiller.class);

    private final ListeningExecutorService executor = listeningDecorator(newCachedThreadPool());
    private final ListeningExecutorService executorBenchmark = listeningDecorator(newFixedThreadPool(1, daemonThreadsNamed("binary-spiller-%s")));
    private final File spillPath = createTempDirectory(getClass().getName()).toFile();

    public TestFileSingleStreamSpiller()
            throws IOException
    {}

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        executor.shutdown();
        executorBenchmark.shutdown();
        deleteRecursively(spillPath.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testSpill()
            throws Exception
    {
        assertSpill(false, false);
    }

    @Test
    public void testSpillCompression()
            throws Exception
    {
        assertSpill(true, false);
    }

    @Test
    public void testSpillEncryption()
            throws Exception
    {
        assertSpill(false, true);
    }

    @Test
    public void testSpillEncryptionWithCompression()
            throws Exception
    {
        assertSpill(true, true);
    }

    private void assertSpill(boolean compression, boolean encryption)
            throws Exception
    {
        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
                executor, // executor won't be closed, because we don't call destroy() on the spiller factory
                createTestMetadataManager().getFunctionAndTypeManager().getBlockEncodingSerde(),
                new SpillerStats(),
                ImmutableList.of(spillPath.toPath()),
                1.0,
                compression,
                encryption,
                false);
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        SingleStreamSpiller singleStreamSpiller = spillerFactory.create(TYPES, bytes -> {}, memoryContext);
        assertTrue(singleStreamSpiller instanceof FileSingleStreamSpiller);
        FileSingleStreamSpiller spiller = (FileSingleStreamSpiller) singleStreamSpiller;

        Page page = buildPage();

        // The spillers will reserve memory in their constructors
        assertEquals(memoryContext.getBytes(), 4096);
        spiller.spill(page).get();
        spiller.spill(Iterators.forArray(page, page, page)).get();
        assertEquals(listFiles(spillPath.toPath()).size(), 1);

        // Assert the spill codec flags match the expected configuration
        try (InputStream is = newInputStream(listFiles(spillPath.toPath()).get(0))) {
            Iterator<SerializedPage> serializedPages = PagesSerdeUtil.readSerializedPages(new InputStreamSliceInput(is));
            assertTrue(serializedPages.hasNext(), "at least one page should be successfully read back");
            byte markers = serializedPages.next().getPageCodecMarkers();
            assertEquals(PageCodecMarker.COMPRESSED.isSet(markers), compression);
            assertEquals(PageCodecMarker.ENCRYPTED.isSet(markers), encryption);
        }

        // The spillers release their memory reservations when they are closed, therefore at this point
        // they will have non-zero memory reservation.

        Iterator<Page> spilledPagesIterator = spiller.getSpilledPages();
        assertEquals(memoryContext.getBytes(), FileSingleStreamSpiller.BUFFER_SIZE);
        ImmutableList<Page> spilledPages = ImmutableList.copyOf(spilledPagesIterator);
        // The spillers release their memory reservations when they are closed, therefore at this point
        // they will have non-zero memory reservation.

        assertEquals(4, spilledPages.size());
        for (int i = 0; i < 4; ++i) {
            PageAssertions.assertPageEquals(TYPES, page, spilledPages.get(i));
        }

        spiller.close();
        assertEquals(listFiles(spillPath.toPath()).size(), 0);
        assertEquals(memoryContext.getBytes(), 0);
    }

    private Page buildPage()
    {
        BlockBuilder col1 = BIGINT.createBlockBuilder(null, 1);
        BlockBuilder col2 = DOUBLE.createBlockBuilder(null, 1);
        BlockBuilder col3 = VARBINARY.createBlockBuilder(null, 1);

        col1.writeLong(42).writeLong(45).writeLong(45).writeLong(45).writeLong(45).writeLong(45).closeEntry();
        col2.writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).closeEntry();
        col3.writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(1).closeEntry();

        return new Page(col1.build(), col2.build(), col3.build());
    }

    @Test
    public void testSpillWithSingleFile()
            throws Exception
    {
        assertSpillBenchmark(false, false, "1GB", 1, false);
    }

    @Test
    public void testSpillWithMultiFile()
            throws Exception
    {
        assertSpillBenchmark(false, false, "2MB", 500, false);
    }

    @Test
    public void testSpillWithSingleFileWithKryo()
            throws Exception
    {
        assertSpillBenchmark(false, false, "1GB", 1, true);
    }

    @Test
    public void testSpillWithMultiFileWithKryo()
            throws Exception
    {
        assertSpillBenchmark(false, false, "2MB", 2, true);
    }

    @Test
    public void testSpillWithSingleSpillerConsolidatedWithoutWorkProcessor()
            throws Exception
    {
        assertSpillBenchmark(false, false, "1GB", 1, false);
        assertSpillBenchmark(false, false, "1GB", 1, true);
        assertSpillBenchmark(false, false, "2MB", 512, false);
        assertSpillBenchmark(false, false, "2MB", 512, true);
    }

    private void assertSpillBenchmark(boolean compression, boolean encryption, String pageSize, int fileCount, boolean useKryo)
            throws Exception
    {
        List<FileSingleStreamSpiller> spillers = new ArrayList<>();
        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
                executorBenchmark, // executor won't be closed, because we don't call destroy() on the spiller factory
                (useKryo) ? createTestMetadataManager().getFunctionAndTypeManager().getBlockKryoEncodingSerde() : createTestMetadataManager().getFunctionAndTypeManager().getBlockEncodingSerde(),
                new SpillerStats(),
                ImmutableList.of(spillPath.toPath()),
                1.0,
                compression,
                encryption,
                useKryo);
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        long startTime = System.currentTimeMillis();
        Stopwatch spillTimer = Stopwatch.createStarted();
        long numberOfPages = pageSize.equals("1GB") ? 262144 : 512;
        Page page = buildPageBenchmark();
        for (int j = 1; j <= fileCount; j++) {
            SingleStreamSpiller singleStreamSpiller = spillerFactory.create(TYPES, bytes -> {}, memoryContext, useKryo);
            assertTrue(singleStreamSpiller instanceof FileSingleStreamSpiller);
            FileSingleStreamSpiller spiller = (FileSingleStreamSpiller) singleStreamSpiller;
            spillers.add(spiller);

            // The spillers will reserve memory in their constructors
            assertEquals(memoryContext.getBytes(), 4096);
            Iterator<Page> pageIterator = new Iterator<Page>()
            {
                private final long pageCount = numberOfPages;
                private long counter = 1;

                @Override
                public boolean hasNext()
                {
                    return counter <= pageCount;
                }

                @Override
                public Page next()
                {
                    if (counter > pageCount) {
                        throw new RuntimeException();
                    }
                    counter++;
                    return page;
                }
            };

            spiller.spill(pageIterator).get();
            assertEquals(listFiles(spillPath.toPath()).size(), j);
        }
        spillTimer.stop();
        System.out.println("Time To Spill: " + spillTimer.elapsed(TimeUnit.MILLISECONDS) + " ms Traditional Timer: " + (System.currentTimeMillis() - startTime));
        log.debug("TimeTakenToSpill = " + (System.currentTimeMillis() - startTime));

        startTime = System.currentTimeMillis();
        spillTimer.reset();
        spillTimer.start();
        for (int i = 0; i < fileCount; i++) {
            // Assert the spill codec flags match the expected configuration
//            try (InputStream is = newInputStream(listFiles(spillPath.toPath()).get(0))) {
//                Iterator<SerializedPage> serializedPages = PagesSerdeUtil.readSerializedPages(new InputStreamSliceInput(is));
//                assertTrue(serializedPages.hasNext(), "at least one page should be successfully read back");
//                byte markers = serializedPages.next().getPageCodecMarkers();
//                assertEquals(PageCodecMarker.COMPRESSED.isSet(markers), compression);
//                assertEquals(PageCodecMarker.ENCRYPTED.isSet(markers), encryption);
//            }

            // The spillers release their memory reservations when they are closed, therefore at this point
            // they will have non-zero memory reservation.

            Iterator<Page> spilledPagesIterator = spillers.get(i).getSpilledPages();
            assertEquals(memoryContext.getBytes(), FileSingleStreamSpiller.BUFFER_SIZE);
            spilledPagesIterator.forEachRemaining(page1 -> page1.getLoadedPage());
        }
        spillTimer.stop();
        log.debug("TimeTakenReadingFromSpill = " + (System.currentTimeMillis() - startTime));
        System.out.println("Time To Unspill: " + spillTimer.elapsed(TimeUnit.MILLISECONDS) + " ms, Traditional Timer: " + (System.currentTimeMillis() - startTime));
        spillers.stream().forEach(spiller -> spiller.close());
        assertEquals(listFiles(spillPath.toPath()).size(), 0);
        assertEquals(memoryContext.getBytes(), 0);
    }

    private Page buildPageBenchmark()
    {
        BlockBuilder col1 = BIGINT.createBlockBuilder(null, 1);
        while (col1.getSizeInBytes() < 4 * 1024) {
            col1.writeLong(42).writeLong(45).writeLong(45).writeLong(45).writeLong(45).writeLong(45);
        }
        col1.closeEntry();

        return new Page(col1.build());
    }

    @Test
    public void testSpillWithSingleSpillerConsolidated()
            throws Exception
    {
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, false);
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, true);
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "2MB", 512, false);
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "2MB", 512, true);
    }

    @Test
    public void testSpillWithSingleFileAndWorkProcessor()
            throws Exception
    {
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, false);
    }

    @Test
    public void testSpillWithMultiFileAndWorkProcessor()
            throws Exception
    {
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "2MB", 500, false);
    }

    @Test
    public void testSpillWithSingleFileAndWorkProcessorWithKryo()
            throws Exception
    {
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, true);
    }

    @Test
    public void testSpillWithMultiFileAndWorkProcessorWithKryo()
            throws Exception
    {
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "2MB", 500, true);
    }

    private void assertSpillBenchmarkReadingUsingWorkProcessor(boolean compression, boolean encryption, String pageSize, int fileCount, boolean useKryo)
            throws Exception
    {
        List<FileSingleStreamSpiller> spillers = new ArrayList<>();
        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
                executorBenchmark, // executor won't be closed, because we don't call destroy() on the spiller factory
                (useKryo) ? createTestMetadataManager().getFunctionAndTypeManager().getBlockKryoEncodingSerde() : createTestMetadataManager().getFunctionAndTypeManager().getBlockEncodingSerde(),
                new SpillerStats(),
                ImmutableList.of(spillPath.toPath()),
                1.0,
                compression,
                encryption,
                useKryo);

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        long startTime = System.currentTimeMillis();
        Stopwatch spillTimer = Stopwatch.createStarted();
        long numberOfPages = pageSize.equals("1GB") ? 262144 : 512;
        Page page = buildPageBenchmark();
        for (int j = 1; j <= fileCount; j++) {
            SingleStreamSpiller singleStreamSpiller = spillerFactory.create(TYPES, bytes -> {}, memoryContext, useKryo);
            assertTrue(singleStreamSpiller instanceof FileSingleStreamSpiller);
            FileSingleStreamSpiller spiller = (FileSingleStreamSpiller) singleStreamSpiller;
            spillers.add(spiller);

            // The spillers will reserve memory in their constructors
            assertEquals(memoryContext.getBytes(), 4096);
            Iterator<Page> pageIterator = new Iterator<Page>()
            {
                private final long pageCount = numberOfPages;
                private long counter = 1;

                @Override
                public boolean hasNext()
                {
                    return counter <= pageCount;
                }

                @Override
                public Page next()
                {
                    if (counter > pageCount) {
                        throw new RuntimeException();
                    }
                    counter++;
                    return page;
                }
            };

            spiller.spill(pageIterator).get();
            assertEquals(listFiles(spillPath.toPath()).size(), j);
        }
        spillTimer.stop();
        System.out.println("[isKryo: " + useKryo + "] Time to spill: " + spillTimer.elapsed(TimeUnit.MILLISECONDS) + " traditional counter: " + (System.currentTimeMillis() - startTime));
        log.debug("TimeTakenToSpill = " + (System.currentTimeMillis() - startTime));

        startTime = System.currentTimeMillis();
        spillTimer.reset();
        spillTimer.start();
        AtomicInteger pageCount = new AtomicInteger();
        for (int i = 0; i < fileCount; i++) {
            // Assert the spill codec flags match the expected configuration
//            try (InputStream is = newInputStream(listFiles(spillPath.toPath()).get(0))) {
//                Iterator<SerializedPage> serializedPages = PagesSerdeUtil.readSerializedPages(new InputStreamSliceInput(is));
//                assertTrue(serializedPages.hasNext(), "at least one page should be successfully read back");
//                byte markers = serializedPages.next().getPageCodecMarkers();
//                assertEquals(PageCodecMarker.COMPRESSED.isSet(markers), compression);
//                assertEquals(PageCodecMarker.ENCRYPTED.isSet(markers), encryption);
//            }

            // The spillers release their memory reservations when they are closed, therefore at this point
            // they will have non-zero memory reservation.

            Iterator<Page> spilledPagesIterator = spillers.get(i).getSpilledPages();
            WorkProcessor<Page> workProcessor = WorkProcessor.fromIterator(spilledPagesIterator);

            workProcessor.iterator().forEachRemaining(page1 -> {
                page1.getLoadedPage();
                pageCount.getAndIncrement();
            });
            assertEquals(memoryContext.getBytes(), FileSingleStreamSpiller.BUFFER_SIZE);
        }
        spillTimer.stop();
        System.out.println("[isKryo: " + useKryo + "] Time to Unspill: " + spillTimer.elapsed(TimeUnit.MILLISECONDS) + " traditional counter: " + (System.currentTimeMillis() - startTime) + ", pageCount: " + pageCount.get());
        log.debug("TimeTakenReadingFromSpill = " + (System.currentTimeMillis() - startTime));
        spillers.stream().forEach(spiller -> spiller.close());
        assertEquals(listFiles(spillPath.toPath()).size(), 0);
        assertEquals(memoryContext.getBytes(), 0);
    }
}
