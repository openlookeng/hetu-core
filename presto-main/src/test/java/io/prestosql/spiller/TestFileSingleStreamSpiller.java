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
import io.hetu.core.filesystem.HdfsConfig;
import io.hetu.core.filesystem.HetuHdfsFileSystemClient;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.hetu.core.transport.execution.buffer.PageCodecMarker;
import io.hetu.core.transport.execution.buffer.PagesSerdeUtil;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.PageAssertions;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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
    FileSystemClientManager fileSystemClientManager = mock(FileSystemClientManager.class);
    private final String rootPath = spillPath.getAbsolutePath();
    private HetuHdfsFileSystemClient fs;

    public TestFileSingleStreamSpiller()
            throws IOException
    {}

    @BeforeClass
    public void prepare()
            throws IOException
    {
        fs = getLocalHdfs();
        if (fs.exists(Paths.get(rootPath))) {
            fs.delete(Paths.get(rootPath));
        }
        fs.createDirectories(Paths.get(rootPath));

        when(fileSystemClientManager.getFileSystemClient(any(Path.class))).thenReturn(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(spillPath.getCanonicalPath())));
        when(fileSystemClientManager.getFileSystemClient(any(String.class), any(Path.class))).thenReturn(fs);
    }

    private HetuHdfsFileSystemClient getLocalHdfs()
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("fs.client.type", "hdfs");
        properties.setProperty("hdfs.config.resources", "");
        properties.setProperty("hdfs.authentication.type", "NONE");
        return new HetuHdfsFileSystemClient(new HdfsConfig(properties), Paths.get(rootPath));
    }

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
        assertSpill(false, false, false, null);
        assertSpill(false, false, true, "hdfs");
    }

    @Test
    public void testSpillCompression()
            throws Exception
    {
        assertSpill(true, false, false, null);
        assertSpill(true, false, true, "hdfs");
    }

    @Test
    public void testSpillEncryption()
            throws Exception
    {
        assertSpill(false, true, false, null);
        assertSpill(false, true, true, "hdfs");
    }

    @Test
    public void testSpillEncryptionWithCompression()
            throws Exception
    {
        assertSpill(true, true, false, null);
        assertSpill(true, true, true, "hdfs");
    }

    private void assertSpill(boolean compression, boolean encryption, boolean spillToHdfs, String spillProfile)
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
                false,
                1,
                spillToHdfs,
                spillProfile,
                fileSystemClientManager);
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        SingleStreamSpiller singleStreamSpiller = spillerFactory.create(TYPES, bytes -> {}, memoryContext);
        assertTrue(singleStreamSpiller instanceof FileSingleStreamSpiller);
        FileSingleStreamSpiller spiller = (FileSingleStreamSpiller) singleStreamSpiller;

        Page page = buildPage();

        // The spillers will reserve memory in their constructors
        assertEquals(memoryContext.getBytes(), 4096);
        spiller.spill(page).get();
        spiller.spill(Iterators.forArray(page, page, page)).get();
        Path finalSpillPath = spillToHdfs ? spillerFactory.getSpillPaths().get(0) : spillPath.toPath();
        assertEquals(listFiles(finalSpillPath).stream().filter(path -> path.toString().endsWith(".bin")).count(), 1);

        // Assert the spill codec flags match the expected configuration
        try (InputStream is = newInputStream((listFiles(finalSpillPath).stream().filter(path -> path.toString().endsWith(".bin")).collect(Collectors.toList())).get(0))) {
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

        if (spillToHdfs) {
            assertEquals(3, spilledPages.size());
            for (int i = 0; i < 3; ++i) {
                PageAssertions.assertPageEquals(TYPES, page, spilledPages.get(i));
            }
        }
        else {
            assertEquals(4, spilledPages.size());
            for (int i = 0; i < 4; ++i) {
                PageAssertions.assertPageEquals(TYPES, page, spilledPages.get(i));
            }
        }

        spiller.close();
        assertEquals(listFiles(finalSpillPath).stream().filter(path -> path.toString().endsWith(".bin")).count(), 0);
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
        assertSpillBenchmark(false, false, "1GB", 1, false, false, false, null);
        assertSpillBenchmark(false, false, "1GB", 1, false, true, false, null);
        assertSpillBenchmark(false, false, "1GB", 1, false, false, true, "hdfs");
        assertSpillBenchmark(false, false, "1GB", 1, false, true, true, "hdfs");
    }

    @Test
    public void testSpillWithMultiFile()
            throws Exception
    {
        assertSpillBenchmark(false, false, "2MB", 500, false, false, false, null);
        assertSpillBenchmark(false, false, "2MB", 500, false, false, true, "hdfs");
    }

    @Test
    public void testSpillWithSingleFileWithKryo()
            throws Exception
    {
        assertSpillBenchmark(false, false, "1GB", 1, true, true, false, null);
        assertSpillBenchmark(false, false, "1GB", 1, true, true, false, null);
        assertSpillBenchmark(false, false, "1GB", 1, true, true, true, "hdfs");
        assertSpillBenchmark(false, false, "1GB", 1, true, true, true, "hdfs");
    }

    @Test
    public void testSpillWithMultiFileWithKryo()
            throws Exception
    {
        assertSpillBenchmark(false, false, "2MB", 2, true, true, false, null);
        assertSpillBenchmark(false, false, "2MB", 2, true, true, false, null);
        assertSpillBenchmark(false, false, "2MB", 2, true, true, true, "hdfs");
        assertSpillBenchmark(false, false, "2MB", 2, true, true, true, "hdfs");
    }

    @Test
    public void testSpillWithSingleSpillerConsolidatedWithoutWorkProcessor()
            throws Exception
    {
        assertSpillBenchmark(false, false, "1GB", 1, false, false, false, null);
        assertSpillBenchmark(false, false, "1GB", 1, true, true, false, null);
        assertSpillBenchmark(false, false, "2MB", 512, false, false, false, null);
        assertSpillBenchmark(false, false, "2MB", 512, true, true, false, null);
        assertSpillBenchmark(false, false, "1GB", 1, false, false, true, "hdfs");
        assertSpillBenchmark(false, false, "1GB", 1, true, true, true, "hdfs");
        assertSpillBenchmark(false, false, "2MB", 512, false, false, true, "hdfs");
        assertSpillBenchmark(false, false, "2MB", 512, true, true, true, "hdfs");
    }

    private void assertSpillBenchmark(boolean compression, boolean encryption, String pageSize, int fileCount, boolean useKryo, boolean useDirectSerde, boolean spillToHdfs, String spillProfile)
            throws Exception
    {
        List<FileSingleStreamSpiller> spillers = new ArrayList<>();
        when(fileSystemClientManager.getFileSystemClient(any(Path.class))).thenReturn(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(spillPath.getCanonicalPath())));
        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
                executorBenchmark, // executor won't be closed, because we don't call destroy() on the spiller factory
                (useKryo) ? createTestMetadataManager().getFunctionAndTypeManager().getBlockKryoEncodingSerde() : createTestMetadataManager().getFunctionAndTypeManager().getBlockEncodingSerde(),
                new SpillerStats(),
                ImmutableList.of(spillPath.toPath()),
                1.0,
                compression,
                encryption,
                useDirectSerde,
                1,
                useKryo,
                spillToHdfs,
                spillProfile,
                fileSystemClientManager);
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        long startTime = System.currentTimeMillis();
        Stopwatch spillTimer = Stopwatch.createStarted();
        long numberOfPages = pageSize.equals("1GB") ? 262144 : 512;
        Page page = buildPageBenchmark();
        Path finalSpillPath = spillToHdfs ? spillerFactory.getSpillPaths().get(0) : spillPath.toPath();
        for (int j = 1; j <= fileCount; j++) {
            SingleStreamSpiller singleStreamSpiller = spillerFactory.create(TYPES, bytes -> {}, memoryContext);
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
            assertEquals(listFiles(finalSpillPath).stream().filter(path -> path.toString().endsWith(".bin")).count(), j);
        }
        spillTimer.stop();
        System.out.println("Time To Spill: " + spillTimer.elapsed(TimeUnit.MILLISECONDS) + " ms Traditional Timer: " + (System.currentTimeMillis() - startTime));
        log.debug("TimeTakenToSpill = " + (System.currentTimeMillis() - startTime));

        startTime = System.currentTimeMillis();
        spillTimer.reset();
        spillTimer.start();
        for (int i = 0; i < fileCount; i++) {
            Iterator<Page> spilledPagesIterator = spillers.get(i).getSpilledPages();
            assertEquals(memoryContext.getBytes(), FileSingleStreamSpiller.BUFFER_SIZE);
            spilledPagesIterator.forEachRemaining(page1 -> page1.getLoadedPage());
        }
        spillTimer.stop();
        log.debug("TimeTakenReadingFromSpill = " + (System.currentTimeMillis() - startTime));
        System.out.println("Time To Unspill: " + spillTimer.elapsed(TimeUnit.MILLISECONDS) + " ms, Traditional Timer: " + (System.currentTimeMillis() - startTime));
        spillers.stream().forEach(spiller -> spiller.close());
        assertEquals(listFiles(finalSpillPath).stream().filter(path -> path.toString().endsWith(".bin")).count(), 0);
        assertEquals(memoryContext.getBytes(), 0);
    }

    private Page buildPageBenchmark()
    {
        BlockBuilder col1 = BIGINT.createBlockBuilder(null, 1);
        BlockBuilder col2 = VARBINARY.createBlockBuilder(null, 1);

        while (col1.getSizeInBytes() < 4 * 1024) {
            col1.writeLong(42).writeLong(45).writeLong(45).writeLong(45).writeLong(45).writeLong(45);
        }
        col1.closeEntry();

        while (col2.getSizeInBytes() < 4 * 1024) {
            col2.writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(doubleToLongBits(43.0)).writeLong(1);
        }
        col2.closeEntry();

        return new Page(col1.build(), col2.build());
    }

    @Test
    public void testSpillWithSingleSpillerConsolidated()
            throws Exception
    {
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, false, 1, false, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, true, 1, true, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "2MB", 512, false, 1, false, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "2MB", 512, true, 1, true, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, false, 1, false, true, "hdfs");
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, true, 1, true, true, "hdfs");
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "2MB", 512, false, 1, false, true, "hdfs");
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "2MB", 512, true, 1, true, true, "hdfs");
    }

    @Test
    public void testSpillWithSingleSpillerConsolidatedWithCompression()
            throws Exception
    {
        assertSpillBenchmarkReadingUsingWorkProcessor(true, false, "1GB", 1, false, 1, false, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(true, false, "1GB", 1, true, 1, false, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(true, false, "1GB", 1, false, 1, false, true, "hdfs");
        assertSpillBenchmarkReadingUsingWorkProcessor(true, false, "1GB", 1, true, 1, false, true, "hdfs");
    }

    @Test
    public void testSpillWithSingleSpillerConsolidatedWithCompressionMultiFile()
            throws Exception
    {
        assertSpillBenchmarkReadingUsingWorkProcessor(true, false, "2MB", 512, false, 1, false, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(true, false, "2MB", 512, true, 1, false, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(true, false, "2MB", 512, false, 1, false, true, "hdfs");
        assertSpillBenchmarkReadingUsingWorkProcessor(true, false, "2MB", 512, true, 1, false, true, "hdfs");
    }

    @Test
    public void testSpillWithSingleSpillerConsolidatedWithoutCompression()
            throws Exception
    {
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, false, 25, false, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, true, 25, false, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, false, 25, false, true, "hdfs");
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, true, 25, false, true, "hdfs");
    }

    @Test
    public void testSpillWithSingleSpillerConsolidatedWithoutCompressionMultiFile()
            throws Exception
    {
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "2MB", 512, false, 25, false, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "2MB", 512, true, 25, false, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "2MB", 512, false, 25, false, true, "hdfs");
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "2MB", 512, true, 25, false, true, "hdfs");
    }

    @Test
    public void testSpillWithSingleSpillerConsolidatedWithEncryption()
            throws Exception
    {
        assertSpillBenchmarkReadingUsingWorkProcessor(true, true, "1GB", 1, false, 25, false, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(true, true, "1GB", 1, true, 25, false, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(true, true, "1GB", 1, false, 25, false, true, "hdfs");
        assertSpillBenchmarkReadingUsingWorkProcessor(true, true, "1GB", 1, true, 25, false, true, "hdfs");
    }

    @Test
    public void testSpillWithSingleSpillerConsolidatedDirectWriteCompareWithWorkProcessorWithKryo()
            throws Exception
    {
        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, true, 1, true, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(true, false, "1GB", 1, true, 1, true, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(false, true, "1GB", 1, true, 1, true, false, null);
        assertSpillBenchmarkReadingUsingWorkProcessor(true, true, "1GB", 1, true, 1, true, false, null);

        assertSpillBenchmarkReadingUsingWorkProcessor(false, false, "1GB", 1, true, 1, true, true, "hdfs");
        assertSpillBenchmarkReadingUsingWorkProcessor(true, false, "1GB", 1, true, 1, true, true, "hdfs");
        assertSpillBenchmarkReadingUsingWorkProcessor(false, true, "1GB", 1, true, 1, true, true, "hdfs");
        assertSpillBenchmarkReadingUsingWorkProcessor(true, true, "1GB", 1, true, 1, true, true, "hdfs");
    }

    private void assertSpillBenchmarkReadingUsingWorkProcessor(boolean compression, boolean encryption, String pageSize, int fileCount, boolean useDirectSerde, int spillPrefetchReadPages, boolean useKryo, boolean spillToHdfs, String spillProfile)
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
                useDirectSerde,
                spillPrefetchReadPages,
                useKryo,
                spillToHdfs,
                spillProfile,
                fileSystemClientManager);

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        long startTime = System.currentTimeMillis();
        Stopwatch spillTimer = Stopwatch.createStarted();
        long numberOfPages = pageSize.equals("1GB") ? 262144 : 512;
        Page page = buildPageBenchmark();
        Path finalSpillPath = spillToHdfs ? spillerFactory.getSpillPaths().get(0) : spillPath.toPath();
        for (int j = 1; j <= fileCount; j++) {
            SingleStreamSpiller singleStreamSpiller = spillerFactory.create(TYPES, bytes -> {}, memoryContext);
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
            assertEquals(listFiles(finalSpillPath).stream().filter(path -> path.toString().endsWith(".bin")).count(), j);
        }
        spillTimer.stop();
        long timeToSpill = spillTimer.elapsed(TimeUnit.MILLISECONDS);
        log.debug("TimeTakenToSpill = " + (System.currentTimeMillis() - startTime));
        long spilledDiskSize = getDirectorySize(finalSpillPath);

        startTime = System.currentTimeMillis();
        spillTimer.reset();
        spillTimer.start();
        AtomicInteger pageCount = new AtomicInteger();
        for (int i = 0; i < fileCount; i++) {
            Iterator<Page> spilledPagesIterator = spillers.get(i).getSpilledPages();
            WorkProcessor<Page> workProcessor = WorkProcessor.fromIterator(spilledPagesIterator);

            workProcessor.iterator().forEachRemaining(page1 -> {
                page1.getLoadedPage();
                pageCount.getAndIncrement();
            });
            assertEquals(memoryContext.getBytes(), FileSingleStreamSpiller.BUFFER_SIZE);
        }
        spillTimer.stop();
        long timeToUnspill = spillTimer.elapsed(TimeUnit.MILLISECONDS);
        log.debug("TimeTakenReadingFromSpill = " + (System.currentTimeMillis() - startTime));
        log.info("[isEncrypted: %6s, isCompressed: %6s, isDirect: %6s, SpillSize: %4s, SpillFiles: %4d, useKryo: %6s] --> Spill: %8d, Unspill: %8d, DiskUsed: %5s",
                encryption, compression, useDirectSerde, pageSize, fileCount, useKryo,
                timeToSpill, timeToUnspill, humanReadableByteCountBin(spilledDiskSize));
        spillers.stream().forEach(spiller -> spiller.close());
        assertEquals(listFiles(finalSpillPath).stream().filter(path -> path.toString().endsWith(".bin")).count(), 0);
        assertEquals(memoryContext.getBytes(), 0);
    }

    private static long getDirectorySize(Path path)
    {
        long size = 0;
        try (Stream<Path> walk = Files.walk(path)) {
            size = walk
                    //.peek(System.out::println) // debug
                    .filter(Files::isRegularFile)
                    .mapToLong(p -> {
                        // ugly, can pretty it with an extract method
                        try {
                            return Files.size(p);
                        }
                        catch (IOException e) {
                            System.out.printf("Failed to get size of %s%n%s", p, e);
                            return 0L;
                        }
                    })
                    .sum();
        }
        catch (IOException e) {
            System.out.printf("IO errors %s", e);
        }
        return size;
    }

    public static String humanReadableByteCountBin(long bytes)
    {
        long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        if (absB < 1024) {
            return bytes + " B";
        }
        long value = absB;
        CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
            value >>= 10;
            ci.next();
        }
        value *= Long.signum(bytes);
        return String.format("%.1f %ciB", value / 1024.0, ci.current());
    }
}
