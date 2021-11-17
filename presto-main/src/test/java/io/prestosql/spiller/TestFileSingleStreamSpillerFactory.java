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
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.type.Type;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.MoreFiles.listFiles;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_PREFIX;
import static io.prestosql.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_SUFFIX;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestFileSingleStreamSpillerFactory
{
    private final BlockEncodingSerde blockEncodingSerde = createTestMetadataManager().getFunctionAndTypeManager().getBlockEncodingSerde();
    private Closer closer;
    private ListeningExecutorService executor;
    private File spillPath1;
    private File spillPath2;

    @BeforeMethod
    public void setUp() throws IOException
    {
        closer = Closer.create();
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        closer.register(() -> executor.shutdownNow());
        spillPath1 = createTempDirectory(getClass().getName()).toFile();
        closer.register(() -> deleteRecursively(spillPath1.toPath(), ALLOW_INSECURE));
        spillPath2 = createTempDirectory(getClass().getName()).toFile();
        closer.register(() -> deleteRecursively(spillPath2.toPath(), ALLOW_INSECURE));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closer.close();
    }

    @Test
    public void testDistributesSpillOverPaths()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
                executor, // executor won't be closed, because we don't call destroy() on the spiller factory
                blockEncodingSerde,
                new SpillerStats(),
                spillPaths,
                1.0,
                false,
                false,
                false);

        assertEquals(listFiles(spillPath1.toPath()).size(), 0);
        assertEquals(listFiles(spillPath2.toPath()).size(), 0);

        Page page = buildPage();
        List<SingleStreamSpiller> spillers = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            SingleStreamSpiller singleStreamSpiller = spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
            getUnchecked(singleStreamSpiller.spill(page));
            spillers.add(singleStreamSpiller);
        }
        assertEquals(listFiles(spillPath1.toPath()).size(), 5);
        assertEquals(listFiles(spillPath2.toPath()).size(), 5);

        spillers.forEach(SingleStreamSpiller::close);
        assertEquals(listFiles(spillPath1.toPath()).size(), 0);
        assertEquals(listFiles(spillPath2.toPath()).size(), 0);
    }

    private Page buildPage()
    {
        BlockBuilder col1 = BIGINT.createBlockBuilder(null, 1);
        col1.writeLong(42).closeEntry();
        return new Page(col1.build());
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "No free space available for spill")
    public void throwsIfNoDiskSpace()
    {
        List<Type> types = ImmutableList.of(BIGINT);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
                executor, // executor won't be closed, because we don't call destroy() on the spiller factory
                blockEncodingSerde,
                new SpillerStats(),
                spillPaths,
                0.0,
                false,
                false,
                false);

        spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "No spill paths configured")
    public void throwIfNoSpillPaths()
    {
        List<Path> spillPaths = emptyList();
        List<Type> types = ImmutableList.of(BIGINT);
        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
                executor, // executor won't be closed, because we don't call destroy() on the spiller factory
                blockEncodingSerde,
                new SpillerStats(),
                spillPaths,
                1.0,
                false,
                false,
                false);
        spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
    }

    @Test
    public void testCleanupOldSpillFiles()
            throws Exception
    {
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        spillPath1.mkdirs();
        spillPath2.mkdirs();

        java.nio.file.Files.createTempFile(spillPath1.toPath(), SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX);
        java.nio.file.Files.createTempFile(spillPath1.toPath(), SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX);
        java.nio.file.Files.createTempFile(spillPath1.toPath(), SPILL_FILE_PREFIX, "blah");
        java.nio.file.Files.createTempFile(spillPath2.toPath(), SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX);
        java.nio.file.Files.createTempFile(spillPath2.toPath(), "blah", SPILL_FILE_SUFFIX);
        java.nio.file.Files.createTempFile(spillPath2.toPath(), "blah", "blah");

        assertEquals(listFiles(spillPath1.toPath()).size(), 3);
        assertEquals(listFiles(spillPath2.toPath()).size(), 3);

        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
                executor, // executor won't be closed, because we don't call destroy() on the spiller factory
                blockEncodingSerde,
                new SpillerStats(),
                spillPaths,
                1.0,
                false,
                false,
                false);
        spillerFactory.cleanupOldSpillFiles();

        assertEquals(listFiles(spillPath1.toPath()).size(), 1);
        assertEquals(listFiles(spillPath2.toPath()).size(), 2);
    }
}
