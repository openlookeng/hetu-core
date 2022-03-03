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

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.compress.snappy.SnappyFramedInputStream;
import io.airlift.compress.snappy.SnappyFramedOutputStream;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeUtil;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.SpillContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.spiller.SpillCipher;

import javax.annotation.concurrent.NotThreadSafe;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static io.hetu.core.transport.execution.buffer.PagesSerdeUtil.writeSerializedPage;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_PREFIX;
import static io.prestosql.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_SUFFIX;
import static io.prestosql.spiller.FileSingleStreamSpillerFactory.getFileSystem;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Objects.requireNonNull;

@NotThreadSafe

@RestorableConfig(uncapturedFields = {"closer", "serde",
        "spillerStats", "localSpillContext", "memoryContext", "executor", "spillInProgress", "cipherIV", "spillCipher", "fileSystemClientManager", "fileSystemClient", "spillPath"})
public class FileSingleStreamSpiller
        implements SingleStreamSpiller
{
    @VisibleForTesting
    static final int BUFFER_SIZE = 4 * 1024;

    private FileHolder targetFile;
    private final Closer closer = Closer.create();
    private final PagesSerde serde;
    private final SpillerStats spillerStats;
    private final SpillContext localSpillContext;
    private final LocalMemoryContext memoryContext;

    private final ListeningExecutorService executor;

    private boolean writable = true;
    private long spilledPagesInMemorySize;
    private ListenableFuture<?> spillInProgress = Futures.immediateFuture(null);
    private boolean useDirectSerde;
    private boolean useKryo;
    private boolean compressionEnabled;
    private Path spillPath;
    private String spillProfile;
    private boolean spillToHdfs;
    private Optional<SpillCipher> spillCipher;
    private byte[] cipherIV;
    private FileSystemClientManager fileSystemClientManager;
    private HetuFileSystemClient fileSystemClient;

    // Snapshot: capture page sizes that are used to update localSpillContext and spillerStats during resume
    private List<Long> pageSizeList = new LinkedList<>();
    private final int spillPrefetchReadPages;

    public FileSingleStreamSpiller(
            PagesSerde serde,
            ListeningExecutorService executor,
            Path spillPath,
            SpillerStats spillerStats,
            SpillContext spillContext,
            LocalMemoryContext memoryContext,
            Optional<SpillCipher> spillCipher,
            boolean compressionEnabled,
            boolean useDirectSerde,
            int spillPrefetchReadPages,
            boolean useKryo,
            boolean spillToHdfs,
            String spillProfile,
            FileSystemClientManager fileSystemClientManager)
    {
        this.serde = requireNonNull(serde, "serde is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats is null");
        this.spillPath = spillPath;
        this.spillProfile = spillProfile;
        this.fileSystemClientManager = requireNonNull(fileSystemClientManager, "fileSystemClient is null");
        this.spillToHdfs = spillToHdfs;
        this.localSpillContext = spillContext.newLocalSpillContext();
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        if (requireNonNull(spillCipher, "spillCipher is null").isPresent()) {
            closer.register(spillCipher.get()::close);
        }
        // HACK!
        // The writePages() method is called in a separate thread pool and it's possible that
        // these spiller thread can run concurrently with the close() method.
        // Due to this race when the spiller thread is running, the driver thread:
        // 1. Can zero out the memory reservation even though the spiller thread physically holds onto that memory.
        // 2. Can close/delete the temp file(s) used for spilling, which doesn't have any visible side effects, but still not desirable.
        // To hack around the first issue we reserve the memory in the constructor and we release it in the close() method.
        // This means we start accounting for the memory before the spiller thread allocates it, and we release the memory reservation
        // before/after the spiller thread allocates that memory -- -- whether before or after depends on whether writePages() is in the
        // middle of execution when close() is called (note that this applies to both readPages() and writePages() methods).
        this.memoryContext.setBytes(BUFFER_SIZE);
        try {
            this.fileSystemClient = getFileSystem(spillPath, spillToHdfs, spillProfile, fileSystemClientManager);
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get filesystem", e);
        }
        try {
            createSpillFiles();
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create spill file", e);
        }
        this.spillPrefetchReadPages = spillPrefetchReadPages;
        this.useDirectSerde = useDirectSerde;
        this.useKryo = useKryo;
        this.compressionEnabled = compressionEnabled;
        this.spillCipher = spillCipher;
    }

    @Override
    public ListenableFuture<?> spill(Iterator<Page> pageIterator)
    {
        requireNonNull(pageIterator, "pageIterator is null");
        checkNoSpillInProgress();
        if (useDirectSerde) {
            spillInProgress = executor.submit(() -> writePagesDirect(pageIterator));
        }
        else {
            spillInProgress = executor.submit(() -> writePages(pageIterator));
        }
        return spillInProgress;
    }

    @Override
    public long getSpilledPagesInMemorySize()
    {
        return spilledPagesInMemorySize;
    }

    @Override
    public Iterator<Page> getSpilledPages()
    {
        checkNoSpillInProgress();
        return readPages();
    }

    @Override
    public ListenableFuture<List<Page>> getAllSpilledPages()
    {
        return executor.submit(() -> ImmutableList.copyOf(getSpilledPages()));
    }

    private void writePages(Iterator<Page> pageIterator)
    {
        checkState(writable, "Spilling no longer allowed. The spiller has been made non-writable on first read for subsequent reads to be consistent");
        try (SliceOutput output = new OutputStreamSliceOutput(getOutputStreamBasedOnSpillLocation(), BUFFER_SIZE)) {
            Stopwatch timer = Stopwatch.createStarted();
            while (pageIterator.hasNext()) {
                Page page = pageIterator.next();
                spilledPagesInMemorySize += page.getSizeInBytes();
                SerializedPage serializedPage = serde.serialize(page);
                long pageSize = serializedPage.getSizeInBytes();
                localSpillContext.updateBytes(pageSize);
                spillerStats.addToTotalSpilledBytes(pageSize);
                pageSizeList.add(pageSize);
                writeSerializedPage(output, serializedPage);
            }
            timer.stop();
            localSpillContext.updateWriteTime(timer.elapsed(TimeUnit.MILLISECONDS));
        }
        catch (UncheckedIOException | IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to spill pages", e);
        }
    }

    private OutputStream getStreamForWriting(OutputStream outputStream, int bufferSize) throws IOException
    {
        OutputStream tmpOutputStream = outputStream;
        if (spillCipher.isPresent()) {
            Cipher cipher = spillCipher.get().getEncryptionCipher();
            byte[] tmpCipherIV = cipher.getIV();
            this.cipherIV = new byte[tmpCipherIV.length];
            System.arraycopy(tmpCipherIV, 0, this.cipherIV, 0, tmpCipherIV.length);
            tmpOutputStream = new CipherOutputStream(tmpOutputStream, cipher);
        }

        if (compressionEnabled) {
            tmpOutputStream = new SnappyFramedOutputStream(tmpOutputStream);
        }

        if (useKryo) {
            return new Output(tmpOutputStream, bufferSize);
        }
        return new OutputStreamSliceOutput(tmpOutputStream, bufferSize);
    }

    private void writePagesDirect(Iterator<Page> pageIterator)
    {
        checkState(writable, "Spilling no longer allowed. The spiller has been made non-writable on first read for subsequent reads to be consistent");
        try (OutputStream output = getStreamForWriting(getOutputStreamBasedOnSpillLocation(), BUFFER_SIZE)) {
            Stopwatch timer = Stopwatch.createStarted();
            while (pageIterator.hasNext()) {
                Page page = pageIterator.next();
                long pageSize = page.getSizeInBytes();
                spilledPagesInMemorySize += pageSize;
                localSpillContext.updateBytes(pageSize);
                spillerStats.addToTotalSpilledBytes(pageSize);
                pageSizeList.add(pageSize);
                serde.serialize(output, page);
            }
            timer.stop();
            localSpillContext.updateWriteTime(timer.elapsed(TimeUnit.MILLISECONDS));
        }
        catch (UncheckedIOException | IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to spill pages", e);
        }
    }

    private InputStream getStreamForReading(InputStream inputStream, int bufferSize) throws IOException
    {
        InputStream tmpInputStream = inputStream;
        if (spillCipher.isPresent()) {
            tmpInputStream = new CipherInputStream(tmpInputStream, spillCipher.get().getDecryptionCipher(cipherIV));
        }

        if (compressionEnabled) {
            tmpInputStream = new SnappyFramedInputStream(tmpInputStream);
        }

        if (useKryo) {
            return new Input(tmpInputStream, bufferSize);
        }
        return new InputStreamSliceInput(tmpInputStream, bufferSize);
    }

    private Predicate<InputStream> getStreamEndOfData()
    {
        if (useKryo) {
            return (input) -> ((Input) input).end();
        }

        return (input) -> !((InputStreamSliceInput) input).isReadable();
    }

    private Iterator<Page> readPages()
    {
        checkState(writable, "Repeated reads are disallowed to prevent potential resource leaks");
        writable = false;

        try {
            InputStream input = closer.register(targetFile.newInputStream());
            Iterator<Page> pages;

            if (useDirectSerde) {
                pages = PagesSerdeUtil.readPagesDirect(serde, getStreamForReading(input, BUFFER_SIZE), getStreamEndOfData(), spillPrefetchReadPages);
            }
            else {
                pages = PagesSerdeUtil.readPages(serde, new InputStreamSliceInput(input, BUFFER_SIZE), spillPrefetchReadPages);
            }
            return closeWhenExhausted(pages, input, localSpillContext);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to read spilled pages", e);
        }
    }

    @Override
    public void close()
    {
        closer.register(localSpillContext);
        closer.register(() -> memoryContext.setBytes(0));
        try {
            closer.close();
            cipherIV = null;
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to close spiller", e);
        }
    }

    private void checkNoSpillInProgress()
    {
        checkState(spillInProgress.isDone(), "spill in progress");
    }

    private static <T> Iterator<T> closeWhenExhausted(Iterator<T> iterator, Closeable resource, SpillContext spillerStats)
    {
        requireNonNull(iterator, "iterator is null");
        requireNonNull(resource, "resource is null");

        return new AbstractIterator<T>()
        {
            Stopwatch stopwatch = Stopwatch.createUnstarted();

            @Override
            protected T computeNext()
            {
                try {
                    stopwatch.reset();
                    stopwatch.start();
                    if (iterator.hasNext()) {
                        return iterator.next();
                    }
                    try {
                        resource.close();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return endOfData();
                }
                finally {
                    stopwatch.stop();
                    spillerStats.updateReadTime(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                }
            }
        };
    }

    @Override
    public void deleteFile()
    {
        targetFile.close();
    }

    @Override
    public Path getFile()
    {
        return targetFile.getFilePath();
    }

    private void createSpillFiles() throws IOException
    {
        this.targetFile = closer.register(new FileHolder(fileSystemClient.createTemporaryFile(spillPath, SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX), fileSystemClient));
    }

    private OutputStream getOutputStreamBasedOnSpillLocation() throws IOException
    {
        if (spillToHdfs) {
            return targetFile.newOutputStream();
        }
        else {
            return targetFile.newOutputStream(APPEND);
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        FileSingleStreamSpillerState state = new FileSingleStreamSpillerState();
        state.writable = this.writable;
        state.spilledPagesInMemorySize = spilledPagesInMemorySize;
        state.pageSizeList = this.pageSizeList;
        state.targetFile = this.targetFile.getFilePath().toAbsolutePath().toString();
        state.compressionEnabled = this.compressionEnabled;
        state.useDirectSerde = this.useDirectSerde;
        state.useKryo = this.useKryo;
        state.spillToHdfs = this.spillToHdfs;
        state.spillProfile = this.spillProfile;
        return state;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        try {
            FileSingleStreamSpillerState myState = (FileSingleStreamSpillerState) state;
            this.writable = myState.writable;
            this.spilledPagesInMemorySize = myState.spilledPagesInMemorySize;
            this.pageSizeList = myState.pageSizeList;
            this.targetFile.close();
            this.spillProfile = myState.spillProfile;
            Path path = Paths.get(myState.targetFile);
            this.fileSystemClient = getFileSystem(path, spillToHdfs, spillProfile, fileSystemClientManager);
            // Actual file content is restored after this returns, in SingleInputSnapshotState.loadSpilledFiles
            fileSystemClient.deleteIfExists(path);
            this.targetFile = closer.register(new FileHolder(fileSystemClient.createFile(path), fileSystemClient));
            for (Long pageSize : pageSizeList) {
                // restore localSpillContext and spillerStats
                this.localSpillContext.updateBytes(pageSize);
                this.spillerStats.addToTotalSpilledBytes(pageSize);
            }
            this.compressionEnabled = myState.compressionEnabled;
            this.useDirectSerde = myState.useDirectSerde;
            this.useKryo = myState.useKryo;
            this.spillToHdfs = myState.spillToHdfs;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class FileSingleStreamSpillerState
            implements Serializable
    {
        private boolean writable;
        private long spilledPagesInMemorySize;
        private List<Long> pageSizeList;
        private String targetFile;
        private boolean compressionEnabled;
        private boolean useDirectSerde;
        private boolean useKryo;
        private boolean spillToHdfs;
        private String spillProfile;
    }
}
