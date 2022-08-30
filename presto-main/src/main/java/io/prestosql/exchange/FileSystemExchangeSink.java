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
package io.prestosql.exchange;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.exchange.FileSystemExchangeConfig.DirectSerialisationType;
import io.prestosql.exchange.storage.ExchangeStorageWriter;
import io.prestosql.exchange.storage.FileSystemExchangeStorage;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.checksum.CheckSumAlgorithm;
import io.prestosql.spi.exchange.checksum.ExchangeMarkerChecksumFactory;
import io.prestosql.spi.exchange.marker.ExchangeMarker;
import io.prestosql.spi.exchange.marker.ExchangeMarkerFactory;
import io.prestosql.spi.exchange.marker.HetuFileSystemExchangeMarkerFactory;
import io.prestosql.spi.util.SizeOf;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.SecretKey;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.util.SizeOf.estimatedSizeOf;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class FileSystemExchangeSink
        implements ExchangeSink
{
    private static final Logger log = Logger.get(FileSystemExchangeSink.class);
    public static final String COMMITTED_MARKER_FILE_NAME = "committed";
    public static final String DATA_FILE_SUFFIX = ".data";
    public static final String MARKER_FILE_SUFFIX = ".marker";

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FileSystemExchangeSink.class).instanceSize();

    private final FileSystemExchangeStorage exchangeStorage;
    private final FileSystemExchangeStats stats;
    private final URI outputDirectory;
    private final int outputPartitionCount;
    private final Optional<SecretKey> secretKey;
    private final boolean exchangeCompressionEnabled;
    private final boolean preserveRecordsOrder;
    private final int maxPageStorageSizeInBytes;
    private final long maxFileSizeInBytes;
    private final BufferPool bufferPool;
    private final CheckSumAlgorithm checkSumAlgorithm;
    private final int maxNumberOfPagesPerMarker;
    private final long maxSizePerMarkerInBytes;

    private final Map<Integer, BufferedStorageWriter> writerMap = new ConcurrentHashMap<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private boolean closed;
    private final DirectSerialisationType directSerialisationType;
    private final int directSerialisationBufferSize;

    public FileSystemExchangeSink(
            FileSystemExchangeStorage exchangeStorage,
            FileSystemExchangeStats stats,
            URI outputDirectory,
            int outputPartitionCount,
            Optional<SecretKey> secretKey,
            boolean exchangeCompressionEnabled,
            boolean preserveRecordsOrder,
            int maxPageStorageSizeInBytes,
            int exchangeSinkBufferPoolMinSize,
            int exchangeSinkBuffersPerPartition,
            long maxFileSizeInBytes,
            CheckSumAlgorithm checkSumAlgorithm,
            int maxNumberOfPagesPerMarker,
            long maxSizePerMarkerInBytes,
            DirectSerialisationType directSerialisationType,
            int directSerialisationBufferSize)
    {
        checkArgument(maxPageStorageSizeInBytes <= maxFileSizeInBytes,
                format("maxPageStorageSizeInBytes %s exceeded maxFileSizeInBytes %s", succinctBytes(maxPageStorageSizeInBytes), succinctBytes(maxFileSizeInBytes)));
        requireNonNull(checkSumAlgorithm, "checkSumAlgorithm is null");
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.outputPartitionCount = outputPartitionCount;
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
        this.exchangeCompressionEnabled = exchangeCompressionEnabled;
        this.preserveRecordsOrder = preserveRecordsOrder;
        this.maxPageStorageSizeInBytes = maxPageStorageSizeInBytes;
        this.maxFileSizeInBytes = maxFileSizeInBytes;
        this.checkSumAlgorithm = checkSumAlgorithm;
        this.maxNumberOfPagesPerMarker = maxNumberOfPagesPerMarker;
        this.maxSizePerMarkerInBytes = maxSizePerMarkerInBytes;
        this.directSerialisationType = directSerialisationType;
        this.directSerialisationBufferSize = directSerialisationBufferSize;
        if (directSerialisationType == DirectSerialisationType.OFF) {
            this.bufferPool = new BufferPool(
                    stats,
                    max(outputPartitionCount * exchangeSinkBuffersPerPartition, exchangeSinkBufferPoolMinSize),
                    exchangeStorage.getWriterBufferSize());
        }
        else {
            this.bufferPool = null;
        }
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        return (bufferPool != null) ? bufferPool.isBlocked() : NOT_BLOCKED;
    }

    public DirectSerialisationType getDirectSerialisationType()
    {
        return directSerialisationType;
    }

    @Override
    public void add(String taskFullId, int partitionId, Slice data, int rowCount)
    {
        throwIfFailed();

        checkArgument(partitionId < outputPartitionCount, "partition id is expected to be less than %s: %s", outputPartitionCount, partitionId);

        BufferedStorageWriter writer;
        synchronized (this) {
            if (closed) {
                return;
            }
            writer = writerMap.computeIfAbsent(partitionId, this::createWriter);
        }
        writer.write(taskFullId, data, rowCount);
    }

    @Override
    public void add(int partitionId, Page page, PagesSerde directSerde)
    {
        throwIfFailed();

        checkArgument(partitionId < outputPartitionCount, "partition id is expected to be less than %s: %s", outputPartitionCount, partitionId);
        checkState(directSerialisationType != DirectSerialisationType.OFF, "Should be used in case of direct serialization only");

        BufferedStorageWriter writer;
        synchronized (this) {
            if (closed) {
                return;
            }
            writer = writerMap.computeIfAbsent(partitionId, this::createWriter);
        }
        writer.write(page, directSerde);
    }

    private BufferedStorageWriter createWriter(int partitionId)
    {
        return new BufferedStorageWriter(
                exchangeStorage,
                stats,
                outputDirectory,
                secretKey,
                exchangeCompressionEnabled,
                preserveRecordsOrder,
                partitionId,
                bufferPool,
                failure,
                maxPageStorageSizeInBytes,
                maxFileSizeInBytes,
                checkSumAlgorithm,
                maxNumberOfPagesPerMarker,
                maxSizePerMarkerInBytes,
                directSerialisationType,
                directSerialisationBufferSize);
    }

    private void throwIfFailed()
    {
        Throwable throwable = failure.get();
        if (throwable != null) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE
                + ((bufferPool != null) ? bufferPool.bufferRetainedSize : 0)
                + estimatedSizeOf(writerMap, SizeOf::sizeOf, BufferedStorageWriter::getRetainedSize);
    }

    @Override
    public CompletableFuture<Void> finish()
    {
        if (closed) {
            return new CompletableFuture<>();
        }
        ListenableFuture<Void> finishFuture = Futures.transform(Futures.allAsList(
                        writerMap.values().stream().map(BufferedStorageWriter::finish).collect(toImmutableList())),
                val -> null,
                directExecutor());
        addSuccessCallback(finishFuture, this::destroy);
        finishFuture = Futures.transformAsync(
                finishFuture,
                ignored -> exchangeStorage.createEmptyFile(outputDirectory.resolve(COMMITTED_MARKER_FILE_NAME)),
                directExecutor());
        Futures.addCallback(finishFuture, new FutureCallback<Void>()
        {
            @Override
            public void onSuccess(@Nullable Void result)
            {
                closed = true;
            }

            @Override
            public void onFailure(Throwable t)
            {
                abort();
            }
        }, directExecutor());
        return stats.getExchangeSinkFinished().record(toCompletableFuture(finishFuture));
    }

    private void destroy()
    {
        writerMap.clear();
        if (bufferPool != null) {
            bufferPool.close();
        }
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, FileSystemExchangeSink::toVoid, directExecutor());
    }

    private static <T> Void toVoid(T value)
    {
        return null;
    }

    @Override
    public synchronized CompletableFuture<Void> abort()
    {
        if (closed) {
            return completedFuture(null);
        }
        closed = true;
        ListenableFuture<Void> abortFuture = asVoid(Futures.allAsList(
                writerMap.values().stream().map(BufferedStorageWriter::abort).collect(toImmutableList())));
        addSuccessCallback(abortFuture, this::destroy);

        return stats.getExchangeSinkAborted().record(
                toCompletableFuture(Futures.transformAsync(
                        abortFuture,
                        ignored -> exchangeStorage.deleteRecursively(ImmutableList.of(outputDirectory)),
                        directExecutor())));
    }

    @ThreadSafe
    private static class BufferedStorageWriter
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(BufferedStorageWriter.class).instanceSize();

        private final FileSystemExchangeStorage exchangeStorage;
        private final FileSystemExchangeStats stats;
        private final URI outputDirectory;
        private final Optional<SecretKey> secretKey;
        private final boolean exchangeCompressionEnabled;
        private final boolean preserveRecordsOrder;
        private final int partitionId;
        private final BufferPool bufferPool;
        private final AtomicReference<Throwable> failure;
        private final int maxPageStorageSizeInBytes;
        private final long maxFileSizeInBytes;
        private final CheckSumAlgorithm checkSumAlgorithm;
        private final int maxNumberOfPagesPerMarker;
        private final long maxSizePerMarkerInBytes;

        @GuardedBy("this")
        private ExchangeStorageWriter currentWriter;
        @GuardedBy("this")
        private ExchangeStorageWriter currentMarkerWriter;
        @GuardedBy("this")
        private long currentFileSize;
        @GuardedBy("this")
        private final ExchangeMarkerFactory markerFactory;
        @GuardedBy("this")
        private SliceOutput currentBuffer;
        @GuardedBy("this")
        private SliceOutput currentMarkerBuffer;
        @GuardedBy("this")
        private ExchangeMarker currentMarker;
        @GuardedBy("this")
        private final List<ExchangeStorageWriter> writers = new ArrayList<>();
        @GuardedBy("this")
        private final List<ExchangeStorageWriter> markerWriters = new ArrayList<>();
        @GuardedBy("this")
        private boolean closed;
        @GuardedBy("this")
        private final DirectSerialisationType directSerialisationType;
        @GuardedBy("this")
        private final int directSerialisationBufferSize;

        public BufferedStorageWriter(FileSystemExchangeStorage exchangeStorage,
                                     FileSystemExchangeStats stats,
                                     URI outputDirectory,
                                     Optional<SecretKey> secretKey,
                                     boolean exchangeCompressionEnabled,
                                     boolean preserveRecordsOrder,
                                     int partitionId,
                                     BufferPool bufferPool,
                                     AtomicReference<Throwable> failure,
                                     int maxPageStorageSizeInBytes,
                                     long maxFileSizeInBytes,
                                     CheckSumAlgorithm checkSumAlgorithm,
                                     int maxNumberOfPagesPerMarker,
                                     long maxSizePerMarkerInBytes,
                                     DirectSerialisationType directSerialisationType,
                                     int directSerialisationBufferSize)
        {
            this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
            this.stats = requireNonNull(stats, "stats is null");
            this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
            this.secretKey = requireNonNull(secretKey, "secretKey is null");
            this.exchangeCompressionEnabled = exchangeCompressionEnabled;
            this.preserveRecordsOrder = preserveRecordsOrder;
            this.partitionId = partitionId;
            this.bufferPool = bufferPool;
            this.failure = requireNonNull(failure, "failure is null");
            this.maxPageStorageSizeInBytes = maxPageStorageSizeInBytes;
            this.maxFileSizeInBytes = maxFileSizeInBytes;
            this.markerFactory = new HetuFileSystemExchangeMarkerFactory(new ExchangeMarkerChecksumFactory());
            this.checkSumAlgorithm = requireNonNull(checkSumAlgorithm, "checkSumAlgorithm is null");
            this.maxNumberOfPagesPerMarker = maxNumberOfPagesPerMarker;
            this.maxSizePerMarkerInBytes = maxSizePerMarkerInBytes;
            this.directSerialisationType = directSerialisationType;
            this.directSerialisationBufferSize = directSerialisationBufferSize;

            addExchangeStorageWriter();
            addMarkerWriter();
        }

        private void addExchangeStorageWriter()
        {
            currentWriter = exchangeStorage.createExchangeWriter(
                    outputDirectory.resolve(partitionId + "_" + writers.size() + DATA_FILE_SUFFIX),
                    secretKey, exchangeCompressionEnabled, directSerialisationType, directSerialisationBufferSize);
            writers.add(currentWriter);
        }

        private void addMarkerWriter()
        {
            currentMarkerWriter = exchangeStorage.createExchangeWriter(
                    outputDirectory.resolve(partitionId + "_" + markerWriters.size() + MARKER_FILE_SUFFIX),
                    secretKey, exchangeCompressionEnabled, directSerialisationType, directSerialisationBufferSize);
            markerWriters.add(currentMarkerWriter);
        }

        public synchronized void write(String taskFullId, Slice data, int rowCount)
        {
            if (closed) {
                return;
            }
            if (currentMarker == null) {
                currentMarker = markerFactory.create(taskFullId, 0, checkSumAlgorithm);
            }
            Slice markerPageSlice = null;
            if (currentMarker.getPageCount() == maxNumberOfPagesPerMarker || currentMarker.getSizeInBytes() + data.length() > maxSizePerMarkerInBytes) {
                currentMarker.calculateChecksum();
                markerPageSlice = SerializedPage.forExchangeMarker(currentMarker).toSlice();
                currentMarker = markerFactory.create(taskFullId, currentMarker.getOffset() + currentMarker.getSizeInBytes(), checkSumAlgorithm);
            }
            currentMarker.addPage(data, rowCount);
            int requiredPageStorageSize = data.length();
            if (markerPageSlice != null) {
                writeMarkerInternal(markerPageSlice);
                requiredPageStorageSize += markerPageSlice.length();
            }
            if (requiredPageStorageSize > maxPageStorageSizeInBytes) {
                throw new PrestoException(NOT_SUPPORTED, format("Max page storage size of %s exceeded: %s",
                        succinctBytes(maxPageStorageSizeInBytes),
                        succinctBytes(requiredPageStorageSize)));
            }
            if (currentFileSize + requiredPageStorageSize > maxFileSizeInBytes && !preserveRecordsOrder) {
                stats.getFileSizeInBytes().add(currentFileSize);
                flushIfNeeded(true);
                flushMarkerBufferIfNeeded(true);
                addExchangeStorageWriter();
                addMarkerWriter();
                currentFileSize = 0;
                currentBuffer = null;
                currentMarkerBuffer = null;
            }
            if (markerPageSlice != null) {
                writeInternal(markerPageSlice);
            }
            writeInternal(data);

            currentFileSize += requiredPageStorageSize;
        }

        private void writeMarkerInternal(Slice slice)
        {
            int position = 0;
            while (position < slice.length()) {
                if (currentMarkerBuffer == null) {
                    currentMarkerBuffer = bufferPool.take();
                    if (currentMarkerBuffer == null) {
                        return;
                    }
                }
                int writableBytes = min(currentMarkerBuffer.writableBytes(), slice.length() - position);
                currentMarkerBuffer.writeBytes(slice.getBytes(position, writableBytes));
                position += writableBytes;

                flushMarkerBufferIfNeeded(false);
            }
        }

        public synchronized void write(Page page, PagesSerde directSerde)
        {
            if (closed) {
                return;
            }

            long requiredPageStorageSize = page.getSizeInBytes();
            if (requiredPageStorageSize > maxPageStorageSizeInBytes) {
                throw new PrestoException(NOT_SUPPORTED, format("Max page storage size of %s exceeded: %s",
                        succinctBytes(maxPageStorageSizeInBytes),
                        succinctBytes(requiredPageStorageSize)));
            }

            if (currentFileSize + requiredPageStorageSize > maxFileSizeInBytes && !preserveRecordsOrder) {
                stats.getFileSizeInBytes().add(currentFileSize);
                addExchangeStorageWriter();
                currentFileSize = 0;
            }

            currentWriter.write(page, directSerde);
            currentFileSize += requiredPageStorageSize;
        }

        private void writeInternal(Slice slice)
        {
            int position = 0;
            while (position < slice.length()) {
                if (currentBuffer == null) {
                    currentBuffer = bufferPool.take();
                    if (currentBuffer == null) {
                        return;
                    }
                }
                int writableBytes = min(currentBuffer.writableBytes(), slice.length() - position);
                currentBuffer.writeBytes(slice.getBytes(position, writableBytes));
                position += writableBytes;

                flushIfNeeded(false);
            }
        }

        public synchronized ListenableFuture<Void> finish()
        {
            if (closed) {
                return immediateFailedFuture(new IllegalStateException("BufferedStorageWriter has closed"));
            }

            stats.getFileSizeInBytes().add(currentFileSize);
            flushIfNeeded(true);
            flushMarkerBufferIfNeeded(true);
            markerWriters.forEach(ExchangeStorageWriter::finish);
            return Futures.transform(Futures.allAsList(writers.stream().map(ExchangeStorageWriter::finish).collect(toImmutableList())), val -> null, directExecutor());
        }

        public synchronized ListenableFuture<Void> abort()
        {
            if (closed) {
                return immediateFuture(null);
            }
            closed = true;
            markerWriters.forEach(ExchangeStorageWriter::abort);
            if (writers.size() == 1) {
                return currentWriter.abort();
            }
            return Futures.transform(Futures.allAsList(writers.stream().map(ExchangeStorageWriter::abort).collect(toImmutableList())), val -> null, directExecutor());
        }

        public synchronized long getRetainedSize()
        {
            return INSTANCE_SIZE + estimatedSizeOf(writers, ExchangeStorageWriter::getRetainedSize) + estimatedSizeOf(markerWriters, ExchangeStorageWriter::getRetainedSize);
        }

        private void flushIfNeeded(boolean finished)
        {
            SliceOutput buffer = currentBuffer;
            if (buffer != null && (!buffer.isWritable() || finished)) {
                if (!buffer.isWritable()) {
                    currentBuffer = null;
                }
                ListenableFuture<Void> writeFuture = currentWriter.write(buffer.slice());
                writeFuture.addListener(() -> bufferPool.offer(buffer), directExecutor());
                addExceptionCallback(writeFuture, throwable -> failure.compareAndSet(null, throwable));
            }
        }

        private void flushMarkerBufferIfNeeded(boolean finished)
        {
            // small query, last few pages -> not exceed marker's page/size limit
            SliceOutput buffer = currentMarkerBuffer;
            if (buffer != null && (!buffer.isWritable() || finished)) {
                if (!buffer.isWritable()) {
                    currentMarkerBuffer = null;
                }
                ListenableFuture<Void> writeFuture = currentMarkerWriter.write(buffer.slice());
                writeFuture.addListener(() -> bufferPool.offer(buffer), directExecutor());
                addExceptionCallback(writeFuture, throwable -> failure.compareAndSet(null, throwable));
            }
        }
    }

    @ThreadSafe
    private static class BufferPool
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(BufferPool.class).instanceSize();

        private final FileSystemExchangeStats stats;
        private final int numOfBuffers;
        private final long bufferRetainedSize;
        @GuardedBy("this")
        private final Queue<SliceOutput> freeBuffersQueue;
        @GuardedBy("this")
        private CompletableFuture<Void> blockedFuture = new CompletableFuture<>();
        @GuardedBy("this")
        private boolean closed;

        public BufferPool(
                FileSystemExchangeStats stats,
                int numOfBuffers,
                int writeBufferSize)
        {
            this.stats = requireNonNull(stats, "stats is null");
            checkArgument(numOfBuffers >= 1, "numOfBuffers must be at least 1");

            this.numOfBuffers = numOfBuffers;
            this.freeBuffersQueue = new ArrayDeque<>(numOfBuffers);
            for (int i = 0; i < numOfBuffers; i++) {
                freeBuffersQueue.add(Slices.allocate(writeBufferSize).getOutput());
            }
            this.bufferRetainedSize = freeBuffersQueue.peek().getRetainedSize();
        }

        public synchronized CompletableFuture<Void> isBlocked()
        {
            if (freeBuffersQueue.isEmpty()) {
                if (blockedFuture.isDone()) {
                    blockedFuture = new CompletableFuture<>();
                    stats.getExchangeSinkBlocked().record(blockedFuture);
                }
                return blockedFuture;
            }
            else {
                return NOT_BLOCKED;
            }
        }

        public synchronized SliceOutput take()
        {
            while (true) {
                if (closed) {
                    return null;
                }
                if (!freeBuffersQueue.isEmpty()) {
                    return freeBuffersQueue.poll();
                }
                try {
                    wait();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }

        public void offer(SliceOutput buffer)
        {
            buffer.reset();

            CompletableFuture<Void> future;
            synchronized (this) {
                if (closed) {
                    return;
                }
                future = blockedFuture;
                freeBuffersQueue.add(buffer);
                notifyAll();
            }
            future.complete(null);
        }

        public synchronized long getRetainedSize()
        {
            if (closed) {
                return INSTANCE_SIZE;
            }
            else {
                return INSTANCE_SIZE + numOfBuffers * bufferRetainedSize;
            }
        }

        public void close()
        {
            CompletableFuture<Void> future;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                notifyAll();
                future = blockedFuture;
                freeBuffersQueue.clear();
            }
            future.complete(null);
        }
    }
}
