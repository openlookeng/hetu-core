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
package io.hetu.core.plugin.exchange.filesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.hetu.core.plugin.exchange.filesystem.storage.ExchangeStorageWriter;
import io.hetu.core.plugin.exchange.filesystem.storage.FileSystemExchangeStorage;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.exchange.ExchangeSink;
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
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
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
    private static final Logger LOG = Logger.get(FileSystemExchangeSink.class);
    public static final String COMMITTED_MARKER_FILE_NAME = "committed";
    public static final String DATA_FILE_SUFFIX = ".data";

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

    private final Map<Integer, BufferedStorageWriter> writerMap = new ConcurrentHashMap<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private boolean closed;

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
            long maxFileSizeInBytes)
    {
        checkArgument(maxPageStorageSizeInBytes <= maxFileSizeInBytes,
                format("maxPageStorageSizeInBytes %s exceeded maxFileSizeInBytes %s", succinctBytes(maxPageStorageSizeInBytes), succinctBytes(maxFileSizeInBytes)));
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.outputPartitionCount = outputPartitionCount;
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
        this.exchangeCompressionEnabled = exchangeCompressionEnabled;
        this.preserveRecordsOrder = preserveRecordsOrder;
        this.maxPageStorageSizeInBytes = maxPageStorageSizeInBytes;
        this.maxFileSizeInBytes = maxFileSizeInBytes;
        this.bufferPool = new BufferPool(
                stats,
                max(outputPartitionCount * exchangeSinkBuffersPerPartition, exchangeSinkBufferPoolMinSize),
                exchangeStorage.getWriterBufferSize());
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        return bufferPool.isBlocked();
    }

    @Override
    public void add(int partitionId, Slice data)
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
        writer.write(data);
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
                maxFileSizeInBytes);
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
                + bufferPool.bufferRetainedSize
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
        bufferPool.close();
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

        @GuardedBy("this")
        private ExchangeStorageWriter currentWriter;
        @GuardedBy("this")
        private long currentFileSize;
        @GuardedBy("this")
        private SliceOutput currentBuffer;
        @GuardedBy("this")
        private final List<ExchangeStorageWriter> writers = new ArrayList<>();
        @GuardedBy("this")
        private boolean closed;

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
                                     long maxFileSizeInBytes)
        {
            this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
            this.stats = requireNonNull(stats, "stats is null");
            this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
            this.secretKey = requireNonNull(secretKey, "secretKey is null");
            this.exchangeCompressionEnabled = exchangeCompressionEnabled;
            this.preserveRecordsOrder = preserveRecordsOrder;
            this.partitionId = partitionId;
            this.bufferPool = requireNonNull(bufferPool, "bufferPool is null");
            this.failure = requireNonNull(failure, "failure is null");
            this.maxPageStorageSizeInBytes = maxPageStorageSizeInBytes;
            this.maxFileSizeInBytes = maxFileSizeInBytes;

            addExchangeStorageWriter();
        }

        private void addExchangeStorageWriter()
        {
            currentWriter = exchangeStorage.createExchangeWriter(
                    outputDirectory.resolve(partitionId + "_" + writers.size() + DATA_FILE_SUFFIX),
                    secretKey, exchangeCompressionEnabled);
            writers.add(currentWriter);
        }

        private String propertiesToString(Properties properties)
        {
            StringBuilder stringBuilder = new StringBuilder();
            if (properties.size() > 0) {
                for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                    stringBuilder.append(entry.getKey()).append("=")
                            .append(entry.getValue()).append(System.lineSeparator());
                }
                stringBuilder.replace(stringBuilder.length() - 1, stringBuilder.length(), System.lineSeparator());
            }
            return stringBuilder.toString();
        }

        public synchronized void write(Slice data)
        {
            if (closed) {
                return;
            }

            int requiredPageStorageSize = Integer.BYTES + data.length();
            if (requiredPageStorageSize > maxPageStorageSizeInBytes) {
                throw new PrestoException(NOT_SUPPORTED, format("Max page storage size of %s exceeded: %s",
                        succinctBytes(maxPageStorageSizeInBytes),
                        succinctBytes(requiredPageStorageSize)));
            }

            if (currentFileSize + requiredPageStorageSize > maxFileSizeInBytes && !preserveRecordsOrder) {
                stats.getFileSizeInBytes().add(currentFileSize);
                flushIfNeeded(true);
                addExchangeStorageWriter();
                currentFileSize = 0;
                currentBuffer = null;
            }

            writeInternal(Slices.wrappedIntArray(data.length()));
            writeInternal(data);

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
            if (writers.size() == 1) {
                return currentWriter.finish();
            }
            return Futures.transform(Futures.allAsList(writers.stream().map(ExchangeStorageWriter::finish).collect(toImmutableList())), val -> null, directExecutor());
        }

        public synchronized ListenableFuture<Void> abort()
        {
            if (closed) {
                return immediateFuture(null);
            }
            closed = true;

            if (writers.size() == 1) {
                return currentWriter.abort();
            }
            return Futures.transform(Futures.allAsList(writers.stream().map(ExchangeStorageWriter::abort).collect(toImmutableList())), val -> null, directExecutor());
        }

        public synchronized long getRetainedSize()
        {
            return INSTANCE_SIZE + estimatedSizeOf(writers, ExchangeStorageWriter::getRetainedSize);
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
