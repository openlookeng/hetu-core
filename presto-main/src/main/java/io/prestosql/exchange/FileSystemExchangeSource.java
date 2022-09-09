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
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.exchange.FileSystemExchangeConfig.DirectSerialisationType;
import io.prestosql.exchange.storage.ExchangeStorageReader;
import io.prestosql.exchange.storage.FileSystemExchangeStorage;
import io.prestosql.spi.Page;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSource
        implements ExchangeSource
{
    private static final Logger LOG = Logger.get(FileSystemExchangeSource.class);
    private final FileSystemExchangeStats stats;
    private final List<ExchangeStorageReader> readers;
    private final DirectSerialisationType directSerialisationType;
    private final int directSerialisationBufferSize;
    private volatile CompletableFuture<?> blocked;
    private volatile boolean closed;

    public FileSystemExchangeSource(
            FileSystemExchangeStorage exchangeStorage,
            FileSystemExchangeStats stats,
            List<ExchangeSourceFile> sourceFiles,
            int maxPageStorageSize,
            int exchangeSourceConcurrentReaders,
            DirectSerialisationType directSerialisationType,
            int directSerialisationBufferSize)
    {
        requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.stats = requireNonNull(stats, "stats is null");
        Queue<ExchangeSourceFile> sourceFileQueue = new ArrayBlockingQueue<>(sourceFiles.size());
        sourceFileQueue.addAll(sourceFiles);

        int numOfReaders = min(sourceFiles.size(), exchangeSourceConcurrentReaders);

        ImmutableList.Builder<ExchangeStorageReader> exchangeReaders = ImmutableList.builder();
        for (int i = 0; i < numOfReaders; i++) {
            exchangeReaders.add(exchangeStorage.createExchangeReader(sourceFileQueue, maxPageStorageSize, directSerialisationType, directSerialisationBufferSize));
        }
        this.readers = exchangeReaders.build();
        this.directSerialisationType = directSerialisationType;
        this.directSerialisationBufferSize = directSerialisationBufferSize;
        LOG.debug("directSerialisationType: " + directSerialisationType + ", directSerialisationBufferSize: " + directSerialisationBufferSize);
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        if (this.blocked != null && !this.blocked.isDone()) {
            return this.blocked;
        }
        for (ExchangeStorageReader reader : readers) {
            if (reader.isBlocked().isDone()) {
                return NOT_BLOCKED;
            }
        }
        synchronized (this) {
            if (this.blocked == null || this.blocked.isDone()) {
                this.blocked = stats.getExchangeSourceBlocked().record(toCompletableFuture(
                        nonCancellationPropagating(
                                whenAnyComplete(readers.stream()
                                        .map(ExchangeStorageReader::isBlocked)
                                        .collect(toImmutableList())))));
            }
            return this.blocked;
        }
    }

    @Override
    public boolean isFinished()
    {
        if (closed) {
            return true;
        }

        for (ExchangeStorageReader reader : readers) {
            if (!reader.isFinished()) {
                return false;
            }
        }
        return true;
    }

    @Nullable
    @Override
    public Slice read()
    {
        if (closed) {
            return null;
        }

        for (ExchangeStorageReader reader : readers) {
            if (reader.isBlocked().isDone() && !reader.isFinished()) {
                try {
                    return reader.read();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        return null;
    }

    @Nullable
    @Override
    public SerializedPage readSer()
    {
        if (closed) {
            return null;
        }

        for (ExchangeStorageReader reader : readers) {
            if (reader.isBlocked().isDone() && !reader.isFinished()) {
                try {
                    return reader.readSer();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        return null;
    }

    @Override
    public long getMemoryUsage()
    {
        long memoryUsage = 0;
        for (ExchangeStorageReader reader : readers) {
            memoryUsage += reader.getRetainedSize();
        }
        return memoryUsage;
    }

    @Override
    public void close()
    {
        // Make sure we will only close once
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }

        readers.forEach(ExchangeStorageReader::close);
    }

    @Override
    public DirectSerialisationType getDirectSerialisationType()
    {
        return directSerialisationType;
    }

    @Override
    public Page readPage(PagesSerde directSerde)
    {
        if (closed) {
            return null;
        }

        for (ExchangeStorageReader reader : readers) {
            if (reader.isBlocked().isDone() && !reader.isFinished()) {
                try {
                    return reader.read(directSerde);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        return null;
    }
}
