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
package io.prestosql.execution.buffer;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.hetu.core.transport.execution.buffer.SerializedPageSerde;
import io.prestosql.exchange.ExchangeSink;
import io.prestosql.exchange.FileSystemExchangeConfig.DirectSerialisationType;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.hetu.core.transport.execution.buffer.PagesSerde.getSerializedPagePositionCount;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class SpoolingExchangeOutputBuffer
        implements OutputBuffer
{
    private static final Logger log = Logger.get(SpoolingExchangeOutputBuffer.class);

    private final OutputBufferStateMachine stateMachine;
    private final OutputBuffers outputBuffers;
    private ExchangeSink exchangeSink;
    private final Supplier<LocalMemoryContext> memoryContextSupplier;

    private final AtomicLong peakMemoryUsage = new AtomicLong();
    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();

    public SpoolingExchangeOutputBuffer(
            OutputBufferStateMachine stateMachine,
            OutputBuffers outputBuffers,
            ExchangeSink exchangeSink,
            Supplier<LocalMemoryContext> memoryContextSupplier)
    {
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.outputBuffers = requireNonNull(outputBuffers, "outputBuffers is null");
        this.exchangeSink = requireNonNull(exchangeSink, "exchangeSink is null");
        this.memoryContextSupplier = requireNonNull(memoryContextSupplier, "memoryContextSupplier is null");
    }

    @Override
    public void setTaskContext(TaskContext taskContext)
    {
    }

    @Override
    public void setNoMoreInputChannels()
    {
    }

    @Override
    public void addInputChannel(String inputId)
    {
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        BufferState state = stateMachine.getState();
        LocalMemoryContext memoryContext = getSystemMemoryContextOrNull();
        return new OutputBufferInfo(
                "EXTERNAL",
                state,
                false,
                state.canAddPages(),
                memoryContext == null ? 0 : memoryContext.getBytes(),
                totalPagesAdded.get(),
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                ImmutableList.of());
    }

    @Override
    public boolean isFinished()
    {
        return false;
    }

    @Override
    public double getUtilization()
    {
        return 0;
    }

    @Override
    public boolean isOverutilized()
    {
        return false;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");
        if (stateMachine.getState().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }
        outputBuffers.checkValidTransition(newOutputBuffers);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long token, DataSize maxSize)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acknowledge(OutputBufferId bufferId, long token)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(OutputBufferId bufferId)
    {
    }

    @Override
    public void abort()
    {
        if (!stateMachine.abort()) {
            return;
        }
        ExchangeSink sink = exchangeSink;
        if (sink == null) {
            return;
        }
        sink.abort().whenComplete((value, failure) -> {
            if (failure != null) {
                log.warn(failure, "Error aborting exchange sink");
            }
            exchangeSink = null;
            updateMemoryUsage(0);
        });
    }

    @Override
    public Optional<Throwable> getFailureCause()
    {
        return stateMachine.getFailureCause();
    }

    @Override
    public ListenableFuture<?> isFull()
    {
        ExchangeSink sink = exchangeSink;
        if (sink != null) {
            return toListenableFuture(sink.isBlocked());
        }
        return immediateFuture(null);
    }

    @Override
    public void enqueue(List<SerializedPage> pages, String origin)
    {
        enqueue(0, pages, origin);
    }

    private int calculateSerializedPageSizeInBytes(SerializedPage page)
    {
        int sizeInBytes = Integer.BYTES     // positionCount
                + Byte.BYTES                // pageCodecMarkers
                + Integer.BYTES             // uncompressedSizeInBytes
                + Integer.BYTES             // slice length
                + page.getSlice().length(); // slice data
        if (page.getPageMetadata().size() != 0) {
            String pageProperties = page.getPageMetadata().toString();
            byte[] propertiesByte = pageProperties
                    .replaceAll(",", System.lineSeparator())
                    .substring(1, pageProperties.length() - 1)
                    .getBytes(UTF_8);
            sizeInBytes += Integer.BYTES + propertiesByte.length;
        }
        else {
            sizeInBytes += Integer.BYTES;
        }
        return sizeInBytes;
    }

    @Override
    public void enqueue(int partition, List<SerializedPage> pages, String origin)
    {
        requireNonNull(pages, "pages is null");
        if (!stateMachine.getState().canAddPages()) {
            return;
        }
        ExchangeSink sink = exchangeSink;
        checkState(sink != null, "exchangeSink is null");
        for (SerializedPage page : pages) {
            int sizeRequired = calculateSerializedPageSizeInBytes(page);
            Output output = new Output(sizeRequired);
            SerializedPageSerde.serialize(output, page);
            sink.add(partition, page);
            totalRowsAdded.addAndGet(getSerializedPagePositionCount(page.getSlice()));
        }
        updateMemoryUsage(sink.getMemoryUsage());
        totalPagesAdded.addAndGet(pages.size());
    }

    @Override
    public void setNoMorePages()
    {
        if (!stateMachine.noMorePages()) {
            return;
        }
        ExchangeSink sink = exchangeSink;
        if (sink == null) {
            return;
        }
        sink.finish().whenComplete((value, failure) -> {
            if (failure != null) {
                stateMachine.fail(failure);
            }
            else {
                stateMachine.finish();
            }
            exchangeSink = null;
            updateMemoryUsage(0);
        });
    }

    @Override
    public BufferState getState()
    {
        return stateMachine.getState();
    }

    @Override
    public void destroy()
    {
        abort();
    }

    @Override
    public void fail()
    {
    }

    @Override
    public long getPeakMemoryUsage()
    {
        return 0;
    }

    private void updateMemoryUsage(long bytes)
    {
        LocalMemoryContext context = getSystemMemoryContextOrNull();
        if (context != null) {
            context.setBytes(bytes);
        }
        updatePeakMemoryUsage(bytes);
    }

    private void updatePeakMemoryUsage(long bytes)
    {
        while (true) {
            long currentValue = peakMemoryUsage.get();
            if (currentValue >= bytes) {
                return;
            }
            if (peakMemoryUsage.compareAndSet(currentValue, bytes)) {
                return;
            }
        }
    }

    private LocalMemoryContext getSystemMemoryContextOrNull()
    {
        try {
            return memoryContextSupplier.get();
        }
        catch (RuntimeException ignored) {
            return null;
        }
    }

    @Override
    public boolean isSpoolingOutputBuffer()
    {
        return true;
    }

    public DirectSerialisationType getExchangeDirectSerialisationType()
    {
        ExchangeSink sink = exchangeSink;
        checkState(sink != null, "exchangeSink is null");
        return sink.getDirectSerialisationType();
    }

    @Override
    public void enqueuePages(List<Page> pages, String id, PagesSerde directSerde)
    {
        requireNonNull(pages, "pages is null");
        if (!stateMachine.getState().canAddPages()) {
            return;
        }
        ExchangeSink sink = exchangeSink;
        checkState(sink != null, "exchangeSink is null");
        for (Page page : pages) {
            sink.add(0, page, directSerde);
            totalRowsAdded.addAndGet(page.getPositionCount());
        }
        updateMemoryUsage(sink.getMemoryUsage());
        totalPagesAdded.addAndGet(pages.size());
    }
}
