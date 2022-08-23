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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.ExtendedSettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.exchange.ExchangeManagerRegistry;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.exchange.ExchangeManager;
import io.prestosql.spi.exchange.ExchangeSink;
import io.prestosql.spi.exchange.ExchangeSinkInstanceHandle;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.execution.buffer.BufferResult.emptyResults;
import static io.prestosql.execution.buffer.BufferState.FAILED;
import static io.prestosql.execution.buffer.BufferState.FINISHED;
import static java.util.Objects.requireNonNull;

public class LazyOutputBuffer
        implements OutputBuffer
{
    private static final Logger LOG = Logger.get(LazyOutputBuffer.class);
    private final OutputBufferStateMachine stateMachine;

    private final DataSize maxBufferSize;
    private final Supplier<LocalMemoryContext> systemMemoryContextSupplier;
    private final Executor executor;

    @GuardedBy("this")
    private OutputBuffer delegate;

    @GuardedBy("this")
    private final Set<OutputBufferId> abortedBuffers = new HashSet<>();

    @GuardedBy("this")
    private final List<PendingRead> pendingReads = new ArrayList<>();
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private Optional<ExchangeSink> exchangeSink;

    public LazyOutputBuffer(
            TaskId taskId,
            Executor executor,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            ExchangeManagerRegistry exchangeManagerRegistry)
    {
        requireNonNull(taskId, "taskId is null");
        this.executor = requireNonNull(executor, "executor is null");
        stateMachine = new OutputBufferStateMachine(taskId, executor);
        this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        this.systemMemoryContextSupplier = requireNonNull(systemMemoryContextSupplier, "systemMemoryContextSupplier is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public boolean isFinished()
    {
        return stateMachine.getState() == FINISHED;
    }

    @Override
    public double getUtilization()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            outputBuffer = delegate;
        }

        // until output buffer is initialized, it is "full"
        if (outputBuffer == null) {
            return 1.0;
        }
        return outputBuffer.getUtilization();
    }

    @Override
    public boolean isOverutilized()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            outputBuffer = delegate;
        }

        // until output buffer is initialized, readers cannot enqueue and thus cannot be blocked
        return (outputBuffer != null) && outputBuffer.isOverutilized();
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            outputBuffer = delegate;
        }

        if (outputBuffer == null) {
            //
            // NOTE: this code must be lock free to not hanging state machine updates
            //
            BufferState bufferState = this.stateMachine.getState();

            return new OutputBufferInfo(
                    "UNINITIALIZED",
                    bufferState,
                    bufferState.canAddBuffers(),
                    bufferState.canAddPages(),
                    0,
                    0,
                    0,
                    0,
                    ImmutableList.of());
        }
        return outputBuffer.getInfo();
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        Set<OutputBufferId> abortedBuffersIds = ImmutableSet.of();
        List<PendingRead> bufferPendingReads = ImmutableList.of();
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                // ignore set output if buffer was already destroyed or failed
                if (stateMachine.getState().isTerminal()) {
                    return;
                }
                switch (newOutputBuffers.getType()) {
                    case PARTITIONED:
                        delegate = new PartitionedOutputBuffer(stateMachine, newOutputBuffers, maxBufferSize, systemMemoryContextSupplier, executor);
                        break;
                    case BROADCAST:
                        delegate = new BroadcastOutputBuffer(stateMachine, maxBufferSize, systemMemoryContextSupplier, executor);
                        break;
                    case ARBITRARY:
                        delegate = new ArbitraryOutputBuffer(stateMachine, maxBufferSize, systemMemoryContextSupplier, executor);
                        break;
                    default:
                        //TODO(Alex): decide the spool output buffer
                        LOG.info("Unexpected output buffer type: " + newOutputBuffers.getType());
                }

                if (newOutputBuffers.getExchangeSinkInstanceHandle().isPresent()) {
                    ExchangeSinkInstanceHandle exchangeSinkInstanceHandle = newOutputBuffers.getExchangeSinkInstanceHandle()
                            .orElseThrow(() -> new IllegalArgumentException("exchange sink handle is expected to be present for buffer type EXTERNAL"));
                    ExchangeManager exchangeManager = exchangeManagerRegistry.getExchangeManager();
                    ExchangeSink exchangeSinkInstance = exchangeManager.createSink(exchangeSinkInstanceHandle, false); //TODO: create directories
                    this.exchangeSink = Optional.ofNullable(exchangeSinkInstance);
                }
                else {
                    this.exchangeSink = Optional.empty();
                }

                // process pending aborts and reads outside of synchronized lock
                abortedBuffersIds = ImmutableSet.copyOf(this.abortedBuffers);
                this.abortedBuffers.clear();
                bufferPendingReads = ImmutableList.copyOf(this.pendingReads);
                this.pendingReads.clear();
            }
            this.exchangeSink.ifPresent(sink -> delegate = new SpoolingExchangeOutputBuffer(
                    stateMachine,
                    newOutputBuffers,
                    sink,
                    systemMemoryContextSupplier));
            outputBuffer = delegate;
        }

        outputBuffer.setOutputBuffers(newOutputBuffers);

        // process pending aborts and reads outside of synchronized lock
        abortedBuffersIds.forEach(outputBuffer::abort);
        for (PendingRead pendingRead : bufferPendingReads) {
            pendingRead.process(outputBuffer);
        }
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long token, DataSize maxSize)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                if (stateMachine.getState() == FINISHED) {
                    return immediateFuture(emptyResults(0, true));
                }

                PendingRead pendingRead = new PendingRead(bufferId, token, maxSize);
                pendingReads.add(pendingRead);
                return pendingRead.getFutureResult();
            }
            outputBuffer = delegate;
        }
        return outputBuffer.get(bufferId, token, maxSize);
    }

    @Override
    public void acknowledge(OutputBufferId bufferId, long token)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "delegate is null");
            outputBuffer = delegate;
        }
        outputBuffer.acknowledge(bufferId, token);
    }

    @Override
    public void abort(OutputBufferId bufferId)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                abortedBuffers.add(bufferId);
                // Normally, we should free any pending readers for this buffer,
                // but we assume that the real buffer will be created quickly.
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.abort(bufferId);
    }

    @Override
    public void abort()
    {
        OutputBuffer outputBuffer = delegate;
        if (outputBuffer == null) {
            synchronized (this) {
                if (delegate == null) {
                    stateMachine.abort();
                    return;
                }
                outputBuffer = delegate;
            }
        }
        outputBuffer.abort();
    }

    @Override
    public Optional<Throwable> getFailureCause()
    {
        return stateMachine.getFailureCause();
    }

    @Override
    public ListenableFuture<?> isFull()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        return outputBuffer.isFull();
    }

    @Override
    public void enqueue(List<SerializedPage> pages, String origin)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        outputBuffer.enqueue(pages, origin);
    }

    @Override
    public void enqueue(int partition, List<SerializedPage> pages, String origin)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        outputBuffer.enqueue(partition, pages, origin);
    }

    @Override
    public void setNoMorePages()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        outputBuffer.setNoMorePages();
    }

    @Override
    public BufferState getState()
    {
        return stateMachine.getState();
    }

    @Override
    public void destroy()
    {
        OutputBuffer outputBuffer;
        List<PendingRead> bufferPendingReads = ImmutableList.of();
        synchronized (this) {
            if (delegate == null) {
                // ignore destroy if the buffer already in a terminal state.
                if (!stateMachine.setIf(FINISHED, state -> !state.isTerminal())) {
                    return;
                }

                bufferPendingReads = ImmutableList.copyOf(this.pendingReads);
                this.pendingReads.clear();
            }
            outputBuffer = delegate;
        }

        // if there is no output buffer, free the pending reads
        if (outputBuffer == null) {
            for (PendingRead pendingRead : bufferPendingReads) {
                pendingRead.getFutureResult().set(emptyResults(0, true));
            }
            return;
        }

        outputBuffer.destroy();
    }

    @Override
    public void fail()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                // ignore fail if the buffer already in a terminal state.
                stateMachine.setIf(FAILED, state -> !state.isTerminal());

                // Do not free readers on fail
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.fail();
    }

    @Override
    public long getPeakMemoryUsage()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            outputBuffer = delegate;
        }

        if (outputBuffer != null) {
            return outputBuffer.getPeakMemoryUsage();
        }
        return 0;
    }

    private static class PendingRead
    {
        private final OutputBufferId bufferId;
        private final long startingSequenceId;
        private final DataSize maxSize;

        private final ExtendedSettableFuture<BufferResult> futureResult = ExtendedSettableFuture.create();

        public PendingRead(OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
        {
            this.bufferId = requireNonNull(bufferId, "bufferId is null");
            this.startingSequenceId = startingSequenceId;
            this.maxSize = requireNonNull(maxSize, "maxSize is null");
        }

        public ExtendedSettableFuture<BufferResult> getFutureResult()
        {
            return futureResult;
        }

        public void process(OutputBuffer delegate)
        {
            if (futureResult.isDone()) {
                return;
            }

            try {
                ListenableFuture<BufferResult> result = delegate.get(bufferId, startingSequenceId, maxSize);
                futureResult.setAsync(result);
            }
            catch (Exception e) {
                futureResult.setException(e);
            }
        }
    }

    @Override
    public void setTaskContext(TaskContext taskContext)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "delegate is null");
            outputBuffer = delegate;
        }
        outputBuffer.setTaskContext(taskContext);
    }

    @Override
    public void setNoMoreInputChannels()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "delegate is null");
            outputBuffer = delegate;
        }
        outputBuffer.setNoMoreInputChannels();
    }

    @Override
    public void addInputChannel(String inputId)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "delegate is null");
            outputBuffer = delegate;
        }
        outputBuffer.addInputChannel(inputId);
    }
}
