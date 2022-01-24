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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.snapshot.MultiInputRestorable;
import io.prestosql.snapshot.MultiInputSnapshotState;
import io.prestosql.snapshot.SnapshotStateId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.execution.buffer.BufferState.FAILED;
import static io.prestosql.execution.buffer.BufferState.FINISHED;
import static io.prestosql.execution.buffer.BufferState.FLUSHING;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_PAGES;
import static io.prestosql.execution.buffer.BufferState.OPEN;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static java.util.Objects.requireNonNull;

@RestorableConfig(stateClassName = "PartitionedOutputBufferState", uncapturedFields = {"state", "outputBuffers", "memoryManager", "taskContext",
        "snapshotStates", "isSnapshotEnabled", "partitions", "noMoreInputChannels", "inputChannels"})
public class PartitionedOutputBuffer
        implements OutputBuffer, MultiInputRestorable
{
    private final StateMachine<BufferState> state;
    private final OutputBuffers outputBuffers;
    private final OutputBufferMemoryManager memoryManager;
    // Snapshot: Output buffers can receive markers from multiple drivers
    private TaskContext taskContext;
    private boolean noMoreInputChannels;
    private final Set<String> inputChannels = Sets.newConcurrentHashSet();

    private final List<ClientBuffer> partitions;
    private final List<MultiInputSnapshotState> snapshotStates = new ArrayList<>();
    private boolean isSnapshotEnabled;

    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();

    public PartitionedOutputBuffer(
            StateMachine<BufferState> state,
            OutputBuffers outputBuffers,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            Executor notificationExecutor)
    {
        this.state = requireNonNull(state, "state is null");

        requireNonNull(outputBuffers, "outputBuffers is null");
        checkArgument(outputBuffers.getType() == PARTITIONED, "Expected a PARTITIONED output buffer descriptor");
        checkArgument(outputBuffers.isNoMoreBufferIds(), "Expected a final output buffer descriptor");
        this.outputBuffers = outputBuffers;

        this.memoryManager = new OutputBufferMemoryManager(
                requireNonNull(maxBufferSize, "maxBufferSize is null").toBytes(),
                requireNonNull(systemMemoryContextSupplier, "systemMemoryContextSupplier is null"),
                requireNonNull(notificationExecutor, "notificationExecutor is null"));

        ImmutableList.Builder<ClientBuffer> partitionsBuffer = ImmutableList.builder();
        for (OutputBufferId bufferId : outputBuffers.getBuffers().keySet()) {
            ClientBuffer partition = new ClientBuffer(bufferId);
            partitionsBuffer.add(partition);
        }
        this.partitions = partitionsBuffer.build();

        state.compareAndSet(OPEN, NO_MORE_BUFFERS);
        state.compareAndSet(NO_MORE_PAGES, FLUSHING);
        checkFlushComplete();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    @Override
    public boolean isFinished()
    {
        return state.get() == FINISHED;
    }

    @Override
    public double getUtilization()
    {
        return memoryManager.getUtilization();
    }

    @Override
    public boolean isOverutilized()
    {
        return memoryManager.isOverutilized();
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //

        // always get the state first before any other stats
        BufferState bufferState = this.state.get();

        int totalBufferedPages = 0;
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (ClientBuffer partition : partitions) {
            BufferInfo bufferInfo = partition.getInfo();
            infos.add(bufferInfo);

            PageBufferInfo pageBufferInfo = bufferInfo.getPageBufferInfo();
            totalBufferedPages += pageBufferInfo.getBufferedPages();
        }

        return new OutputBufferInfo(
                "PARTITIONED",
                bufferState,
                bufferState.canAddBuffers(),
                bufferState.canAddPages(),
                memoryManager.getBufferedBytes(),
                totalBufferedPages,
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                infos.build());
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (state.get().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        // no more buffers can be added but verify this is valid state change
        outputBuffers.checkValidTransition(newOutputBuffers);
    }

    @Override
    public ListenableFuture<?> isFull()
    {
        return memoryManager.getBufferBlockedFuture();
    }

    @Override
    public void enqueue(List<SerializedPage> pages, String origin)
    {
        checkState(partitions.size() == 1, "Expected exactly one partition");
        enqueue(0, pages, origin);
    }

    @Override
    public void enqueue(int partitionNumber, List<SerializedPage> pages, String origin)
    {
        requireNonNull(pages, "pages is null");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!state.get().canAddPages()) {
            return;
        }

        if (!isSnapshotEnabled) {
            doEnqueue(partitionNumber, pages);
            return;
        }

        // Snapshot: pages being processed by the snapshotState and added to the buffer must be synchronized,
        // otherwise it's possible for some pages to be recorded as "channel state" by the snapshotState (i.e. after marker),
        // but still arrives at the buffer *before* the marker. These pages are potentially used twice, if we resume from this marker.
        if (pages.size() == 1 && pages.get(0).isMarkerPage()) {
            // Broadcast marker to all clients
            for (int i = 0; i < partitions.size(); i++) {
                MultiInputSnapshotState snapshotState = snapshotStates.get(i);
                synchronized (snapshotState) {
                    // All marker related processing is handled by this utility method
                    List<SerializedPage> processedPages = snapshotState.processSerializedPages(pages, origin);
                    doEnqueue(i, processedPages);
                }
            }
        }
        else {
            MultiInputSnapshotState snapshotState = snapshotStates.get(partitionNumber);
            synchronized (snapshotState) {
                List<SerializedPage> processedPages = snapshotState.processSerializedPages(pages, origin);
                doEnqueue(partitionNumber, processedPages);
            }
        }
    }

    private void doEnqueue(int partitionNumber, List<SerializedPage> pages)
    {
        // reserve memory
        long bytesAdded = pages.stream().mapToLong(SerializedPage::getRetainedSizeInBytes).sum();
        memoryManager.updateMemoryUsage(bytesAdded);

        // update stats
        long rowCount = pages.stream().mapToLong(SerializedPage::getPositionCount).sum();
        totalRowsAdded.addAndGet(rowCount);

        long pageCount = pages.size();
        totalPagesAdded.addAndGet(pageCount);

        // create page reference counts with an initial single reference
        List<SerializedPageReference> serializedPageReferences = pages.stream()
                .map(bufferedPage -> new SerializedPageReference(bufferedPage, 1, () -> memoryManager.updateMemoryUsage(-bufferedPage.getRetainedSizeInBytes())))
                .collect(toImmutableList());

        // add pages to the buffer (this will increase the reference count by one)
        partitions.get(partitionNumber).enqueuePages(serializedPageReferences);

        // drop the initial reference
        serializedPageReferences.forEach(SerializedPageReference::dereferencePage);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId outputBufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(outputBufferId, "outputBufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return partitions.get(outputBufferId.getId()).getPages(startingSequenceId, maxSize);
    }

    @Override
    public void acknowledge(OutputBufferId outputBufferId, long sequenceId)
    {
        requireNonNull(outputBufferId, "bufferId is null");

        partitions.get(outputBufferId.getId()).acknowledgePages(sequenceId);
    }

    @Override
    public void abort(OutputBufferId bufferId)
    {
        requireNonNull(bufferId, "bufferId is null");

        partitions.get(bufferId.getId()).destroy();

        checkFlushComplete();
    }

    @Override
    public void setNoMorePages()
    {
        state.compareAndSet(OPEN, NO_MORE_PAGES);
        state.compareAndSet(NO_MORE_BUFFERS, FLUSHING);
        memoryManager.setNoBlockOnFull();

        partitions.forEach(ClientBuffer::setNoMorePages);

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        // ignore destroy if the buffer already in a terminal state.
        if (state.setIf(FINISHED, oldState -> !oldState.isTerminal())) {
            partitions.forEach(ClientBuffer::destroy);
            memoryManager.setNoBlockOnFull();
            forceFreeMemory();
        }
    }

    @Override
    public void fail()
    {
        // ignore fail if the buffer already in a terminal state.
        if (state.setIf(FAILED, oldState -> !oldState.isTerminal())) {
            memoryManager.setNoBlockOnFull();
            forceFreeMemory();
            // DO NOT destroy buffers or set no more pages.  The coordinator manages the teardown of failed queries.
        }
    }

    @Override
    public long getPeakMemoryUsage()
    {
        return memoryManager.getPeakMemoryUsage();
    }

    @VisibleForTesting
    void forceFreeMemory()
    {
        memoryManager.close();
    }

    private void checkFlushComplete()
    {
        if (state.get() != FLUSHING && state.get() != NO_MORE_BUFFERS) {
            return;
        }

        if (partitions.stream().allMatch(ClientBuffer::isDestroyed)) {
            destroy();
        }
    }

    @VisibleForTesting
    OutputBufferMemoryManager getMemoryManager()
    {
        return memoryManager;
    }

    @Override
    public void setTaskContext(TaskContext taskContext)
    {
        checkArgument(taskContext != null, "taskContext is null");
        checkState(this.taskContext == null, "setTaskContext is called multiple times");
        this.taskContext = taskContext;
        if (SystemSessionProperties.isSnapshotEnabled(taskContext.getSession())) {
            isSnapshotEnabled = true;
            for (int i = 0; i < partitions.size(); i++) {
                // Create one snapshot state for each partition, because restored pages need to be associated with the same partition
                String componentName = "OutputBuffer" + i;
                snapshotStates.add(MultiInputSnapshotState.forTaskComponent(this, taskContext, snapshotId -> SnapshotStateId.forTaskComponent(snapshotId, taskContext, componentName)));
            }
        }
    }

    @Override
    public void setNoMoreInputChannels()
    {
        checkState(!noMoreInputChannels, "setNoMoreInputChannels is called multiple times");
        this.noMoreInputChannels = true;
    }

    @Override
    public void addInputChannel(String inputId)
    {
        checkState(!noMoreInputChannels, "addInputChannel is called after setNoMoreInputChannels");
        inputChannels.add(inputId);
    }

    @Override
    public Optional<Set<String>> getInputChannels()
    {
        return noMoreInputChannels ? Optional.of(Collections.unmodifiableSet(inputChannels)) : Optional.empty();
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        PartitionedOutputBufferState myState = new PartitionedOutputBufferState();
        myState.totalPagesAdded = totalPagesAdded.get();
        myState.totalRowsAdded = totalRowsAdded.get();
        // TODO-cp-I2DSGR: other fields worth capturing?
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        PartitionedOutputBufferState myState = (PartitionedOutputBufferState) state;
        totalPagesAdded.set(myState.totalPagesAdded);
        totalRowsAdded.set(myState.totalRowsAdded);
    }

    private static class PartitionedOutputBufferState
            implements Serializable
    {
        // Do not need to capture initialPagesForNewBuffers, because pages stored there
        // will be sent out before markers are sent to their targets.
        private long totalPagesAdded;
        private long totalRowsAdded;
    }
}
