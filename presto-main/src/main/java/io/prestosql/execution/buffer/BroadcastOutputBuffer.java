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
import com.google.common.collect.Sets.SetView;
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

import javax.annotation.concurrent.GuardedBy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.BROADCAST;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"state", "memoryManager", "taskContext", "snapshotState", "outputBuffers",
        "buffers", "initialPagesForNewBuffers", "noMoreInputChannels", "inputChannels"})
public class BroadcastOutputBuffer
        implements OutputBuffer, MultiInputRestorable
{
    private final StateMachine<BufferState> state;
    private final OutputBufferMemoryManager memoryManager;
    // Snapshot: Output buffers can receive markers from multiple drivers
    private TaskContext taskContext;
    private boolean noMoreInputChannels;
    private final Set<String> inputChannels = Sets.newConcurrentHashSet();
    private MultiInputSnapshotState snapshotState;

    @GuardedBy("this")
    private OutputBuffers outputBuffers = OutputBuffers.createInitialEmptyOutputBuffers(BROADCAST);

    @GuardedBy("this")
    private final Map<OutputBufferId, ClientBuffer> buffers = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final List<SerializedPageReference> initialPagesForNewBuffers = new ArrayList<>();

    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();
    private final AtomicLong totalBufferedPages = new AtomicLong();

    public BroadcastOutputBuffer(
            StateMachine<BufferState> state,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            Executor notificationExecutor)
    {
        this.state = requireNonNull(state, "state is null");
        this.memoryManager = new OutputBufferMemoryManager(
                requireNonNull(maxBufferSize, "maxBufferSize is null").toBytes(),
                requireNonNull(systemMemoryContextSupplier, "systemMemoryContextSupplier is null"),
                requireNonNull(notificationExecutor, "notificationExecutor is null"));
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

        // buffer it a concurrent collection so it is safe to access out side of guard
        // in this case we only want a snapshot of the current buffers
        @SuppressWarnings("FieldAccessNotGuarded")
        Collection<ClientBuffer> clientBuffers = this.buffers.values();

        return new OutputBufferInfo(
                "BROADCAST",
                bufferState,
                bufferState.canAddBuffers(),
                bufferState.canAddPages(),
                memoryManager.getBufferedBytes(),
                totalBufferedPages.get(),
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                clientBuffers.stream()
                        .map(ClientBuffer::getInfo)
                        .collect(toImmutableList()));
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        checkState(!Thread.holdsLock(this), "Can not set output buffers while holding a lock on this");
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        synchronized (this) {
            // ignore buffers added after query finishes, which can happen when a query is canceled
            // also ignore old versions, which is normal
            BufferState bufferState = this.state.get();
            if (bufferState.isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
                return;
            }

            // verify this is valid state change
            outputBuffers.checkValidTransition(newOutputBuffers);
            outputBuffers = newOutputBuffers;

            // add the new buffers
            for (Entry<OutputBufferId, Integer> entry : outputBuffers.getBuffers().entrySet()) {
                if (!buffers.containsKey(entry.getKey())) {
                    ClientBuffer buffer = getBuffer(entry.getKey());
                    if (!bufferState.canAddPages()) {
                        buffer.setNoMorePages();
                    }
                }
            }

            // update state if no more buffers is set
            if (outputBuffers.isNoMoreBufferIds()) {
                this.state.compareAndSet(OPEN, NO_MORE_BUFFERS);
                this.state.compareAndSet(NO_MORE_PAGES, FLUSHING);
            }
        }

        if (!state.get().canAddBuffers()) {
            noMoreBuffers();
        }

        checkFlushComplete();
    }

    @Override
    public ListenableFuture<?> isFull()
    {
        return memoryManager.getBufferBlockedFuture();
    }

    @Override
    public void enqueue(List<SerializedPage> pages, String origin)
    {
        checkState(!Thread.holdsLock(this), "Can not enqueue pages while holding a lock on this");
        requireNonNull(pages, "pages is null");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!state.get().canAddPages()) {
            return;
        }

        if (snapshotState == null) {
            doEnqueue(pages);
            return;
        }

        // Snapshot: pages being processed by the snapshotState and added to the buffer must be synchronized,
        // otherwise it's possible for some pages to be recorded as "channel state" by the snapshotState (i.e. after marker),
        // but still arrives at the buffer *before* the marker. These pages are potentially used twice, if we resume from this marker.
        synchronized (this) {
            // All marker related processing is handled by this utility method
            List<SerializedPage> finalPages = snapshotState.processSerializedPages(pages, origin);
            doEnqueue(finalPages);
        }
    }

    private void doEnqueue(List<SerializedPage> pages)
    {
        // reserve memory
        long bytesAdded = pages.stream().mapToLong(SerializedPage::getRetainedSizeInBytes).sum();
        memoryManager.updateMemoryUsage(bytesAdded);

        // update stats
        long rowCount = pages.stream().mapToLong(SerializedPage::getPositionCount).sum();
        totalRowsAdded.addAndGet(rowCount);
        totalPagesAdded.addAndGet(pages.size());
        totalBufferedPages.addAndGet(pages.size());

        // create page reference counts with an initial single reference
        List<SerializedPageReference> serializedPageReferences = pages.stream()
                .map(pageSplit -> new SerializedPageReference(pageSplit, 1, () -> {
                    checkState(totalBufferedPages.decrementAndGet() >= 0);
                    memoryManager.updateMemoryUsage(-pageSplit.getRetainedSizeInBytes());
                }))
                .collect(toImmutableList());

        // if we can still add buffers, remember the pages for the future buffers
        Collection<ClientBuffer> clientBuffers;
        synchronized (this) {
            if (state.get().canAddBuffers()) {
                serializedPageReferences.forEach(SerializedPageReference::addReference);
                initialPagesForNewBuffers.addAll(serializedPageReferences);
            }

            // make a copy while holding the lock to avoid race with initialPagesForNewBuffers.addAll above
            clientBuffers = safeGetBuffersSnapshot();
        }

        // add pages to all existing buffers (each buffer will increment the reference count)
        clientBuffers.forEach(partition -> partition.enqueuePages(serializedPageReferences));

        // drop the initial reference
        serializedPageReferences.forEach(SerializedPageReference::dereferencePage);
    }

    @Override
    public void enqueue(int partitionNumber, List<SerializedPage> pages, String origin)
    {
        checkState(partitionNumber == 0, "Expected partition number to be zero");
        enqueue(pages, origin);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId outputBufferId, long startingSequenceId, DataSize maxSize)
    {
        checkState(!Thread.holdsLock(this), "Can not get pages while holding a lock on this");
        requireNonNull(outputBufferId, "outputBufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return getBuffer(outputBufferId).getPages(startingSequenceId, maxSize);
    }

    @Override
    public void acknowledge(OutputBufferId bufferId, long sequenceId)
    {
        checkState(!Thread.holdsLock(this), "Can not acknowledge pages while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");

        getBuffer(bufferId).acknowledgePages(sequenceId);
    }

    @Override
    public void abort(OutputBufferId bufferId)
    {
        checkState(!Thread.holdsLock(this), "Can not abort while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");

        getBuffer(bufferId).destroy();

        checkFlushComplete();
    }

    @Override
    public void setNoMorePages()
    {
        checkState(!Thread.holdsLock(this), "Can not set no more pages while holding a lock on this");
        state.compareAndSet(OPEN, NO_MORE_PAGES);
        state.compareAndSet(NO_MORE_BUFFERS, FLUSHING);
        memoryManager.setNoBlockOnFull();

        safeGetBuffersSnapshot().forEach(ClientBuffer::setNoMorePages);

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        checkState(!Thread.holdsLock(this), "Can not destroy while holding a lock on this");

        // ignore destroy if the buffer already in a terminal state.
        if (state.setIf(FINISHED, oldState -> !oldState.isTerminal())) {
            noMoreBuffers();

            safeGetBuffersSnapshot().forEach(ClientBuffer::destroy);

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

    private synchronized ClientBuffer getBuffer(OutputBufferId id)
    {
        ClientBuffer buffer = buffers.get(id);
        if (buffer != null) {
            return buffer;
        }

        // NOTE: buffers are allowed to be created in the FINISHED state because destroy() can move to the finished state
        // without a clean "no-more-buffers" message from the scheduler.  This happens with limit queries and is ok because
        // the buffer will be immediately destroyed.
        BufferState bufferState = this.state.get();
        checkState(bufferState.canAddBuffers() || !outputBuffers.isNoMoreBufferIds(), "No more buffers already set");

        // NOTE: buffers are allowed to be created before they are explicitly declared by setOutputBuffers
        // When no-more-buffers is set, we verify that all created buffers have been declared
        buffer = new ClientBuffer(id);

        // do not setup the new buffer if we are already failed
        if (bufferState != FAILED) {
            // add initial pages
            buffer.enqueuePages(initialPagesForNewBuffers);

            // update state
            if (!bufferState.canAddPages()) {
                // BE CAREFUL: set no more pages only if not FAILED, because this allows clients to FINISH
                buffer.setNoMorePages();
            }

            // buffer may have finished immediately before calling this method
            if (bufferState == FINISHED) {
                buffer.destroy();
            }
        }

        buffers.put(id, buffer);
        return buffer;
    }

    private synchronized Collection<ClientBuffer> safeGetBuffersSnapshot()
    {
        return ImmutableList.copyOf(this.buffers.values());
    }

    private void noMoreBuffers()
    {
        checkState(!Thread.holdsLock(this), "Can not set no more buffers while holding a lock on this");
        List<SerializedPageReference> pages;
        synchronized (this) {
            pages = ImmutableList.copyOf(initialPagesForNewBuffers);
            initialPagesForNewBuffers.clear();

            if (outputBuffers.isNoMoreBufferIds()) {
                // verify all created buffers have been declared
                SetView<OutputBufferId> undeclaredCreatedBuffers = Sets.difference(buffers.keySet(), outputBuffers.getBuffers().keySet());
                checkState(undeclaredCreatedBuffers.isEmpty(), "Final output buffers does not contain all created buffer ids: %s", undeclaredCreatedBuffers);
            }
        }

        // dereference outside of synchronized to avoid making a callback while holding a lock
        pages.forEach(SerializedPageReference::dereferencePage);
    }

    private void checkFlushComplete()
    {
        if (state.get() != FLUSHING && state.get() != NO_MORE_BUFFERS) {
            return;
        }

        if (safeGetBuffersSnapshot().stream().allMatch(ClientBuffer::isDestroyed)) {
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
        this.snapshotState = SystemSessionProperties.isSnapshotEnabled(taskContext.getSession())
                ? MultiInputSnapshotState.forTaskComponent(this, taskContext, snapshotId -> SnapshotStateId.forTaskComponent(snapshotId, taskContext, "OutputBuffer"))
                : null;
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
        BroadcastOutputBufferState myState = new BroadcastOutputBufferState();
        myState.totalPagesAdded = totalPagesAdded.get();
        myState.totalRowsAdded = totalRowsAdded.get();
        myState.totalBufferedPages = totalBufferedPages.get();
        // TODO-cp-I2DSGR: other fields worth capturing?
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        BroadcastOutputBufferState myState = (BroadcastOutputBufferState) state;
        totalPagesAdded.set(myState.totalPagesAdded);
        totalRowsAdded.set(myState.totalRowsAdded);
        totalBufferedPages.set(myState.totalBufferedPages);
    }

    private static class BroadcastOutputBufferState
            implements Serializable
    {
        // Do not need to capture initialPagesForNewBuffers, because pages stored there
        // will be sent out before markers are sent to their targets.
        private long totalPagesAdded;
        private long totalRowsAdded;
        private long totalBufferedPages;
    }
}
