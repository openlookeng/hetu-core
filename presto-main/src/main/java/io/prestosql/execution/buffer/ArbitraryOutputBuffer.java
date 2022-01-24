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
import io.prestosql.execution.buffer.ClientBuffer.PagesSupplier;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.snapshot.MultiInputRestorable;
import io.prestosql.snapshot.MultiInputSnapshotState;
import io.prestosql.snapshot.SnapshotStateId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.execution.buffer.BufferState.FAILED;
import static io.prestosql.execution.buffer.BufferState.FINISHED;
import static io.prestosql.execution.buffer.BufferState.FLUSHING;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_PAGES;
import static io.prestosql.execution.buffer.BufferState.OPEN;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.ARBITRARY;
import static io.prestosql.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static java.util.Objects.requireNonNull;

/**
 * A buffer that assigns pages to queues based on a first come, first served basis.
 */
@RestorableConfig(uncapturedFields = {"memoryManager", "taskContext", "inputChannels", "snapshotState", "outputBuffers",
        "masterBuffer", "buffers", "markersForNewBuffers", "state", "noMoreInputChannels"})
public class ArbitraryOutputBuffer
        implements OutputBuffer, MultiInputRestorable
{
    private final OutputBufferMemoryManager memoryManager;
    // Snapshot: Output buffers can receive markers from multiple drivers
    private TaskContext taskContext;
    private boolean noMoreInputChannels;
    private final Set<String> inputChannels = Sets.newConcurrentHashSet();
    private MultiInputSnapshotState snapshotState;

    @GuardedBy("this")
    private OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY);

    private final MasterBuffer masterBuffer;

    @GuardedBy("this")
    private final ConcurrentMap<OutputBufferId, ClientBuffer> buffers = new ConcurrentHashMap<>();

    // When not all remote clients are ready (canAddBuffers is true), need to remember markers, to send to newly added clients
    @GuardedBy("this")
    private final List<SerializedPageReference> markersForNewBuffers = new ArrayList<>();

    private final StateMachine<BufferState> state;

    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();

    public ArbitraryOutputBuffer(
            StateMachine<BufferState> state,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            Executor notificationExecutor)
    {
        this.state = requireNonNull(state, "state is null");
        requireNonNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        this.memoryManager = new OutputBufferMemoryManager(
                maxBufferSize.toBytes(),
                requireNonNull(systemMemoryContextSupplier, "systemMemoryContextSupplier is null"),
                requireNonNull(notificationExecutor, "notificationExecutor is null"));
        this.masterBuffer = new MasterBuffer();
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
        return (memoryManager.getUtilization() >= 0.5) || !state.get().canAddPages();
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //

        // always get the state first before any other stats
        BufferState bufferState = this.state.get();

        // buffers it a concurrent collection so it is safe to access out side of guard
        // in this case we only want a snapshot of the current buffers
        @SuppressWarnings("FieldAccessNotGuarded")
        Collection<ClientBuffer> clientBuffers = this.buffers.values();

        int totalBufferedPages = masterBuffer.getBufferedPages();
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (ClientBuffer buffer : clientBuffers) {
            BufferInfo bufferInfo = buffer.getInfo();
            infos.add(bufferInfo);

            PageBufferInfo pageBufferInfo = bufferInfo.getPageBufferInfo();
            totalBufferedPages += pageBufferInfo.getBufferedPages();
        }

        return new OutputBufferInfo(
                "ARBITRARY",
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
            for (OutputBufferId outputBufferId : outputBuffers.getBuffers().keySet()) {
                getBuffer(outputBufferId);
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
        requireNonNull(pages, "page is null");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!state.get().canAddPages()) {
            return;
        }

        if (snapshotState == null) {
            doEnqueue(pages, null);
            return;
        }

        // Snapshot: pages being processed by the snapshotState and added to the buffer must be synchronized,
        // otherwise it's possible for some pages to be recorded as "channel state" by the snapshotState (i.e. after marker),
        // but still arrives at the buffer *before* the marker. These pages are potentially used twice, if we resume from this marker.
        synchronized (this) {
            // All marker related processing is handled by this utility method
            List<SerializedPage> finalPages = snapshotState.processSerializedPages(pages, origin);
            Collection<ClientBuffer> targetClients = null;
            if (finalPages.size() == 1 && finalPages.get(0).isMarkerPage()) {
                // Broadcast marker to all clients
                targetClients = safeGetBuffersSnapshot();
            }
            doEnqueue(finalPages, targetClients);
        }
    }

    private void doEnqueue(List<SerializedPage> pages, Collection<ClientBuffer> targetClients)
    {
        // reserve memory
        long bytesAdded = pages.stream().mapToLong(SerializedPage::getRetainedSizeInBytes).sum();
        memoryManager.updateMemoryUsage(bytesAdded);

        // update stats
        long rowCount = pages.stream().mapToLong(SerializedPage::getPositionCount).sum();
        totalRowsAdded.addAndGet(rowCount);
        totalPagesAdded.addAndGet(pages.size());

        // create page reference counts with an initial single reference
        List<SerializedPageReference> serializedPageReferences = pages.stream()
                .map(pageSplit -> new SerializedPageReference(pageSplit, 1, () -> memoryManager.updateMemoryUsage(-pageSplit.getRetainedSizeInBytes())))
                .collect(toImmutableList());

        // add pages to the buffer
        masterBuffer.addPages(serializedPageReferences, targetClients);

        // process any pending reads from the client buffers
        for (ClientBuffer clientBuffer : safeGetBuffersSnapshot()) {
            if (masterBuffer.isEmpty()) {
                break;
            }
            clientBuffer.loadPagesIfNecessary(masterBuffer);
        }

        if (targetClients != null && state.get().canAddBuffers()) {
            // targetClients != null means current page is a marker page
            // Record the marker so when new buffers are added, the marker is sent to those new buffers
            serializedPageReferences.forEach(SerializedPageReference::addReference);
            markersForNewBuffers.addAll(serializedPageReferences);
        }
    }

    @Override
    public void enqueue(int partition, List<SerializedPage> pages, String origin)
    {
        checkState(partition == 0, "Expected partition number to be zero");
        enqueue(pages, origin);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
    {
        checkState(!Thread.holdsLock(this), "Can not get pages while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return getBuffer(bufferId).getPages(startingSequenceId, maxSize, Optional.of(masterBuffer));
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

        masterBuffer.setNoMorePages();

        // process any pending reads from the client buffers
        for (ClientBuffer clientBuffer : safeGetBuffersSnapshot()) {
            clientBuffer.loadPagesIfNecessary(masterBuffer);
        }

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        checkState(!Thread.holdsLock(this), "Can not destroy while holding a lock on this");

        // ignore destroy if the buffer already in a terminal state.
        if (state.setIf(FINISHED, oldState -> !oldState.isTerminal())) {
            noMoreBuffers();

            masterBuffer.destroy();

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
            // add pending markers
            if (!markersForNewBuffers.isEmpty()) {
                markersForNewBuffers.forEach(SerializedPageReference::addReference);
                masterBuffer.insertMarkers(markersForNewBuffers, buffer);
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
        List<SerializedPageReference> pages = null;
        synchronized (this) {
            if (!markersForNewBuffers.isEmpty()) {
                pages = ImmutableList.copyOf(markersForNewBuffers);
                markersForNewBuffers.clear();
            }

            if (outputBuffers.isNoMoreBufferIds()) {
                // verify all created buffers have been declared
                SetView<OutputBufferId> undeclaredCreatedBuffers = Sets.difference(buffers.keySet(), outputBuffers.getBuffers().keySet());
                checkState(undeclaredCreatedBuffers.isEmpty(), "Final output buffers does not contain all created buffer ids: %s", undeclaredCreatedBuffers);
            }
        }

        if (pages != null) {
            // dereference outside of synchronized to avoid making a callback while holding a lock
            pages.forEach(SerializedPageReference::dereferencePage);
        }
    }

    @GuardedBy("this")
    private void checkFlushComplete()
    {
        // This buffer type assigns each page to a single, arbitrary reader,
        // so we don't need to wait for no-more-buffers to finish the buffer.
        // Any readers added after finish will simply receive no data.
        BufferState bufferState = this.state.get();
        if ((bufferState == FLUSHING) || ((bufferState == NO_MORE_PAGES) && masterBuffer.isEmpty())) {
            if (safeGetBuffersSnapshot().stream().allMatch(ClientBuffer::isDestroyed)) {
                destroy();
            }
        }
    }

    @ThreadSafe
    private class MasterBuffer
            implements PagesSupplier
    {
        @GuardedBy("this")
        private final LinkedList<SerializedPageReference> masterBuffer = new LinkedList<>();
        // Snapshot: pararrel array to masterBuffer, about which clients need to receive this page. "null" indicates any one client.
        @GuardedBy("this")
        private final LinkedList<Set<ClientBuffer>> targetClients = new LinkedList<>();

        @GuardedBy("this")
        private boolean noMorePages;

        private final AtomicInteger bufferedPages = new AtomicInteger();

        public synchronized void addPages(List<SerializedPageReference> pages, Collection<ClientBuffer> clients)
        {
            masterBuffer.addAll(pages);
            if (snapshotState != null) {
                for (SerializedPageReference page : pages) {
                    if (clients == null) {
                        targetClients.add(null);
                    }
                    else {
                        targetClients.add(new HashSet<>(clients));
                        // This page will be returned multiple times. Make sure reference count is accurate.
                        for (int i = 0; i < clients.size() - 1; i++) {
                            page.addReference();
                        }
                    }
                }
                checkState(masterBuffer.size() == targetClients.size(), "Lists have different sizes");
            }
            bufferedPages.set(masterBuffer.size());
        }

        public void insertMarkers(List<SerializedPageReference> markers, ClientBuffer buffer)
        {
            // Markers are potentially inserted at the beginning of the queue. Process them reversely to maintain marker order.
            for (int i = markers.size() - 1; i >= 0; i--) {
                SerializedPageReference page = markers.get(i);
                // If this marker still exists in the queue, then add target to its target list;
                // otherwise add the page to the front of the queue, so it's the first page retrieved by the new target.
                Iterator<SerializedPageReference> pageIterator = masterBuffer.iterator();
                Iterator<Set<ClientBuffer>> targetIterator = targetClients.iterator();
                while (pageIterator.hasNext() && pageIterator.next() != page) {
                    targetIterator.next();
                }
                if (targetIterator.hasNext()) {
                    targetIterator.next().add(buffer);
                }
                else {
                    masterBuffer.addFirst(page);
                    targetClients.addFirst(Sets.newHashSet(buffer));
                }
                page.addReference();
            }
            bufferedPages.set(masterBuffer.size());
        }

        public synchronized boolean isEmpty()
        {
            return masterBuffer.isEmpty();
        }

        @Override
        public synchronized boolean mayHaveMorePages()
        {
            return !noMorePages || !masterBuffer.isEmpty();
        }

        public synchronized void setNoMorePages()
        {
            this.noMorePages = true;
        }

        @Override
        public synchronized List<SerializedPageReference> getPages(ClientBuffer client, DataSize maxSize)
        {
            long maxBytes = maxSize.toBytes();
            List<SerializedPageReference> pages = new ArrayList<>();
            long bytesRemoved = 0;

            if (snapshotState == null) {
                while (true) {
                    SerializedPageReference page = masterBuffer.peek();
                    if (page == null) {
                        break;
                    }
                    bytesRemoved += page.getRetainedSizeInBytes();
                    // break (and don't add) if this page would exceed the limit
                    if (!pages.isEmpty() && bytesRemoved > maxBytes) {
                        break;
                    }
                    // this should not happen since we have a lock
                    checkState(masterBuffer.poll() == page, "Master buffer corrupted");
                    pages.add(page);
                }
            }
            else {
                for (int i = 0; i < masterBuffer.size(); i++) {
                    Set<ClientBuffer> clients = targetClients.get(i);
                    if (clients != null && !clients.contains(client)) {
                        // Current page is not for this client. Check later pages.
                        continue;
                    }

                    SerializedPageReference page = masterBuffer.get(i);
                    bytesRemoved += page.getRetainedSizeInBytes();
                    // break (and don't add) if this page would exceed the limit
                    if (!pages.isEmpty() && bytesRemoved > maxBytes) {
                        break;
                    }
                    pages.add(page);

                    if (clients != null) {
                        clients.remove(client);
                    }
                    if (clients == null || clients.isEmpty()) {
                        // this should not happen since we have a lock
                        checkState(masterBuffer.remove(i) == page, "Master buffer corrupted");
                        checkState(targetClients.remove(i) == clients, "Target client list corrupted");
                        i--;
                    }
                }
            }

            bufferedPages.set(masterBuffer.size());

            return ImmutableList.copyOf(pages);
        }

        public void destroy()
        {
            checkState(!Thread.holdsLock(this), "Can not destroy master buffer while holding a lock on this");
            List<SerializedPageReference> pages;
            synchronized (this) {
                pages = ImmutableList.copyOf(masterBuffer);
                masterBuffer.clear();
                bufferedPages.set(0);
            }

            // dereference outside of synchronized to avoid making a callback while holding a lock
            pages.forEach(SerializedPageReference::dereferencePage);
        }

        public int getBufferedPages()
        {
            return bufferedPages.get();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("bufferedPages", bufferedPages.get())
                    .toString();
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
        ArbitraryOutputBufferState myState = new ArbitraryOutputBufferState();
        myState.totalPagesAdded = totalPagesAdded.get();
        myState.totalRowsAdded = totalRowsAdded.get();
        // TODO-cp-I2DSGR: other fields worth capturing?
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        ArbitraryOutputBufferState myState = (ArbitraryOutputBufferState) state;
        totalPagesAdded.set(myState.totalPagesAdded);
        totalRowsAdded.set(myState.totalRowsAdded);
    }

    private static class ArbitraryOutputBufferState
            implements Serializable
    {
        // Do not need to capture markersForNewBuffers, because pages stored there
        // will be sent out before markers are sent to their targets.
        private long totalPagesAdded;
        private long totalRowsAdded;
    }
}
