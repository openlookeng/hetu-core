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
package io.prestosql.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.hetu.core.transport.execution.buffer.PageCodecMarker;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.HttpPageBufferClient.ClientCallback;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.snapshot.MultiInputSnapshotState;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ExchangeClient
        implements Closeable
{
    private static final SerializedPage NO_MORE_PAGES = new SerializedPage(EMPTY_SLICE, PageCodecMarker.MarkerSet.empty(), 0, 0);

    private final long bufferCapacity;
    private final DataSize maxResponseSize;
    private final int concurrentRequestMultiplier;
    private final Duration maxErrorDuration;
    private final boolean acknowledgePages;
    private final HttpClient httpClient;
    private final ScheduledExecutorService scheduler;

    @GuardedBy("this")
    private boolean noMoreLocations;

    private final ConcurrentMap<String, HttpPageBufferClient> allClients = new ConcurrentHashMap<>();

    private boolean snapshotEnabled;
    // Snapshot: whether momre target (exchange operators) can be added, and all known targets. Markers are sent to all of them.
    private boolean noMoreTargets;
    private final Set<String> allTargets = new HashSet<>();
    // Markers received before all targets are known. These markers will be sent to all new targets.
    private final List<SerializedPage> pendingMarkers = Collections.synchronizedList(new ArrayList<>());

    @GuardedBy("this")
    private final Deque<HttpPageBufferClient> queuedClients = new LinkedList<>();

    private final Set<HttpPageBufferClient> completedClients = newConcurrentHashSet();
    private final LinkedBlockingDeque<SerializedPage> pageBuffer = new LinkedBlockingDeque<>();
    // Snapshot: pararrel array to pageBuffer, about which targets need to receive this page. "null" indicates any one target.
    private final LinkedBlockingDeque<Set<String>> targetBuffer = new LinkedBlockingDeque<>();

    @GuardedBy("this")
    private final List<SettableFuture<?>> blockedCallers = new ArrayList<>();

    @GuardedBy("this")
    private long bufferRetainedSizeInBytes;
    @GuardedBy("this")
    private long maxBufferRetainedSizeInBytes;
    @GuardedBy("this")
    private long successfulRequests;
    @GuardedBy("this")
    private long averageBytesPerRequest;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    private final LocalMemoryContext systemMemoryContext;
    private final Executor pageBufferClientCallbackExecutor;

    // Only set for MergeOperator, to capture marker pages
    private MultiInputSnapshotState snapshotState;

    // ExchangeClientStatus.mergeWith assumes all clients have the same bufferCapacity.
    // Please change that method accordingly when this assumption becomes not true.
    public ExchangeClient(
            DataSize bufferCapacity,
            DataSize maxResponseSize,
            int concurrentRequestMultiplier,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            HttpClient httpClient,
            ScheduledExecutorService scheduler,
            LocalMemoryContext systemMemoryContext,
            Executor pageBufferClientCallbackExecutor)
    {
        this.bufferCapacity = bufferCapacity.toBytes();
        this.maxResponseSize = maxResponseSize;
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.maxErrorDuration = maxErrorDuration;
        this.acknowledgePages = acknowledgePages;
        this.httpClient = httpClient;
        this.scheduler = scheduler;
        this.systemMemoryContext = systemMemoryContext;
        this.maxBufferRetainedSizeInBytes = Long.MIN_VALUE;
        this.pageBufferClientCallbackExecutor = requireNonNull(pageBufferClientCallbackExecutor, "pageBufferClientCallbackExecutor is null");
    }

    Set<String> getAllClients()
    {
        // Snapshot: Called by exchange operator, to get all their input channels, i.e. remote tasks
        return Collections.unmodifiableSet(allClients.keySet());
    }

    public void setSnapshotEnabled()
    {
        snapshotEnabled = true;
    }

    void setSnapshotState(MultiInputSnapshotState snapshotState)
    {
        // Only used by MergeOperator
        this.snapshotState = requireNonNull(snapshotState);
    }

    public ExchangeClientStatus getStatus()
    {
        // The stats created by this method is only for diagnostics.
        // It does not guarantee a consistent view between different exchange clients.
        // Guaranteeing a consistent view introduces significant lock contention.
        ImmutableList.Builder<PageBufferClientStatus> pageBufferClientStatusBuilder = ImmutableList.builder();
        for (HttpPageBufferClient client : allClients.values()) {
            pageBufferClientStatusBuilder.add(client.getStatus());
        }
        List<PageBufferClientStatus> pageBufferClientStatus = pageBufferClientStatusBuilder.build();
        synchronized (this) {
            int bufferedPages = pageBuffer.size();
            if (bufferedPages > 0 && pageBuffer.peekLast() == NO_MORE_PAGES) {
                bufferedPages--;
            }
            return new ExchangeClientStatus(bufferRetainedSizeInBytes, maxBufferRetainedSizeInBytes, averageBytesPerRequest, successfulRequests, bufferedPages, noMoreLocations, pageBufferClientStatus);
        }
    }

    public synchronized void addTarget(String target)
    {
        allTargets.add(target);

        // Markers are potentially inserted at the beginning of the queue. Process them reversely to maintain marker order.
        for (int i = pendingMarkers.size() - 1; i >= 0; i--) {
            SerializedPage page = pendingMarkers.get(i);
            // If this marker still exists in the queue, then add target to its target list;
            // otherwise add the page to the front of the queue, so it's the first page retrieved by the new target.
            Iterator<SerializedPage> pageIterator = pageBuffer.iterator();
            Iterator<Set<String>> targetIterator = targetBuffer.iterator();
            while (pageIterator.hasNext() && pageIterator.next() != page) {
                targetIterator.next();
            }
            if (targetIterator.hasNext()) {
                targetIterator.next().add(target);
            }
            else {
                pageBuffer.addFirst(page);
                targetBuffer.addFirst(Sets.newHashSet(target));
            }
            bufferRetainedSizeInBytes += page.getRetainedSizeInBytes();
        }
    }

    public void noMoreTargets()
    {
        noMoreTargets = true;
        pendingMarkers.clear();
        scheduleRequestIfNecessary();
    }

    public synchronized boolean addLocation(URI locationUri)
    {
        requireNonNull(locationUri, "locationUri is null");

        // Ignore new locations after close
        // NOTE: this MUST happen before checking no more locations is checked
        if (closed.get()) {
            return false;
        }

        String location = locationUri.toString();
        // ignore duplicate locations
        if (allClients.containsKey(location)) {
            return false;
        }

        checkState(!noMoreLocations, "No more locations already set");

        HttpPageBufferClient client = new HttpPageBufferClient(
                httpClient,
                maxResponseSize,
                maxErrorDuration,
                acknowledgePages,
                locationUri,
                new ExchangeClientCallback(location),
                scheduler,
                pageBufferClientCallbackExecutor);
        allClients.put(location, client);
        queuedClients.add(client);

        scheduleRequestIfNecessary();
        return true;
    }

    public synchronized void noMoreLocations()
    {
        noMoreLocations = true;
        scheduleRequestIfNecessary();
    }

    public WorkProcessor<SerializedPage> pages(String target)
    {
        return WorkProcessor.create(
                new WorkProcessor.Process<SerializedPage>()
                {
                    @Override
                    public ProcessState<SerializedPage> process()
                    {
                        SerializedPage page = pollPage(target);

                        if (page == null) {
                            if (isFinished()) {
                                return ProcessState.finished();
                            }

                            ListenableFuture<?> blocked = isBlocked();
                            if (!blocked.isDone()) {
                                return ProcessState.blocked(blocked);
                            }

                            return ProcessState.yield();
                        }

                        return ProcessState.ofResult(page);
                    }

                    @Override
                    public Object capture(BlockEncodingSerdeProvider serdeProvider)
                    {
                        return 0;
                    }

                    @Override
                    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
                    {
                    }

                    @Override
                    public Object captureResult(SerializedPage result, BlockEncodingSerdeProvider serdeProvider)
                    {
                        return result.capture(serdeProvider);
                    }

                    @Override
                    public SerializedPage restoreResult(Object resultState, BlockEncodingSerdeProvider serdeProvider)
                    {
                        return SerializedPage.restoreSerializedPage(resultState);
                    }
                });
    }

    @Nullable
    public SerializedPage pollPage(String target)
    {
        checkState(!Thread.holdsLock(this), "Can not get next page while holding a lock on this");

        throwIfFailed();

        if (closed.get()) {
            return null;
        }

        if (!snapshotEnabled) {
            return postProcessPage(pageBuffer.poll());
        }

        return postProcessPage(pollPageImpl(target));
    }

    private synchronized SerializedPage pollPageImpl(String target)
    {
        SerializedPage page = pageBuffer.poll();
        Set<String> targets = targetBuffer.poll();
        if (page != null && page.isMarkerPage()) {
            if (targets.contains(target)) {
                targets.remove(target);
                if (!targets.isEmpty()) {
                    // Put unfinished marker back at top of queue for other targets to retrieve.
                    pageBuffer.addFirst(page);
                    targetBuffer.addFirst(targets);
                }
            }
            else {
                SerializedPage marker = page;
                // Already sent marker to this target. Poll other pages.
                page = pollPageImpl(target);
                if (page == NO_MORE_PAGES) {
                    // Can't grab the no-more-pages marker when there are pending marker pages
                    pageBuffer.addFirst(NO_MORE_PAGES);
                    page = null;
                }
                // Put unfinished marker back at top of queue for other targets to retrieve.
                pageBuffer.addFirst(marker);
                targetBuffer.addFirst(targets);
            }
        }
        return page;
    }

    private SerializedPage postProcessPage(SerializedPage page)
    {
        checkState(!Thread.holdsLock(this), "Can not get next page while holding a lock on this");

        if (page == null) {
            return null;
        }

        if (page == NO_MORE_PAGES) {
            // mark client closed; close() will add the end marker
            close();

            notifyBlockedCallers();

            // don't return end of stream marker
            return null;
        }

        synchronized (this) {
            if (!closed.get()) {
                bufferRetainedSizeInBytes -= page.getRetainedSizeInBytes();
                systemMemoryContext.setBytes(bufferRetainedSizeInBytes);
                if (pageBuffer.peek() == NO_MORE_PAGES) {
                    close();
                }
            }
        }
        scheduleRequestIfNecessary();
        return page;
    }

    public boolean isFinished()
    {
        throwIfFailed();
        // For this to works, locations must never be added after is closed is set
        return isClosed() && completedClients.size() == allClients.size();
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    public synchronized void resetForResume()
    {
        cleanup();
        allClients.clear();
        queuedClients.clear();
        completedClients.clear();
        noMoreLocations = false;
        closed.set(false);
    }

    @Override
    public synchronized void close()
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        cleanup();
        if (pageBuffer.peekLast() != NO_MORE_PAGES) {
            checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
        }
        notifyBlockedCallers();
    }

    private void cleanup()
    {
        for (HttpPageBufferClient client : allClients.values()) {
            closeQuietly(client);
        }
        pageBuffer.clear();
        targetBuffer.clear();
        pendingMarkers.clear();
        systemMemoryContext.setBytes(0);
        bufferRetainedSizeInBytes = 0;
    }

    @VisibleForTesting
    synchronized void scheduleRequestIfNecessary()
    {
        if (isFinished() || isFailed()) {
            return;
        }

        // if finished, add the end marker
        if (noMoreLocations && completedClients.size() == allClients.size() && pendingMarkers.isEmpty()) {
            if (pageBuffer.peekLast() != NO_MORE_PAGES) {
                checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
            }
            if (pageBuffer.peek() == NO_MORE_PAGES) {
                close();
            }
            notifyBlockedCallers();
            return;
        }

        long neededBytes = bufferCapacity - bufferRetainedSizeInBytes;
        if (neededBytes <= 0) {
            return;
        }

        int clientCount = (int) ((1.0 * neededBytes / averageBytesPerRequest) * concurrentRequestMultiplier);
        clientCount = Math.max(clientCount, 1);

        int pendingClients = allClients.size() - queuedClients.size() - completedClients.size();
        clientCount -= pendingClients;

        for (int i = 0; i < clientCount; i++) {
            HttpPageBufferClient client = queuedClients.poll();
            if (client == null) {
                // no more clients available
                return;
            }
            client.scheduleRequest();
        }
    }

    public synchronized ListenableFuture<?> isBlocked()
    {
        if (isClosed() || isFailed() || pageBuffer.peek() != null) {
            return Futures.immediateFuture(true);
        }
        SettableFuture<?> future = SettableFuture.create();
        blockedCallers.add(future);
        return future;
    }

    private synchronized boolean addPages(List<SerializedPage> pages, String location)
    {
        if (isClosed() || isFailed()) {
            return false;
        }

        long sizeAdjustment = 0;
        if (!pages.isEmpty()) {
            if (!snapshotEnabled) {
                pageBuffer.addAll(pages);
            }
            else {
                for (SerializedPage page : pages) {
                    page.setOrigin(location);
                    if (snapshotState != null) {
                        // Only for MergeOperator
                        SerializedPage processedPage;
                        synchronized (snapshotState) {
                            processedPage = snapshotState.processSerializedPage(() -> page).orElse(null);
                        }
                        if (processedPage == null || processedPage.isMarkerPage()) {
                            // Don't add markers to the buffer, otherwise it may affect the order in which these buffers are accessed.
                            // Instead, markers are stored in and returned by the snapshot state.
                            continue;
                        }
                    }
                    pageBuffer.add(page);
                    if (page.isMarkerPage()) {
                        if (!noMoreTargets) {
                            pendingMarkers.add(page);
                        }
                        targetBuffer.add(new HashSet<>(allTargets));
                        // This page will be sent out multiple times. Adjust total size.
                        sizeAdjustment += page.getRetainedSizeInBytes() * (allTargets.size() - 1);
                    }
                    else {
                        targetBuffer.add(Collections.emptySet());
                    }
                }
            }
            // notify all blocked callers
            notifyBlockedCallers();
        }

        long pagesRetainedSizeInBytes = pages.stream()
                .mapToLong(SerializedPage::getRetainedSizeInBytes)
                .sum();

        bufferRetainedSizeInBytes += pagesRetainedSizeInBytes + sizeAdjustment;
        maxBufferRetainedSizeInBytes = Math.max(maxBufferRetainedSizeInBytes, bufferRetainedSizeInBytes);
        systemMemoryContext.setBytes(bufferRetainedSizeInBytes);
        successfulRequests++;

        long responseSize = pages.stream()
                .mapToLong(SerializedPage::getSizeInBytes)
                .sum();
        // AVG_n = AVG_(n-1) * (n-1)/n + VALUE_n / n
        averageBytesPerRequest = (long) (1.0 * averageBytesPerRequest * (successfulRequests - 1) / successfulRequests + responseSize / successfulRequests);

        return true;
    }

    private synchronized void notifyBlockedCallers()
    {
        List<SettableFuture<?>> callers = ImmutableList.copyOf(blockedCallers);
        blockedCallers.clear();
        for (SettableFuture<?> blockedCaller : callers) {
            // Notify callers in a separate thread to avoid callbacks while holding a lock
            scheduler.execute(() -> blockedCaller.set(null));
        }
    }

    private synchronized void requestComplete(HttpPageBufferClient client)
    {
        if (!queuedClients.contains(client)) {
            // Snapshot: Client may have been removed as a result of rescheduling, then don't queue it.
            // Use object identity, instead of .equals, for comparison.
            if (!snapshotEnabled || allClients.values().stream().anyMatch(c -> c == client)) {
                queuedClients.add(client);
            }
        }
        scheduleRequestIfNecessary();
    }

    private synchronized void clientFinished(HttpPageBufferClient client)
    {
        requireNonNull(client, "client is null");
        // Snapshot: Client may have been removed as a result of rescheduling, then don't add it.
        // Use object identity, instead of .equals, for comparison.
        if (!snapshotEnabled || allClients.values().stream().anyMatch(c -> c == client)) {
            completedClients.add(client);
        }
        scheduleRequestIfNecessary();
    }

    private synchronized void clientFailed(Throwable cause)
    {
        // TODO: properly handle the failed vs closed state
        // it is important not to treat failures as a successful close
        if (!isClosed()) {
            failure.compareAndSet(null, cause);
            notifyBlockedCallers();
        }
    }

    private boolean isFailed()
    {
        return failure.get() != null;
    }

    private void throwIfFailed()
    {
        Throwable t = failure.get();
        if (t != null) {
            throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    private class ExchangeClientCallback
            implements ClientCallback
    {
        private final String location;

        private ExchangeClientCallback(String location)
        {
            this.location = location;
        }

        @Override
        public boolean addPages(HttpPageBufferClient client, List<SerializedPage> pages)
        {
            requireNonNull(client, "client is null");
            requireNonNull(pages, "pages is null");
            return ExchangeClient.this.addPages(pages, location);
        }

        @Override
        public void requestComplete(HttpPageBufferClient client)
        {
            requireNonNull(client, "client is null");
            ExchangeClient.this.requestComplete(client);
        }

        @Override
        public void clientFinished(HttpPageBufferClient client)
        {
            ExchangeClient.this.clientFinished(client);
        }

        @Override
        public void clientFailed(HttpPageBufferClient client, Throwable cause)
        {
            requireNonNull(client, "client is null");
            requireNonNull(cause, "cause is null");
            ExchangeClient.this.clientFailed(cause);
        }
    }

    private static void closeQuietly(HttpPageBufferClient client)
    {
        try {
            client.close();
        }
        catch (RuntimeException e) {
            // ignored
        }
    }
}
