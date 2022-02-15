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
package io.prestosql.operator.exchange;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.snapshot.MultiInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.MarkerPage;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class LocalExchangeSource
{
    private static final SettableFuture<?> NOT_EMPTY;

    static {
        NOT_EMPTY = SettableFuture.create();
        NOT_EMPTY.set(null);
    }

    // Snapshot: all local-sinks that send markers to this local-source
    // Should include all sinks for local-exchange operator; but only 1 sink for local-merge, because of pass-through exchanger
    private final Set<String> inputChannels = Sets.newConcurrentHashSet();

    private final Consumer<LocalExchangeSource> onFinish;

    private final Queue<PageReference> buffer = new LinkedList<>();
    // originBuffer mirrors buffer, but keeps track of the origin of each page
    private final Queue<Optional<String>> originBuffer = new LinkedList<>();
    private final AtomicLong bufferedBytes = new AtomicLong();

    private final Object lock = new Object();

    @GuardedBy("lock")
    private SettableFuture<?> notEmptyFuture = NOT_EMPTY;

    @GuardedBy("lock")
    private boolean finishing;

    // Only set for local-merge, to record markers.
    // Markers may not reach the local-merge operator because it may be blocked on other channels.
    private final MultiInputSnapshotState snapshotState;

    public LocalExchangeSource(Consumer<LocalExchangeSource> onFinish, MultiInputSnapshotState snapshotState)
    {
        this.onFinish = requireNonNull(onFinish, "onFinish is null");
        this.snapshotState = snapshotState;
    }

    public LocalExchangeBufferInfo getBufferInfo()
    {
        // This must be lock free to assure task info creation is fast
        // Note: the stats my be internally inconsistent
        return new LocalExchangeBufferInfo(bufferedBytes.get(), buffer.size());
    }

    void addInputChannel(String inputChannel)
    {
        inputChannels.add(inputChannel);
    }

    Set<String> getAllInputChannels()
    {
        return Collections.unmodifiableSet(inputChannels);
    }

    void addPage(PageReference pageReference, String origin)
    {
        checkNotHoldsLock();

        boolean added = false;
        SettableFuture<?> notEmptySettableFuture;
        synchronized (lock) {
            // ignore pages after finish
            if (!finishing) {
                if (snapshotState != null) {
                    // For local-merge
                    Page page;
                    synchronized (snapshotState) {
                        // This may look suspicious, in that if there are "pending pages" in the snapshot state, then
                        // a) those pages were associated with specific input channels (exchange source/sink) when the state
                        // was captured, but now they would be returned to any channel asking for the next page, and
                        // b) when the pending page is returned, the current page (in pageReference) is discarded and lost.
                        // But the above never happens because "merge" operators are always preceded by OrderByOperators,
                        // which only send data pages at the end, *after* all markers. That means when snapshot is taken,
                        // no data page has been received, so when the snapshot is restored, there won't be any pending pages.
                        page = snapshotState.processPage(() -> Pair.of(pageReference.peekPage(), origin)).orElse(null);
                    }
                    //if new input page is marker, we don't add it to buffer, it will be obtained through MultiInputSnapshotState's getPendingMarker()
                    if (page instanceof MarkerPage || page == null) {
                        pageReference.removePage();
                        // whenever local exchange source sees a marker page, it's going to check whether operator after local merge is blocked by it.
                        // if it is, this local exchange source will unblock in order for next operator to ask for output to pass down the marker.
                        if (!this.notEmptyFuture.isDone()) {
                            notEmptySettableFuture = this.notEmptyFuture;
                            this.notEmptyFuture = NOT_EMPTY;
                            notEmptySettableFuture.set(null);
                        }
                        return;
                    }
                }
                // buffered bytes must be updated before adding to the buffer to assure
                // the count does not go negative
                bufferedBytes.addAndGet(pageReference.getRetainedSizeInBytes());
                buffer.add(pageReference);
                originBuffer.add(Optional.ofNullable(origin));
                added = true;
            }

            // we just added a page (or we are finishing) so we are not empty
            notEmptySettableFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }

        if (!added) {
            // dereference the page outside of lock
            pageReference.removePage();
        }

        // notify readers outside of lock since this may result in a callback
        notEmptySettableFuture.set(null);
    }

    public WorkProcessor<Page> pages()
    {
        return WorkProcessor.create(
                new WorkProcessor.Process<Page>()
                {
                    @Override
                    public ProcessState<Page> process()
                    {
                        Pair<Page, String> pair = removePage();
                        // all work has already been done with origin, so the right element can be ignored
                        Page page = pair.getLeft();

                        if (page == null) {
                            if (isFinished()) {
                                return ProcessState.finished();
                            }

                            ListenableFuture<?> blocked = waitForReading();
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
                    public Object captureResult(@NotNull Page result, BlockEncodingSerdeProvider serdeProvider)
                    {
                        SerializedPage serializedPage = ((PagesSerde) serdeProvider).serialize(result);
                        return serializedPage.capture(serdeProvider);
                    }

                    @Override
                    public Page restoreResult(Object resultState, BlockEncodingSerdeProvider serdeProvider)
                    {
                        checkState(resultState != null);
                        SerializedPage serializedPage = SerializedPage.restoreSerializedPage(resultState);
                        return ((PagesSerde) serdeProvider).deserialize(serializedPage);
                    }
                });
    }

    public Pair<Page, String> removePage()
    {
        checkNotHoldsLock();

        PageReference pageReference;
        Optional<String> origin;
        // NOTE: need a lock to poll from the buffers. Lock not needed when updating buffered bytes,
        // as it is not expected to be consistent with the buffer (only best effort).
        synchronized (lock) {
            pageReference = buffer.poll();
            origin = originBuffer.poll();
        }
        if (pageReference == null) {
            return Pair.of(null, null);
        }
        checkState(origin != null, "pageReference is not null, but origin is");

        // dereference the page outside of lock, since may trigger a callback
        Page page = pageReference.removePage();
        bufferedBytes.addAndGet(-page.getRetainedSizeInBytes());

        checkFinished();

        return Pair.of(page, origin.orElse(null));
    }

    public ListenableFuture<?> waitForReading()
    {
        checkNotHoldsLock();

        synchronized (lock) {
            // if we need to block readers, and the current future is complete, create a new one
            if (!finishing && buffer.isEmpty() && notEmptyFuture.isDone()) {
                notEmptyFuture = SettableFuture.create();
            }
            return notEmptyFuture;
        }
    }

    public boolean isFinished()
    {
        synchronized (lock) {
            return finishing && buffer.isEmpty();
        }
    }

    public void finish()
    {
        checkNotHoldsLock();

        SettableFuture<?> notEmptySettableFuture;
        synchronized (lock) {
            if (finishing) {
                return;
            }
            finishing = true;

            notEmptySettableFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }

        // notify readers outside of lock since this may result in a callback
        notEmptySettableFuture.set(null);

        checkFinished();
    }

    public void close()
    {
        checkNotHoldsLock();

        List<PageReference> remainingPages;
        SettableFuture<?> notEmptySettableFuture;
        synchronized (lock) {
            finishing = true;

            remainingPages = new ArrayList<>(buffer);
            buffer.clear();
            originBuffer.clear();
            bufferedBytes.addAndGet(-remainingPages.stream().mapToLong(PageReference::getRetainedSizeInBytes).sum());

            notEmptySettableFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }

        // free all the remaining pages
        remainingPages.forEach(PageReference::removePage);

        // notify readers outside of lock since this may result in a callback
        notEmptySettableFuture.set(null);

        // this will always fire the finished event
        checkState(isFinished(), "Expected buffer to be finished");
        checkFinished();
    }

    private void checkFinished()
    {
        checkNotHoldsLock();

        if (isFinished()) {
            // notify finish listener outside of lock, since it may make a callback
            // NOTE: due the race in this method, the onFinish may be called multiple times
            // it is expected that the implementer handles this (which is why this source
            // is passed to the function)
            onFinish.accept(this);
        }
    }

    private void checkNotHoldsLock()
    {
        checkState(!Thread.holdsLock(lock), "Can not execute this method while holding the lock");
    }
}
