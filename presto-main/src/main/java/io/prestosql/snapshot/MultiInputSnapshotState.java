/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.snapshot;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import io.airlift.log.Logger;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.MarkerPage;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;

/**
 * This is a utility class used by Restorable objects that receive inputs from multiple channels, e.g. ExchangeOperator.
 * When a new input is expected to be received, the Restorable calls the processPage() or processPages() method
 * to perform snapshot related processing. The method returns page(s) that the Restorable can proceed to handle.
 * This utility handles storing and loading states behind the scenes. If stored state includes unprocessed pages,
 * the above methods return them after a resuming marker is received.
 * <p>
 * Specially, these methods process pages following these rules:
 * - If there are pending pages, from restored state, then those are returned first.
 * - If there are no pending pages, then the provided page supplier is consulted.
 * - The page supplier may return null, in which case this method returns null as well.
 * - If the page supplier returns a marker page:
 * -- If the page is not associated with an input channel (no origin), then it's a broadcast marker. It's returned to the caller.
 * -- Otherwise if this is the first marker page for a snapshot id, then return it
 * -- Otherwise it is a subsequent marker page for a known snapshot id, then return null
 * - A snapshot marker triggers state-capture and store process
 * - A resume marker triggers state-load and restore process
 * <p>
 * TODO-cp-I2D63E: can simplify this class, by removing "serialized page" pieces, after PageProducer/PageConsumer changes are ready
 */
public class MultiInputSnapshotState
{
    private static final Logger LOG = Logger.get(MultiInputSnapshotState.class);

    private final MultiInputRestorable restorable;
    private final String restorableId; // Only used for logs
    private final TaskSnapshotManager snapshotManager;
    private final PagesSerde pagesSerde;
    // For a given snapshot, generate a unique id, used to locate the saved state of this restorable object for that snapshot
    // For an operator, this is typically /query-id/snapshot-id/stage-id/task-id/pipeline-id/driver-id/operator-id
    // For a task component, this is typically /query-id/snapshot-id/stage-id/task-id/component-id
    private final Function<Long, SnapshotStateId> snapshotStateIdGenerator;
    // All input channels, if known
    private Optional<Set<String>> inputChannels;

    // List of states for active snapshots being taken or resumed
    private final List<SnapshotState> states = new ArrayList<>();
    // When the Restorable restores its state, there may be unprocessed pages that were received after the marker page.
    // Store them here. They are processed _before_ any new pages the Restorable may receive from its data source.
    private Iterator<?> pendingPages;

    // Markers that have been returned to the caller. Maintain a copy for callers that don't process these markers right away,
    // and need a place to store them for later access. The "nextMarker" method polls this list.
    private final List<Page> pendingMarkers = new ArrayList<>();

    /**
     * Construct a utility instance for an operator that has multiple input channels
     */
    public static MultiInputSnapshotState forOperator(MultiInputRestorable restorable, OperatorContext operatorContext)
    {
        return new MultiInputSnapshotState(restorable,
                operatorContext.getDriverContext().getPipelineContext().getTaskContext().getSnapshotManager(),
                operatorContext.getDriverContext().getSerde(),
                snapshotId -> SnapshotStateId.forOperator(snapshotId, operatorContext));
    }

    /**
     * Construct a utility instance for a Restorable object in the "Task" scope, e.g. OutputBuffer.
     * A SnapshotStateId generator is required to generate a unique id for stored states.
     */
    public static MultiInputSnapshotState forTaskComponent(
            MultiInputRestorable restorable,
            TaskContext taskContext,
            Function<Long, SnapshotStateId> snapshotStateIdGenerator)
    {
        return new MultiInputSnapshotState(restorable,
                taskContext.getSnapshotManager(),
                taskContext.getSerdeFactory().createPagesSerde(),
                snapshotStateIdGenerator);
    }

    /**
     * Construct a utility instance with required dependencies
     *
     * @param restorable Restorable object with multiple input channels
     * @param snapshotManager Used to store and retrieve snapshot states
     * @param serde Used to convert between pages and serialized pages
     * @param snapshotStateIdGenerator Used to generate unique id for snapshot states
     */
    public MultiInputSnapshotState(
            MultiInputRestorable restorable,
            TaskSnapshotManager snapshotManager,
            PagesSerde serde,
            Function<Long, SnapshotStateId> snapshotStateIdGenerator)
    {
        this.restorable = restorable;
        this.restorableId = String.format("%s (%s)", restorable.getClass().getSimpleName(), snapshotStateIdGenerator.apply(0L).getId());
        this.snapshotManager = snapshotManager;
        this.pagesSerde = serde;
        this.snapshotStateIdGenerator = snapshotStateIdGenerator;
    }

    /**
     * Retrieve the next page to process. See class documentation for details
     */
    public Optional<Page> processPage(Supplier<Pair<Page, String>> pageSupplier)
    {
        return Optional.ofNullable(processPage(pageSupplier, false, false));
    }

    /**
     * Retrieve the next page to process. See class documentation for details
     */
    public Optional<SerializedPage> processSerializedPage(Supplier<Pair<SerializedPage, String>> pageSupplier)
    {
        return Optional.ofNullable(processPage(pageSupplier, true, false));
    }

    /**
     * Retrieve the next marker to process
     */
    public Optional<Page> nextMarker(Supplier<Pair<Page, String>> pageSupplier)
    {
        return Optional.ofNullable(processPage(pageSupplier, false, true));
    }

    /**
     * Retrieve the next marker to process
     */
    public Optional<SerializedPage> nextSerializedMarker(Supplier<Pair<SerializedPage, String>> pageSupplier)
    {
        return Optional.ofNullable(processPage(pageSupplier, true, true));
    }

    /**
     * Retrieve all available pages. See class documentation for details
     */
    public List<Page> processPages(List<Page> pages, String origin)
    {
        Iterator<Page> iterator = pages.iterator();
        Supplier<Pair<Page, String>> pairSupplier = () -> Pair.of(iterator.hasNext() ? iterator.next() : null, origin);
        return processPages(pairSupplier, iterator::hasNext, false);
    }

    /**
     * Retrieve all available pages. See class documentation for details
     */
    public List<SerializedPage> processSerializedPages(List<SerializedPage> pages, String origin)
    {
        Iterator<SerializedPage> iterator = pages.iterator();
        Supplier<Pair<SerializedPage, String>> pairSupplier = () -> Pair.of(iterator.hasNext() ? iterator.next() : null, origin);
        return processPages(pairSupplier, iterator::hasNext, true);
    }

    private <T> List<T> processPages(Supplier<Pair<T, String>> pageSupplier, Supplier<Boolean> hasMore, boolean serialized)
    {
        List<T> ret = new ArrayList<>();
        T page;
        do {
            page = processPage(pageSupplier, serialized, false);
            if (page != null) {
                ret.add(page);
            }
        }
        while (page != null || hasMore.get());
        return ret;
    }

    private <T> T processPage(Supplier<Pair<T, String>> pageSupplier, boolean serialized, boolean markerOnly)
    {
        if (markerOnly && pendingPages != null && pendingPages.hasNext()) {
            // Pending pages are not markers
            return null;
        }

        // Check pending pages first
        SerializedPage serializedPage = pollPendingPage();
        Page page = null;

        if (serializedPage == null) {
            // Get a new input
            Pair<T, String> pair = pageSupplier.get();
            T pageInTransition = pair.getLeft();
            String origin = pair.getRight();
            if (pageInTransition == null) {
                return null;
            }

            Optional<String> channel;
            if (pageInTransition instanceof SerializedPage) {
                serializedPage = (SerializedPage) pageInTransition;
                channel = Optional.ofNullable(origin);
                page = pagesSerde.deserialize(serializedPage);
            }
            else {
                page = (Page) pageInTransition;
                channel = Optional.ofNullable(origin);
            }

            // Not a restored pending page, so must be a new input that may need snapshot related processing
            if (processNewPage(channel, page)) {
                // Some markers are not passed on
                return null;
            }

            if (markerOnly) {
                if (page instanceof MarkerPage) {
                    // If new page is marker, then return it; otherwise store it in pending list and return null
                    if (serializedPage != null) {
                        return (T) pagesSerde.serialize(page);
                    }
                    else {
                        return (T) page;
                    }
                }
                pendingPages = Iterators.singletonIterator(pagesSerde.serialize(page));
                return null;
            }
        }
        else {
            page = pagesSerde.deserialize(serializedPage);
        }

        // Prepare for correct output type
        if (serialized) {
            return (T) pagesSerde.serialize(page);
        }
        else {
            return (T) page;
        }
    }

    private boolean processNewPage(Optional<String> channel, Page page)
    {
        MarkerPage marker = null;
        if (page instanceof MarkerPage) {
            marker = (MarkerPage) page;
        }

        if (!channel.isPresent()) {
            checkState(marker != null);
            LOG.debug("Sending marker '%s' directly to target '%s'", marker.toString(), restorableId);
            // This is a marker added directly to the page queue.
            // It is only received once and will not come from any of the known input channels.
            // The caller is responsible for ensuring this marker is added at the right time,
            // relative to other pages from input channels.
            // Because of this order guarantee, no additional marker page processing is needed:
            // - The page queue serves as a transmission channel, so no need to snapshot or resume it
            // - The marker is only received once, so no need for multi-input processing.
            // hence the marker will be returned without any further processing
            return false;
        }

        if (marker != null) {
            LOG.debug("Received marker '%s' from source '%s' to target '%s'", marker.toString(), channel.get(), restorableId);
            if (marker.isResuming()) {
                boolean ret = resume(channel.get(), marker);
                if (!ret) {
                    LOG.debug("Sending resume marker '%s' from source '%s' to target '%s'", marker.toString(), channel.get(), restorableId);
                }
                return ret;
            }
            else {
                boolean ret = processMarker(channel.get(), marker);
                if (!ret) {
                    LOG.debug("Sending marker '%s' from source '%s' to target '%s'", marker.toString(), channel.get(), restorableId);
                }
                return ret;
            }
        }
        else {
            return processInput(channel.get(), page);
        }
    }

    private SnapshotState snapshotStateById(MarkerPage marker)
    {
        for (SnapshotState snapshot : states) {
            if (snapshot.snapshotId == marker.getSnapshotId() && snapshot.resuming == marker.isResuming()) {
                return snapshot;
            }
        }
        return null;
    }

    private boolean resume(String channel, MarkerPage marker)
    {
        boolean ret = false;

        long snapshotId = marker.getSnapshotId();
        SnapshotState snapshot = snapshotStateById(marker);
        SnapshotStateId componentId = snapshotStateIdGenerator.apply(snapshotId);
        if (snapshot == null) {
            // There won't be 2 resuming attempts for different snapshot ids happening at the same time.
            // Currently it's also not possible to have 2 attempts with the same snapshot id, but different attempt ids,
            // but the code below supports this scenario.
            // First time a marker is received for a snapshot. Clear pending markers and restore Restorable states.
            states.clear();
            try {
                Optional<Object> storedState;
                if (restorable.supportsConsolidatedWrites()) {
                    storedState = snapshotManager.loadConsolidatedState(componentId);
                }
                else {
                    storedState = snapshotManager.loadState(componentId);
                }
                if (!storedState.isPresent()) {
                    snapshotManager.failedToRestore(componentId, true);
                    LOG.warn("Can't locate saved state for snapshot %d, component %s", snapshotId, restorableId);
                }
                else if (storedState.get() == TaskSnapshotManager.NO_STATE) {
                    snapshotManager.failedToRestore(componentId, true);
                    LOG.error("BUG! State of component %s has never been stored successfully before snapshot %d", restorableId, snapshotId);
                }
                else {
                    Stopwatch timer = Stopwatch.createStarted();
                    List<?> storedStates = (List<?>) storedState.get();
                    pendingPages = storedStates.listIterator();
                    restorable.restore(pendingPages.next(), pagesSerde);
                    LOG.debug("Successfully restored state to snapshot %d for %s", snapshotId, restorableId);
                    timer.stop();
                    snapshotManager.succeededToRestore(componentId, timer.elapsed(TimeUnit.MILLISECONDS));
                }
            }
            catch (Exception e) {
                LOG.warn(e, "Failed to restore snapshot state for %s: %s", componentId, e.getMessage());
                snapshotManager.failedToRestore(componentId, false);
            }

            snapshot = new SnapshotState(marker);
            states.add(snapshot);
            pendingMarkers.add(marker);
        }
        else {
            // Seen marker with same snapshot id. Don't need to pass on this marker.
            ret = true;
        }

        // Received marker from this channel, so remove from pending list
        if (!snapshot.markedChannels.add(channel)) {
            String message = String.format("Received duplicate marker '%s' from source '%s' to target '%s'", marker.toString(), channel, restorableId);
            LOG.error(message);
        }

        inputChannels = restorable.getInputChannels();
        if (inputChannels.isPresent()) {
            checkState(inputChannels.get().containsAll(snapshot.markedChannels));
            if (inputChannels.get().size() == snapshot.markedChannels.size()) {
                checkState(states.get(0) == snapshot, "resume state should be the first one");
                // Received marker from all channels. Operator state is complete.
                states.remove(0);
            }
        }
        return ret;
    }

    private boolean processMarker(String channel, MarkerPage marker)
    {
        boolean ret = false;

        long snapshotId = marker.getSnapshotId();
        SnapshotState snapshot = snapshotStateById(marker);
        if (snapshot == null) {
            // First time a marker is received for a snapshot. Store operator state and make marker available.
            snapshot = new SnapshotState(marker);
            pendingMarkers.add(marker);
            try {
                snapshot.addState(restorable, pagesSerde);
            }
            catch (Exception e) {
                LOG.warn(e, "Failed to capture and store snapshot state");
                snapshotManager.failedToCapture(snapshotStateIdGenerator.apply(snapshotId));
            }
            states.add(snapshot);
        }
        else {
            // Seen marker with same snapshot id. Don't need to pass on this marker.
            ret = true;
        }

        // No longer need to capture inputs on this channel
        if (!snapshot.markedChannels.add(channel)) {
            String message = String.format("Received duplicate marker '%s' from source '%s' to target '%s'", marker.toString(), channel, restorableId);
            LOG.error(message);
            return true;
        }

        inputChannels = restorable.getInputChannels();
        if (inputChannels.isPresent()) {
            checkState(inputChannels.get().containsAll(snapshot.markedChannels));
            if (inputChannels.get().size() == snapshot.markedChannels.size()) {
                // Received marker from all channels. Operator state is complete.
                SnapshotStateId componentId = snapshotStateIdGenerator.apply(snapshotId);
                try {
                    if (restorable.supportsConsolidatedWrites()) {
                        snapshotManager.storeConsolidatedState(componentId, snapshot.states, snapshot.serTime);
                    }
                    else {
                        snapshotManager.storeState(componentId, snapshot.states, snapshot.serTime);
                    }
                    snapshotManager.succeededToCapture(componentId);
                    LOG.debug("Successfully saved state to snapshot %d for %s", snapshotId, restorableId);
                }
                catch (Exception e) {
                    LOG.warn(e, "Failed to capture and store snapshot state");
                    snapshotManager.failedToCapture(componentId);
                }
                int index = states.indexOf(snapshot);
                states.remove(index);
                for (int i = 0; i < index; i++) {
                    // All previous pending snapshots can't be complete
                    SnapshotState failedState = states.remove(0);
                    componentId = snapshotStateIdGenerator.apply(failedState.snapshotId);
                    if (failedState.resuming) {
                        snapshotManager.failedToRestore(componentId, false);
                    }
                    else {
                        snapshotManager.failedToCapture(componentId);
                    }
                }
            }
        }

        return ret;
    }

    private boolean processInput(String channel, Page page)
    {
        Object channelSnapshot = null;
        for (SnapshotState snapshot : states) {
            // For all pending snapshots that have not received marker from this channel,
            // need to capture the input as part of channel state.
            if (!snapshot.markedChannels.contains(channel)) {
                snapshot.addInputState(page, pagesSerde);
            }
        }

        return false;
    }

    public boolean hasPendingPages()
    {
        return hasPendingDataPages() || !pendingMarkers.isEmpty();
    }

    public boolean hasPendingDataPages()
    {
        return pendingPages != null && pendingPages.hasNext();
    }

    private SerializedPage pollPendingPage()
    {
        if (pendingPages != null) {
            if (pendingPages.hasNext()) {
                Object state = pendingPages.next();
                if (state instanceof SerializedPage) {
                    // If pending page comes from calling "nextMarker", then it's a SerializedPage
                    return (SerializedPage) state;
                }
                return SerializedPage.restoreSerializedPage(state);
            }
            pendingPages = null;
        }
        return null;
    }

    public MarkerPage nextMarker()
    {
        if (pendingMarkers.isEmpty()) {
            return null;
        }
        return (MarkerPage) pendingMarkers.remove(0);
    }

    private static class SnapshotState
    {
        private final long snapshotId;
        private final boolean resuming;
        // Which channels have received the marker
        private final Set<String> markedChannels;
        // First entry is snapshot of the operator, followed by inputs from various channels as SerializedPage instances.
        // Inputs contain information about channels, so no need to distinguish and store them per-channel.
        private final List<Object> states = new ArrayList<>();
        // Consolidated time taken to serialize the state
        private long serTime;

        private SnapshotState(MarkerPage marker)
        {
            this.snapshotId = marker.getSnapshotId();
            this.resuming = marker.isResuming();
            this.markedChannels = new HashSet<>();
            this.serTime = 0;
        }

        public void addState(MultiInputRestorable restorable, PagesSerde pagesSerde)
        {
            Stopwatch timer = Stopwatch.createStarted();
            Object state = restorable.capture(pagesSerde);
            timer.stop();
            serTime += timer.elapsed(TimeUnit.MILLISECONDS);
            states.add(state);
        }

        public void addInputState(Page page, PagesSerde pagesSerde)
        {
            Stopwatch timer = Stopwatch.createStarted();
            Object channelSnapshot = pagesSerde.serialize(page).capture(pagesSerde);
            timer.stop();
            serTime += timer.elapsed(TimeUnit.MILLISECONDS);
            states.add(channelSnapshot);
        }
    }
}
