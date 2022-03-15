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
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.execution.Lifespan;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.snapshot.Spillable;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.spiller.SingleStreamSpiller;
import io.prestosql.spiller.SingleStreamSpillerFactory;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.prestosql.SystemSessionProperties.isInnerJoinSpillFilteringEnabled;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

// Snapshot: Most of these fields are immutable objects, excpet for:
// - spilledLookupSourceHandle: content is only changed after index is fully built
// - spillInProgress: must be "done" when markers are received (see needsInput)
// - unspillInProgress: unspill can only happen after "finish", so no marker can be received after that
// - lookupSourceSupplier: only becomes non-null after "finish"
// - lookupSourceChecksum: only used when unspilling lookupSourceSupplier
// - finishMemoryRevoke: must be empty, because new input (including markers) can't be added until finishMemoryRevoke is called
@ThreadSafe
@RestorableConfig(uncapturedFields = {"lookupSourceFactory", "lookupSourceFactoryDestroyed", "outputChannels",
        "hashChannels", "filterFunctionFactory", "sortChannel", "searchFunctionFactories", "singleStreamSpillerFactory",
        "lookupSourceNotNeeded", "spilledLookupSourceHandle", "spillInProgress", "unspillInProgress", "lookupSourceSupplier", "lookupSourceChecksum",
        "finishMemoryRevoke", "snapshotState", "lastMarker"})
public class HashBuilderOperator
        implements SinkOperator, Spillable
{
    public static class HashBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;
        private final List<Integer> outputChannels;
        private final List<Integer> hashChannels;
        private final OptionalInt preComputedHashChannel;
        private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
        private final Optional<Integer> sortChannel;
        private final List<JoinFilterFunctionFactory> searchFunctionFactories;
        private final PagesIndex.Factory pagesIndexFactory;

        private final int expectedPositions;
        private final boolean spillEnabled;
        private final SingleStreamSpillerFactory singleStreamSpillerFactory;

        private final Map<Lifespan, Integer> partitionIndexManager = new HashMap<>();

        private boolean closed;

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager,
                List<Integer> outputChannels,
                List<Integer> hashChannels,
                OptionalInt preComputedHashChannel,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                Optional<Integer> sortChannel,
                List<JoinFilterFunctionFactory> searchFunctionFactories,
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                SingleStreamSpillerFactory singleStreamSpillerFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(sortChannel, "sortChannel can not be null");
            requireNonNull(searchFunctionFactories, "searchFunctionFactories is null");
            checkArgument(sortChannel.isPresent() != searchFunctionFactories.isEmpty(), "both or none sortChannel and searchFunctionFactories must be set");
            this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager, "lookupSourceFactoryManager is null");

            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
            this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
            this.sortChannel = sortChannel;
            this.searchFunctionFactories = ImmutableList.copyOf(searchFunctionFactories);
            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.spillEnabled = spillEnabled;
            this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");

            this.expectedPositions = expectedPositions;
        }

        @Override
        public HashBuilderOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuilderOperator.class.getSimpleName());

            PartitionedLookupSourceFactory partitionedLookupSourceFactory = this.lookupSourceFactoryManager.getJoinBridge(driverContext.getLifespan());
            int incrementPartitionIndex = getAndIncrementPartitionIndex(driverContext.getLifespan());
            // Snapshot: make driver ID and source/partition index the same, to ensure consistency before and after resuming.
            // LocalExchangeSourceOperator also uses the same mechanism to ensure consistency.
            if (addOperatorContext.isSnapshotEnabled()) {
                incrementPartitionIndex = driverContext.getDriverId();
            }
            verify(incrementPartitionIndex < partitionedLookupSourceFactory.partitions());
            return new HashBuilderOperator(
                    addOperatorContext,
                    partitionedLookupSourceFactory,
                    incrementPartitionIndex,
                    outputChannels,
                    hashChannels,
                    preComputedHashChannel,
                    filterFunctionFactory,
                    sortChannel,
                    searchFunctionFactories,
                    expectedPositions,
                    pagesIndexFactory,
                    spillEnabled,
                    singleStreamSpillerFactory);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash build can not be duplicated");
        }

        private int getAndIncrementPartitionIndex(Lifespan lifespan)
        {
            return partitionIndexManager.compute(lifespan, (k, v) -> v == null ? 1 : v + 1) - 1;
        }
    }

    @VisibleForTesting
    public enum State
    {
        /**
         * Operator accepts input
         */
        CONSUMING_INPUT,

        /**
         * Memory revoking occurred during {@link #CONSUMING_INPUT}. Operator accepts input and spills it
         */
        SPILLING_INPUT,

        /**
         * LookupSource has been built and passed on without any spill occurring
         */
        LOOKUP_SOURCE_BUILT,

        /**
         * Input has been finished and spilled
         */
        INPUT_SPILLED,

        /**
         * Spilled input is being unspilled
         */
        INPUT_UNSPILLING,

        /**
         * Spilled input has been unspilled, LookupSource built from it
         */
        INPUT_UNSPILLED_AND_BUILT,

        /**
         * No longer needed
         */
        CLOSED
    }

    private static final double INDEX_COMPACTION_ON_REVOCATION_TARGET = 0.8;

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;
    private final LocalMemoryContext localRevocableMemoryContext;
    //TODO-cp-I2DSGR: Shared field
    private final PartitionedLookupSourceFactory lookupSourceFactory;
    private final ListenableFuture<?> lookupSourceFactoryDestroyed;
    private final int partitionIndex;

    private final List<Integer> outputChannels;
    private final List<Integer> hashChannels;
    private final OptionalInt preComputedHashChannel;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
    private final Optional<Integer> sortChannel;
    private final List<JoinFilterFunctionFactory> searchFunctionFactories;

    private final PagesIndex index;

    private final boolean spillEnabled;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;

    private final HashCollisionsCounter hashCollisionsCounter;

    private State state = State.CONSUMING_INPUT;
    private Optional<ListenableFuture<?>> lookupSourceNotNeeded = Optional.empty();
    private final SpilledLookupSourceHandle spilledLookupSourceHandle;
    private Optional<SingleStreamSpiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private Optional<ListenableFuture<List<Page>>> unspillInProgress = Optional.empty();
    @Nullable
    private LookupSourceSupplier lookupSourceSupplier;
    private OptionalLong lookupSourceChecksum = OptionalLong.empty();

    private Optional<Runnable> finishMemoryRevoke = Optional.empty();
    private SpilledBlooms spillBloom;
    private final long expectedValues;

    private final SingleInputSnapshotState snapshotState;
    // Snapshot: special logic for taking an extra snapshot for HashBuilder operator, for outer joins.
    // LookupOuterOperator uses matched indices to determine which rows on the build side to send out.
    // When LookupOuterOperator captures its state, it captures the above indices. But when the query resumes from a snapshot,
    // pages received by HashBuilderOperator may be in a different order from the initial run.
    // This means rows may occupy different indices after the resume, which invalidates what LookupOuterOperator captured.
    // Note that when LookupOuterOperator captures the indices, the snapshot id must be *after* the last snapshot taken by the HashBuilderOperator.
    // To overcome this problem, an additional snapshot is taken of the HashBuilderOperator, when the operator finishes.
    // This snapshot contains all the rows, so when it's restored, their indices will stay the same.
    // When query resumes to a snapshot with non-empty indices captured by LookupOuterOperator, this final snapshot of HashBuilderOperator is used,
    // ensuring that what's inside the hash and the restored "matched positions" have consistent indices.
    // Note that when this extra snapshot is loaded, operators preceding the HashBuilderOperator are still at an earlier snapshot,
    // and may try to send pages the HashBuilderOperator. These pages must be discarded because HashBuilderOperator already has them.
    // The lastSnapshotId is used to determine what id to use for the final snapshot.
    private MarkerPage lastMarker;
    // If this flag is true, then it was restored from the extra snapshot, and should discard incoming pages.
    private boolean alreadyFinished;

    public HashBuilderOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            OptionalInt preComputedHashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            SingleStreamSpillerFactory singleStreamSpillerFactory)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;
        this.filterFunctionFactory = filterFunctionFactory;
        this.sortChannel = sortChannel;
        this.searchFunctionFactories = searchFunctionFactories;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.localRevocableMemoryContext = operatorContext.localRevocableMemoryContext();

        this.index = pagesIndexFactory.newPagesIndex(lookupSourceFactory.getTypes(), expectedPositions);
        this.lookupSourceFactory = lookupSourceFactory;
        lookupSourceFactoryDestroyed = lookupSourceFactory.isDestroyed();

        this.outputChannels = outputChannels;
        this.hashChannels = hashChannels;
        this.preComputedHashChannel = preComputedHashChannel;

        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);

        this.spillEnabled = spillEnabled;
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;
        this.expectedValues = expectedPositions * 10;
        if (preComputedHashChannel.isPresent() && spillEnabled && isInnerJoinSpillFilteringEnabled(operatorContext.getDriverContext().getSession())) {
            this.spillBloom = new SpilledBlooms();
        }
        else {
            this.spillBloom = null;
        }

        spilledLookupSourceHandle = new SpilledLookupSourceHandle(spillBloom);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @VisibleForTesting
    public State getState()
    {
        return state;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        switch (state) {
            case CONSUMING_INPUT:
                return NOT_BLOCKED;

            case SPILLING_INPUT:
                return spillInProgress;

            case LOOKUP_SOURCE_BUILT:
                return lookupSourceNotNeeded.orElseThrow(() -> new IllegalStateException("Lookup source built, but disposal future not set"));

            case INPUT_SPILLED:
                return spilledLookupSourceHandle.getUnspillingOrDisposeRequested();

            case INPUT_UNSPILLING:
                return unspillInProgress.orElseThrow(() -> new IllegalStateException("Unspilling in progress, but unspilling future not set"));

            case INPUT_UNSPILLED_AND_BUILT:
                return spilledLookupSourceHandle.getDisposeRequested();

            case CLOSED:
                return NOT_BLOCKED;
        }
        throw new IllegalStateException("Unhandled state: " + state);
    }

    @Override
    public boolean needsInput()
    {
        boolean stateNeedsInput = (state == State.CONSUMING_INPUT)
                || (state == State.SPILLING_INPUT && spillInProgress.isDone());

        return stateNeedsInput && !lookupSourceFactoryDestroyed.isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                // Remember the last snapshot id, so we know what to use for the final snapshot
                lastMarker = (MarkerPage) page;
                return;
            }
            if (alreadyFinished) {
                // We already have all the pages. Ignore additional ones.
                return;
            }
        }

        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        if (state == State.SPILLING_INPUT) {
            spillInput(page);
            return;
        }

        checkState(state == State.CONSUMING_INPUT);
        updateIndex(page);
    }

    private void updateIndex(Page page)
    {
        index.addPage(page);

        if (spillEnabled) {
            localRevocableMemoryContext.setBytes(index.getEstimatedSize().toBytes());
        }
        else {
            if (!localUserMemoryContext.trySetBytes(index.getEstimatedSize().toBytes())) {
                index.compact();
                localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
            }
        }
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    private void spillInput(Page page)
    {
        checkState(spillInProgress.isDone(), "Previous spill still in progress");
        checkSuccess(spillInProgress, "spilling failed");
        updateBloom(page);
        spillInProgress = getSpiller().spill(page);
    }

    private void updateBloom(Page page)
    {
        if (spillBloom == null) {
            return;
        }

        int hashChannel = preComputedHashChannel.getAsInt();
        Block hashes = page.getBlock(hashChannel);
        for (int i = 0; i < page.getPositionCount(); i++) {
            //Todo make generic: spillBloom.put(type.get(hashes, i));
            spillBloom.put(BigintType.BIGINT.getLong(hashes, i));
        }
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        checkState(spillEnabled, "Spill not enabled, no revokable memory should be reserved");

        if (state == State.CONSUMING_INPUT) {
            long indexSizeBeforeCompaction = index.getEstimatedSize().toBytes();
            index.compact();
            long indexSizeAfterCompaction = index.getEstimatedSize().toBytes();
            if (indexSizeAfterCompaction < indexSizeBeforeCompaction * INDEX_COMPACTION_ON_REVOCATION_TARGET) {
                finishMemoryRevoke = Optional.of(() -> {});
                return immediateFuture(null);
            }

            finishMemoryRevoke = Optional.of(() -> {
                index.clear();
                localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
                localRevocableMemoryContext.setBytes(0);
                lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
                state = State.SPILLING_INPUT;
            });
            return spillIndex();
        }
        if (state == State.LOOKUP_SOURCE_BUILT) {
            finishMemoryRevoke = Optional.of(() -> {
                if (spillBloom != null) {
                    spillBloom.markReady();
                }
                lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
                lookupSourceNotNeeded = Optional.empty();
                index.clear();
                localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
                localRevocableMemoryContext.setBytes(0);
                lookupSourceChecksum = OptionalLong.of(lookupSourceSupplier.checksum());
                lookupSourceSupplier = null;
                state = State.INPUT_SPILLED;
            });
            return spillIndex();
        }
        if (operatorContext.getReservedRevocableBytes() == 0) {
            // Probably stale revoking request
            finishMemoryRevoke = Optional.of(() -> {});
            return immediateFuture(null);
        }

        throw new IllegalStateException(format("State %s can not have revocable memory, but has %s revocable bytes", state, operatorContext.getReservedRevocableBytes()));
    }

    private ListenableFuture<?> spillIndex()
    {
        checkState(!spiller.isPresent(), "Spiller already created");
        spiller = Optional.of(singleStreamSpillerFactory.create(
                index.getTypes(),
                operatorContext.getSpillContext().newLocalSpillContext(),
                operatorContext.newLocalSystemMemoryContext(HashBuilderOperator.class.getSimpleName())));
        return getSpiller().spill(new AbstractIterator<Page>()
        {
            private Iterator<Page> spillPages = index.getPages();

            @Override
            protected Page computeNext()
            {
                if (!spillPages.hasNext()) {
                    return endOfData();
                }
                Page page = spillPages.next();
                updateBloom(page);
                return page;
            }
        });
    }

    @Override
    public void finishMemoryRevoke()
    {
        checkState(finishMemoryRevoke.isPresent(), "Cannot finish unknown revoking");
        finishMemoryRevoke.get().run();
        finishMemoryRevoke = Optional.empty();
    }

    @Override
    public void finish()
    {
        if (snapshotState != null && !alreadyFinished && lastMarker != null) {
            // Update alreadyFinished field *before* calling captureState
            alreadyFinished = true;
            // There should have been at least one marker, either resume or snapshot.
            // In case there isn't, then this must be the initial run but marker was never received,
            // so no snapshot wil be marked complete. In this case there's no need to produce the final snapshot.
            if (lastMarker.isResuming()) {
                // If it was a resume marker, then we don't know what the "next" snapshot should be.
                // This means we can't use the resumed snapshot for "backtrack".
                // This is guaranteed by QuerySnapshotManager#queryRestoreComplete().
            }
            else {
                // Take a final snapshot of this operator
                snapshotState.captureExtraState(lastMarker.getSnapshotId() + 1);
            }
        }
        doFinish();
    }

    private void doFinish()
    {
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        if (finishMemoryRevoke.isPresent()) {
            return;
        }

        switch (state) {
            case CONSUMING_INPUT:
                finishInput();
                return;

            case LOOKUP_SOURCE_BUILT:
                disposeLookupSourceIfRequested();
                return;

            case SPILLING_INPUT:
                finishSpilledInput();
                return;

            case INPUT_SPILLED:
                if (spilledLookupSourceHandle.getDisposeRequested().isDone()) {
                    close();
                }
                else {
                    unspillLookupSourceIfRequested();
                }
                return;

            case INPUT_UNSPILLING:
                finishLookupSourceUnspilling();
                return;

            case INPUT_UNSPILLED_AND_BUILT:
                disposeUnspilledLookupSourceIfRequested();
                return;

            case CLOSED:
                // no-op
                return;
        }

        throw new IllegalStateException("Unhandled state: " + state);
    }

    private void finishInput()
    {
        checkState(state == State.CONSUMING_INPUT);
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        LookupSourceSupplier partition = buildLookupSource();
        if (spillEnabled) {
            localRevocableMemoryContext.setBytes(partition.get().getInMemorySizeInBytes());
        }
        else {
            localUserMemoryContext.setBytes(partition.get().getInMemorySizeInBytes());
        }
        lookupSourceNotNeeded = Optional.of(lookupSourceFactory.lendPartitionLookupSource(partitionIndex, partition));

        state = State.LOOKUP_SOURCE_BUILT;
    }

    private void disposeLookupSourceIfRequested()
    {
        checkState(state == State.LOOKUP_SOURCE_BUILT);
        verify(lookupSourceNotNeeded.isPresent());
        if (!lookupSourceNotNeeded.get().isDone()) {
            return;
        }

        index.clear();
        localRevocableMemoryContext.setBytes(0);
        localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());
        lookupSourceSupplier = null;
        close();
    }

    private void finishSpilledInput()
    {
        checkState(state == State.SPILLING_INPUT);
        if (!spillInProgress.isDone()) {
            // Not ready to handle finish() yet
            return;
        }
        checkSuccess(spillInProgress, "spilling failed");
        if (spillBloom != null) {
            spillBloom.markReady();
        }
        state = State.INPUT_SPILLED;
    }

    private void unspillLookupSourceIfRequested()
    {
        checkState(state == State.INPUT_SPILLED);
        if (!spilledLookupSourceHandle.getUnspillingRequested().isDone()) {
            // Nothing to do yet.
            return;
        }

        verify(spiller.isPresent());
        verify(!unspillInProgress.isPresent());

        localUserMemoryContext.setBytes(getSpiller().getSpilledPagesInMemorySize() + index.getEstimatedSize().toBytes());
        unspillInProgress = Optional.of(getSpiller().getAllSpilledPages());

        state = State.INPUT_UNSPILLING;
    }

    private void finishLookupSourceUnspilling()
    {
        checkState(state == State.INPUT_UNSPILLING);
        if (!unspillInProgress.get().isDone()) {
            // Pages have not be unspilled yet.
            return;
        }

        // Use Queue so that Pages already consumed by Index are not retained by us.
        Queue<Page> pages = new ArrayDeque<>(getDone(unspillInProgress.get()));
        long memoryRetainedByRemainingPages = pages.stream()
                .mapToLong(Page::getRetainedSizeInBytes)
                .sum();
        localUserMemoryContext.setBytes(memoryRetainedByRemainingPages + index.getEstimatedSize().toBytes());

        while (!pages.isEmpty()) {
            Page next = pages.remove();
            index.addPage(next);
            // There is no attempt to compact index, since unspilled pages are unlikely to have blocks with retained size > logical size.
            memoryRetainedByRemainingPages -= next.getRetainedSizeInBytes();
            localUserMemoryContext.setBytes(memoryRetainedByRemainingPages + index.getEstimatedSize().toBytes());
        }

        LookupSourceSupplier partition = buildLookupSource();
        lookupSourceChecksum.ifPresent(checksum ->
                checkState(partition.checksum() == checksum, "Unspilled lookupSource checksum does not match original one"));
        localUserMemoryContext.setBytes(partition.get().getInMemorySizeInBytes());

        spilledLookupSourceHandle.setLookupSource(partition);

        state = State.INPUT_UNSPILLED_AND_BUILT;
    }

    private void disposeUnspilledLookupSourceIfRequested()
    {
        checkState(state == State.INPUT_UNSPILLED_AND_BUILT);
        if (!spilledLookupSourceHandle.getDisposeRequested().isDone()) {
            return;
        }

        index.clear();
        localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes());

        close();
        spilledLookupSourceHandle.setDisposeCompleted();
    }

    private LookupSourceSupplier buildLookupSource()
    {
        LookupSourceSupplier partition = index.createLookupSourceSupplier(operatorContext.getSession(), hashChannels, preComputedHashChannel, filterFunctionFactory, sortChannel, searchFunctionFactories, Optional.of(outputChannels));
        hashCollisionsCounter.recordHashCollision(partition.getHashCollisions(), partition.getExpectedHashCollisions());
        checkState(lookupSourceSupplier == null, "lookupSourceSupplier is already set");
        this.lookupSourceSupplier = partition;
        return partition;
    }

    @Override
    public boolean isFinished()
    {
        if (lookupSourceFactoryDestroyed.isDone()) {
            // Finish early when the probe side is empty
            close();
            return true;
        }

        return state == State.CLOSED;
    }

    private SingleStreamSpiller getSpiller()
    {
        return spiller.orElseThrow(() -> new IllegalStateException("Spiller not created"));
    }

    @Override
    public void close()
    {
        if (state == State.CLOSED) {
            return;
        }
        // close() can be called in any state, due for example to query failure, and must clean resource up unconditionally

        lookupSourceSupplier = null;
        state = State.CLOSED;
        finishMemoryRevoke = finishMemoryRevoke.map(ifPresent -> () -> {});

        try (Closer closer = Closer.create()) {
            closer.register(index::clear);
            spiller.ifPresent(closer::register);
            closer.register(() -> localUserMemoryContext.setBytes(0));
            closer.register(() -> localRevocableMemoryContext.setBytes(0));
            closer.register(() -> {
                if (snapshotState != null) {
                    snapshotState.close();
                }
            });
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isSpilled()
    {
        return state == State.SPILLING_INPUT || state == State.INPUT_SPILLED || state == State.INPUT_UNSPILLING;
    }

    @Override
    public List<Path> getSpilledFilePaths()
    {
        if (isSpilled()) {
            return ImmutableList.of(spiller.get().getFile());
        }
        return ImmutableList.of();
    }

    public class SpilledBlooms
    {
        AtomicBoolean isReady = new AtomicBoolean(false);
        List<BloomFilter> blooms = new ArrayList<>(Arrays.asList(new BloomFilter(expectedValues, 0.01)));
        int counter;

        public void put(long value)
        {
            int current = blooms.size() - 1;
            if (counter >= expectedValues) {
                blooms.add(new BloomFilter(expectedValues, 0.01));
                counter = 0;
            }

            blooms.get(current).add(value);
            counter++;
        }

        public boolean mightContain(long value)
        {
            if (!isReady.get()) {
                return true;
            }

            return blooms.stream().anyMatch(bf -> bf.test(value));
        }

        public void markReady()
        {
            isReady.compareAndSet(false, true);
        }

        public Object capture()
        {
            SpillBloomState myState = new SpillBloomState();
            myState.counter = counter;
            myState.isReady = isReady.get();

            for (BloomFilter bf : blooms) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                try {
                    bf.writeTo(bos);
                    myState.blooms.add(bos.toByteArray());
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return myState;
        }

        public void restore(Object state)
        {
            SpillBloomState myState = (SpillBloomState) state;
            this.isReady.set(myState.isReady);
            this.counter = myState.counter;

            blooms.clear();
            for (Object obj : myState.blooms) {
                ByteArrayInputStream bis = new ByteArrayInputStream((byte[]) obj);
                try {
                    blooms.add(BloomFilter.readFrom(bis));
                    bis.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        private class SpillBloomState
                implements Serializable
        {
            boolean isReady;
            int counter;
            List<Object> blooms = new ArrayList<>();
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        HashBuilderOperatorState myState = new HashBuilderOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.localUserMemoryContext = localUserMemoryContext.getBytes();
        myState.localRevocableMemoryContext = localRevocableMemoryContext.getBytes();
        myState.index = index.capture(serdeProvider);
        myState.hashCollisionsCounter = hashCollisionsCounter.capture(serdeProvider);
        myState.state = state.toString();
        myState.alreadyFinished = alreadyFinished;

        // Capture spill related to spill
        if (spiller.isPresent()) {
            myState.spiller = spiller.get().capture(serdeProvider);
        }

        if (spillBloom != null) {
            myState.spillBloom = spillBloom.capture();
        }
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        HashBuilderOperatorState myState = (HashBuilderOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.localUserMemoryContext.setBytes(myState.localUserMemoryContext);
        this.localRevocableMemoryContext.setBytes(myState.localRevocableMemoryContext);

        this.index.restore(myState.index, serdeProvider);

        this.hashCollisionsCounter.restore(myState.hashCollisionsCounter, serdeProvider);
        State oldState = this.state;
        this.state = State.valueOf(myState.state);
        this.alreadyFinished = myState.alreadyFinished;

        // Restore spill related fields
        if (myState.spiller != null) {
            if (!spiller.isPresent()) {
                spiller = Optional.of(singleStreamSpillerFactory.create(
                        index.getTypes(),
                        operatorContext.getSpillContext().newLocalSpillContext(),
                        operatorContext.newLocalSystemMemoryContext(HashBuilderOperator.class.getSimpleName())));
            }
            this.spiller.get().restore(myState.spiller, serdeProvider);
        }
        if (myState.spillBloom != null) {
            this.spillBloom.restore(myState.spillBloom);
        }
        if (oldState == State.CONSUMING_INPUT && this.state == State.SPILLING_INPUT) {
            lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
        }
    }

    @Override
    public boolean supportsConsolidatedWrites()
    {
        return false;
    }

    private static class HashBuilderOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private long localUserMemoryContext;
        private long localRevocableMemoryContext;

        private Object index;

        private Object hashCollisionsCounter;
        private String state;
        private boolean alreadyFinished;
        private Object spiller;

        private Object spillBloom;
    }
}
