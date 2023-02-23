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
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.GroupJoinProbe.GroupJoinProbeFactory;
import io.prestosql.operator.LookupJoinOperator.SpillInfoSnapshot;
import io.prestosql.operator.LookupJoinOperators.JoinType;
import io.prestosql.operator.PartitionedConsumption.Partition;
import io.prestosql.operator.aggregation.builder.AggregationBuilder;
import io.prestosql.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.prestosql.operator.aggregation.builder.InMemoryHashAggregationBuilderWithReset;
import io.prestosql.operator.exchange.LocalPartitionGenerator;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.PartitioningSpiller;
import io.prestosql.spiller.PartitioningSpillerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.prestosql.SystemSessionProperties.isInnerJoinSpillFilteringEnabled;
import static io.prestosql.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static io.prestosql.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

public class LookupGroupJoinOperator
        implements Operator
{
    @VisibleForTesting
    public enum State
    {
        /**
         * Operator accepts input
         */
        CONSUMING_INPUT,

        /**
         * All inputs accepted, finishing aggregations
         */
        AGGR_FINISHING,

        /**
         * Aggregation on input finished
         */
        AGGR_FINISHED,

        /**
         * LookupSource has been built and passed on without any spill occurring
         */
        SOURCE_BUILT,

        /**
         * No longer needed
         */
        CLOSED
    }

    private final OperatorContext operatorContext;

    private final List<Type> probeTypes;
    private final List<Symbol> probeFinalOutputSymbols;
    private final List<Integer> probeFinalOutputChannels;
    private final List<Integer> buildFinalOutputChannels;
    private final GroupJoinProbeFactory joinProbeFactory;
    private final Runnable afterClose;

    private final PartitioningSpillerFactory partitioningSpillerFactory;

    private Runnable afterMemOpFinish;
    private final OptionalInt lookupJoinsCount;
    private final HashGenerator hashGenerator;
    private final LookupSourceFactory lookupSourceFactory;

    private final JoinStatisticsCounter statisticsCounter;

    private final LookupGroupJoinPageBuilder pageBuilder;

    private final boolean probeOnOuterSide;
    private final boolean spillBypassEnabled;

    private final ListenableFuture<LookupSourceProvider> lookupSourceProviderFuture;
    private LookupSourceProvider lookupSourceProvider;
    private GroupJoinProbe probe;

    private Page outputPage;

    private Optional<PartitioningSpiller> spiller = Optional.empty();
    private Optional<LocalPartitionGenerator> partitionGenerator = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private long inputPageSpillEpoch;
    private boolean closed;
    private boolean finishing;
    private boolean unspilling;
    private boolean isSpillerRestored;
    private boolean finished;
    private long joinPosition = -1;
    private int joinSourcePositions;

    private boolean currentProbePositionProducedRow;

    @Nullable
    private ListenableFuture<PartitionedConsumption<Supplier<LookupSource>>> partitionedConsumption;
    @Nullable
    private Iterator<Partition<Supplier<LookupSource>>> lookupPartitions;
    private Optional<Partition<Supplier<LookupSource>>> currentPartition = Optional.empty();
    private Optional<ListenableFuture<Supplier<LookupSource>>> unspilledLookupSource = Optional.empty();
    private Iterator<Page> unspilledInputPages = emptyIterator();
    private Iterator<Page> unspilledMemoryPartitions = emptyIterator();
    private Map<Integer, Iterator<Page>> backUpUnspilledMemoryPartitions = new HashMap<>();
    private Map<Integer, Page> backUpRestoredPages = new HashMap<>();
    private Integer restoredPartition;
    private final GroupJoinAggregator aggregator;
    private final GroupJoinAggregator aggrOnAggregator;

    private final SingleInputSnapshotState snapshotState;
    private boolean isSingleSessionSpiller;
    private List<Integer> spilledPartitionsList = new ArrayList<>();

    protected AggregationBuilder aggregationBuilder;
    protected AggregationBuilder probeAggregationBuilder;
    protected AggregationBuilder buildAggregationBuilder;
    protected LocalMemoryContext memoryContext;

    private final HashCollisionsCounter hashCollisionsCounter;

    protected boolean aggregationFinishing;
    protected long numberOfInputRowsProcessed;
    protected long numberOfUniqueRowsProduced;
    protected Work<?> unfinishedWork;
    protected boolean aggregationInputProcessed;
    private final boolean spillEnabled = false;
    private boolean aggregationFinished;
    protected WorkProcessor<Page> outputPages;
    protected State state = State.CONSUMING_INPUT;

    public LookupGroupJoinOperator(
            OperatorContext operatorContext,
            boolean forked,
            List<Type> probeTypes,
            List<Type> outputTypes,
            List<Type> buildTypes,
            JoinType joinType,
            LookupSourceFactory lookupSourceFactory,
            GroupJoinProbeFactory joinProbeFactory,
            Runnable afterClose,
            OptionalInt lookupJoinsCount,
            HashGenerator hashGenerator,
            PartitioningSpillerFactory partitioningSpillerFactory,
            Runnable afterMemOpFinish,
            boolean isSingleSessionSpiller,
            GroupJoinAggregator aggregator,
            GroupJoinAggregator aggrOnAggregator,
            List<Symbol> probeFinalOutputSymbols,
            List<Integer> probeFinalOutputChannels,
            List<Integer> buildFinalOutputChannels)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));

        requireNonNull(joinType, "joinType is null");
        // Cannot use switch case here, because javac will synthesize an inner class and cause IllegalAccessError
        probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;
        spillBypassEnabled = probeOnOuterSide || !isInnerJoinSpillFilteringEnabled(operatorContext.getDriverContext().getSession());

        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");
        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        this.lookupSourceProviderFuture = lookupSourceFactory.createLookupSourceProvider();

        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        operatorContext.setInfoSupplier(this.statisticsCounter);

        this.pageBuilder = new LookupGroupJoinPageBuilder(outputTypes, buildTypes, buildFinalOutputChannels, probeFinalOutputChannels);
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;

        this.afterMemOpFinish = afterMemOpFinish;
        this.isSingleSessionSpiller = isSingleSessionSpiller;

        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);

        this.memoryContext = operatorContext.localUserMemoryContext();
        if (aggregator.isUseSystemMemory()) {
            this.memoryContext = operatorContext.localSystemMemoryContext();
        }

        this.aggregator = aggregator;
        this.aggrOnAggregator = aggrOnAggregator;
        this.probeFinalOutputSymbols = probeFinalOutputSymbols;
        this.probeFinalOutputChannels = probeFinalOutputChannels;
        this.buildFinalOutputChannels = buildFinalOutputChannels;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (finishing) {
            return;
        }
        finishing = true;
        aggregationFinishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finishedNow = this.finishing && this.finished && probe == null && pageBuilder.isEmpty() && outputPage == null;
        // TODO Vineet Check the aggregation related pending work
        // if finishedNow drop references so memory is freed early
        if (finishedNow) {
            close();
        }
        return finishedNow;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (finishing) {
            return NOT_BLOCKED;
        }

        return lookupSourceProviderFuture;
    }

    @Override
    public boolean needsInput()
    {
        // TODO Vineet check aggregation related conditions
        if (!finishing && state == State.CONSUMING_INPUT) {
            if (aggregationFinishing || outputPages != null) {
                return false;
            }
            else if (aggregationBuilder != null && aggregationBuilder.isFull()) {
                return false;
            }
            else {
                // TODO Vineet Need to move this out of needsInput and need to make it light weight.
                if (unfinishedWork != null) {
                    boolean workDone = unfinishedWork.process();
                    aggregationBuilder.updateMemory();
                    if (!workDone) {
                        return false;
                    }
                    unfinishedWork = null;
                }
                return true;
            }
        }
        else {
            return allowMarker2()
                    && lookupSourceProviderFuture.isDone();
        }
    }

    public boolean allowMarker2()
    {
        return !finishing
                && probe == null
                && outputPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        addInput(page, false);
    }

    private void addInput(Page page, boolean isRestoredPage)
    {
        requireNonNull(page, "page is null");
        checkState(probe == null, "Current page has not been completely processed yet");

        checkState(tryFetchLookupSourceProvider(), "Not ready to handle input yet");
        // create Aggregators and pass page to them for process
        checkState(!aggregationFinishing, "Operator is already finishing");
        aggregationInputProcessed = true;

        if (aggregationBuilder == null) {
            createAggregationBuilder();
        }
        else {
            checkState(!aggregationBuilder.isFull(), "Aggregation buffer is full");
        }

        // process the current page; save the unfinished work if we are waiting for memory
        unfinishedWork = aggregationBuilder.processPage(page);
        if (unfinishedWork.process()) {
            unfinishedWork = null;
            // TODO Vineet check if can index this pages here.
        }
        aggregationBuilder.updateMemory();
        numberOfInputRowsProcessed += page.getPositionCount();
    }

    private void createProbe(Page page)
    {
        // create probe
        if (buildAggregationBuilder == null) {
            LookupSource lookupSource = lookupSourceProvider.withLease((lookupSourceLease -> lookupSourceLease.getLookupSource()));
            buildAggregationBuilder = lookupSource.getAggregationBuilder().duplicate();
        }
        probe = joinProbeFactory.createGroupJoinProbe(page/*newPage*/, false/*isSpilled*/, lookupSourceProvider, probeAggregationBuilder, buildAggregationBuilder);

        // initialize to invalid join position to force output code to advance the cursors
        joinPosition = -1;
    }

    private boolean tryFetchLookupSourceProvider()
    {
        if (lookupSourceProvider == null) {
            if (!lookupSourceProviderFuture.isDone()) {
                return false;
            }
            lookupSourceProvider = requireNonNull(getDone(lookupSourceProviderFuture));
            statisticsCounter.updateLookupSourcePositions(lookupSourceProvider.withLease(lookupSourceLease -> lookupSourceLease.getLookupSource().getJoinPositionCount()));
        }
        return true;
    }

    private void finishAggregation()
    {
        Page page = processAggregation();
        if (page != null) {
            // TODO Vineet may need to cache these pages and also check the condition for state change.
            createProbe(page);
        }
    }

    @Override
    public Page getOutput()
    {
        switch (state) {
            case CONSUMING_INPUT:
                if (probe == null) {
                    finishAggregation();
                }
                break;
            case SOURCE_BUILT:
                break;
            case CLOSED:
                // no-op
                return null;
        }

        if (probe == null && pageBuilder.isEmpty() && !finishing) {
            return null;
        }

        if (!tryFetchLookupSourceProvider()) {
            if (!finishing) {
                return null;
            }

            verify(finishing);
            // We are no longer interested in the build side (the lookupSourceProviderFuture's value).
            addSuccessCallback(lookupSourceProviderFuture, LookupSourceProvider::close);
            lookupSourceProvider = new StaticLookupSourceProvider(new EmptyLookupSource());
        }

        if (probe == null && finishing && unfinishedWork == null && !unspilling) {
            /*
             * We do not have input probe and we won't have any, as we're finishing.
             * Let LookupSourceFactory know LookupSources can be disposed as far as we're concerned.
             */
            verify(partitionedConsumption == null, "partitioned consumption already started");
            lookupSourceProvider.close();
            partitionedConsumption = lookupSourceFactory.finishProbeOperator(lookupJoinsCount);
            afterMemOpFinish.run();
            afterMemOpFinish = () -> {};
            unspilling = true;
            finished = true;
        }

        if (probe != null) {
            processProbe();
        }

        if (outputPage != null) {
            verify(pageBuilder.isEmpty());
            Page output = outputPage;
            outputPage = null;
            return output;
        }

        // It is impossible to have probe == null && !pageBuilder.isEmpty(),
        // because we will flush a page whenever we reach the probe end
        verify(probe != null || pageBuilder.isEmpty());
        return null;
    }

    protected boolean hasOrderBy()
    {
        return aggregator.hasOrderBy();
    }

    protected boolean hasDistinct()
    {
        return aggregator.hasDistinct();
    }

    public void createAggregationBuilder()
    {
        if (aggregator.getStep().isOutputPartial() || !spillEnabled || hasOrderBy() || hasDistinct()) {
            aggregationBuilder = new InMemoryHashAggregationBuilder(
                    aggregator.getAccumulatorFactories(),
                    aggregator.getStep(),
                    aggregator.getExpectedGroups(),
                    aggregator.getGroupByTypes(),
                    aggregator.getGroupByChannels(),
                    aggregator.getHashChannel(),
                    operatorContext,
                    aggregator.getMaxPartialMemory(),
                    aggregator.getJoinCompiler(),
                    () -> {
                        memoryContext.setBytes(((InMemoryHashAggregationBuilder) aggregationBuilder).getSizeInMemory());
                        if (aggregator.getStep().isOutputPartial() && aggregator.getMaxPartialMemory().isPresent()) {
                            // do not yield on memory for partial aggregations
                            return true;
                        }
                        return operatorContext.isWaitingForMemory().isDone();
                    });
            probeAggregationBuilder = new InMemoryHashAggregationBuilderWithReset(
                    aggrOnAggregator.getAccumulatorFactories(),
                    aggrOnAggregator.getStep(),
                    aggrOnAggregator.getExpectedGroups(),
                    aggrOnAggregator.getGroupByTypes(),
                    aggrOnAggregator.getGroupByChannels(),
                    aggrOnAggregator.getHashChannel(),
                    operatorContext,
                    aggrOnAggregator.getMaxPartialMemory(),
                    aggrOnAggregator.getJoinCompiler(),
                    () -> {
                        memoryContext.setBytes(((InMemoryHashAggregationBuilder) probeAggregationBuilder).getSizeInMemory());
                        if (aggrOnAggregator.getStep().isOutputPartial() && aggrOnAggregator.getMaxPartialMemory().isPresent()) {
                            // do not yield on memory for partial aggregations
                            return true;
                        }
                        return operatorContext.isWaitingForMemory().isDone();
                    });
        }
        else {
            throw new UnsupportedOperationException("Not Supported");
        }
    }

    public Page processAggregation()
    {
        if (aggregationFinished) {
            return null;
        }

        // process unfinished work if one exists
        if (unfinishedWork != null) {
            boolean workDone = unfinishedWork.process();
            aggregationBuilder.updateMemory();
            if (!workDone) {
                return null;
            }
            unfinishedWork = null;
        }

        if (outputPages == null) {
            if (aggregationFinishing) {
                if (!aggregationInputProcessed && aggregator.isProduceDefaultOutput()) {
                    // global aggregations always generate an output row with the default aggregation output (e.g. 0 for COUNT, NULL for SUM)
                    aggregationFinished = true;
                    state = State.SOURCE_BUILT;
                    return aggregator.getGlobalAggregationOutput();
                }

                if (aggregationBuilder == null) {
                    aggregationFinished = true;
                    state = State.SOURCE_BUILT;
                    return null;
                }
            }

            // only flush if we are finishing or the aggregation builder is full
            if (!aggregationFinishing && (aggregationBuilder == null || !aggregationBuilder.isFull())) {
                return null;
            }

            outputPages = aggregationBuilder.buildResult();
        }

        if (!outputPages.process()) {
            return null;
        }

        if (outputPages.isFinished()) {
            closeAggregationBuilder();
            return null;
        }

        Page result = outputPages.getResult();
        numberOfUniqueRowsProduced += result.getPositionCount();
        return result;
    }

    protected void closeAggregationBuilder()
    {
        outputPages = null;
        if (aggregationBuilder != null) {
            aggregationBuilder.recordHashCollisions(hashCollisionsCounter);
            aggregationBuilder.close();
            // aggregationBuilder.close() will release all memory reserved in memory accounting.
            // The reference must be set to null afterwards to avoid unaccounted memory.
            aggregationBuilder = null;
        }
        //memoryContext.setBytes(0);
        aggregator.getPartialAggregationController().ifPresent(
                controller -> controller.onFlush(numberOfInputRowsProcessed, numberOfUniqueRowsProduced));
        numberOfInputRowsProcessed = 0;
        numberOfUniqueRowsProduced = 0;
    }

    protected void closeProbeAggrOnAggregationBuilder()
    {
        if (probeAggregationBuilder != null) {
            probeAggregationBuilder.recordHashCollisions(hashCollisionsCounter);
            probeAggregationBuilder.close();
            // aggregationBuilder.close() will release all memory reserved in memory accounting.
            // The reference must be set to null afterwards to avoid unaccounted memory.
            probeAggregationBuilder = null;
        }
        //memoryContext.setBytes(0);
        /*aggrOnAggregator.getPartialAggregationController().ifPresent(
                controller -> controller.onFlush(numberOfInputRowsProcessed, numberOfUniqueRowsProduced));
        numberOfInputRowsProcessed = 0;
        numberOfUniqueRowsProduced = 0;*/
    }

    protected void closeBuildAggrOnAggregationBuilder()
    {
        if (buildAggregationBuilder != null) {
            buildAggregationBuilder.recordHashCollisions(hashCollisionsCounter);
            buildAggregationBuilder.close();
            // aggregationBuilder.close() will release all memory reserved in memory accounting.
            // The reference must be set to null afterwards to avoid unaccounted memory.
            buildAggregationBuilder = null;
        }
        //memoryContext.setBytes(0);
        /*aggrOnAggregator.getPartialAggregationController().ifPresent(
                controller -> controller.onFlush(numberOfInputRowsProcessed, numberOfUniqueRowsProduced));
        numberOfInputRowsProcessed = 0;
        numberOfUniqueRowsProduced = 0;*/
    }

    private void processProbe()
    {
        verify(probe != null);
        Optional<?> value = lookupSourceProvider.withLease(lookupSourceLease -> {
            if (lookupSourceLease.spillEpoch() == inputPageSpillEpoch) {
                // Spill state didn't change, so process as usual.
                processProbe(lookupSourceLease.getLookupSource());
                return Optional.empty();
            }

            return Optional.of(SpillInfoSnapshot.from(lookupSourceLease));
        });
        if (!value.isPresent()) {
            return;
        }
        long joinPositionWithinPartition;
        if (joinPosition >= 0) {
            joinPositionWithinPartition = lookupSourceProvider.withLease(lookupSourceLease -> lookupSourceLease.getLookupSource().joinPositionWithinPartition(joinPosition));
        }
        else {
            joinPositionWithinPartition = -1;
        }
        if (probe == null) {
            return;
        }

        Page currentPage = probe.getPage();
        int currentPosition = probe.getPosition();
        long currentJoinPosition = this.joinPosition;
        boolean probePositionProducedRow = this.currentProbePositionProducedRow;

        clearProbe();

        if (currentPosition < 0) {
            // Processing of the page hasn't been started yet.
            createProbe(currentPage);
        }
        else {
            Page remaining = pageTail(currentPage, currentPosition);
            restoreProbe(remaining, currentJoinPosition, probePositionProducedRow, joinSourcePositions);
        }
    }

    private void processProbe(LookupSource lookupSource)
    {
        verify(probe != null);

        DriverYieldSignal yieldSignal = operatorContext.getDriverContext().getYieldSignal();
        while (!yieldSignal.isSet()) {
            if (probe.getPosition() >= 0) {
                if (!joinCurrentPosition(lookupSource, yieldSignal)) {
                    break;
                }
                if (!currentProbePositionProducedRow) {
                    currentProbePositionProducedRow = true;
                }
            }
            currentProbePositionProducedRow = false;
            if (!advanceProbePosition(lookupSource)) {
                break;
            }
            statisticsCounter.recordProbe(joinSourcePositions);
            joinSourcePositions = 0;
        }
    }

    private void restoreProbe(Page probePage, long joinPosition, boolean currentProbePositionProducedRow, int joinSourcePositions)
    {
        verify(probe == null);
        createProbe(probePage);
        if (probe != null) {
            verify(probe.advanceNextPosition());
        }
        this.joinPosition = joinPosition;
        this.currentProbePositionProducedRow = currentProbePositionProducedRow;
        this.joinSourcePositions = joinSourcePositions;
    }

    private Page pageTail(Page currentPage, int startAtPosition)
    {
        verify(currentPage.getPositionCount() - startAtPosition >= 0);
        return currentPage.getRegion(startAtPosition, currentPage.getPositionCount() - startAtPosition);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        probe = null;
        if (state == State.CONSUMING_INPUT) {
            closeAggregationBuilder();
        }

        try (Closer closer = Closer.create()) {
            // `afterClose` must be run last.
            // Closer is documented to mimic try-with-resource, which implies close will happen in reverse order.
            closer.register(afterMemOpFinish::run);
            closer.register(afterClose::run);

            closer.register(pageBuilder::reset);
            closer.register(() -> Optional.ofNullable(lookupSourceProvider).ifPresent(LookupSourceProvider::close));
            closer.register(() -> {
                if (snapshotState != null) {
                    snapshotState.close();
                }
            });
            spiller.ifPresent(closer::register);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        closeProbeAggrOnAggregationBuilder();
        closeBuildAggrOnAggregationBuilder();
        memoryContext.setBytes(0);
    }

    /**
     * Produce rows matching join condition for the current probe position. If this method was called previously
     * for the current probe position, calling this again will produce rows that wasn't been produced in previous
     * invocations.
     *
     * @return true if all eligible rows have been produced; false otherwise
     */
    private boolean joinCurrentPosition(LookupSource lookupSource, DriverYieldSignal yieldSignal)
    {
        // while we have a position on lookup side to join against...
        while (joinPosition >= 0) {
            if (lookupSource.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
                currentProbePositionProducedRow = true;

                // Build count * Probe Rec, probe count * Build Rec, then add to Output Page Builder
                pageBuilder.appendRow(probe, lookupSource, joinPosition);
                joinSourcePositions++;
            }

            // get next position on lookup side for this probe row
            joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());

            if (yieldSignal.isSet() || tryBuildPage()) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return whether there are more positions on probe side
     */
    private boolean advanceProbePosition(LookupSource lookupSource)
    {
        if (!probe.advanceNextPosition()) {
            clearProbe();
            return false;
        }

        // update join position
        joinPosition = probe.getCurrentJoinPosition(lookupSource);
        return true;
    }

    private boolean tryBuildPage()
    {
        if (pageBuilder.isFull()) {
            buildPage();
            return true;
        }
        return false;
    }

    private void buildPage()
    {
        verify(outputPage == null);
        verify(probe != null);

        if (pageBuilder.isEmpty()) {
            return;
        }

        outputPage = pageBuilder.build(probe);
        pageBuilder.reset();
    }

    private void clearProbe()
    {
        // Before updating the probe flush the current page
        buildPage();
        probe = null;
    }

    @Override
    public boolean supportsConsolidatedWrites()
    {
        return false;
    }
}
