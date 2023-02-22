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
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.aggregation.builder.AggregationBuilder;
import io.prestosql.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.prestosql.operator.aggregation.builder.InMemoryHashAggregationBuilderWithReset;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class HashBuilderGroupJoinOperator
        implements SinkOperator
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
        LOOKUP_SOURCE_BUILT,

        /**
         * No longer needed
         */
        CLOSED
    }

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
    private final Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory;
    private final Optional<Integer> sortChannel;
    private final Optional<Integer> countChannel;
    private final List<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> searchFunctionFactories;
    private final PagesIndex index;
    private final boolean spillEnabled;
    private final HashCollisionsCounter hashCollisionsCounter;
    private State state = State.CONSUMING_INPUT;
    private Optional<ListenableFuture<?>> lookupSourceNotNeeded = Optional.empty();
    @Nullable
    private LookupSourceSupplier lookupSourceSupplier;
    private OptionalLong lookupSourceChecksum = OptionalLong.empty();

    private Optional<Runnable> finishMemoryRevoke = Optional.empty();
    private final long expectedValues;
    private boolean spillToHdfsEnabled;

    protected AggregationBuilder aggregationBuilder;
    protected AggregationBuilder aggrOnAggregationBuilder;
    protected LocalMemoryContext memoryContext;
    protected WorkProcessor<Page> outputPages;

    /*protected SettableFuture<?> aggrFinishInProgress;*/
    //protected boolean aggregationFinishing;
    //private boolean aggregationFinished;
    //protected boolean aggregationInputProcessed;

    // for yield when memory is not available
    protected Work<?> unfinishedAggrWork;
    protected long numberOfInputRowsProcessed;
    protected long numberOfUniqueRowsProduced;
    private final GroupJoinAggregator aggregator;
    private final GroupJoinAggregator aggrOnAggregator;

    private final List<Symbol> buildFinalOutputSymbols;
    private final List<Integer> buildFinalOutputChannels;

    public HashBuilderGroupJoinOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            OptionalInt preComputedHashChannel,
            Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            Optional<Integer> countChannel,
            List<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> searchFunctionFactories,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            boolean spillToHdfsEnabled,
            GroupJoinAggregator aggregator,
            GroupJoinAggregator aggrOnAggregator,
            List<Symbol> buildFinalOutputSymbols,
            List<Integer> buildFinalOutputChannels,
            ListeningExecutorService executor)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;
        this.filterFunctionFactory = filterFunctionFactory;
        this.sortChannel = sortChannel;
        this.countChannel = countChannel;
        this.searchFunctionFactories = searchFunctionFactories;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.localRevocableMemoryContext = operatorContext.localRevocableMemoryContext();

        this.index = pagesIndexFactory.newPagesIndex(lookupSourceFactory.getTypes(), expectedPositions);
        this.lookupSourceFactory = lookupSourceFactory;
        this.lookupSourceFactoryDestroyed = lookupSourceFactory.isDestroyed();

        this.outputChannels = outputChannels;
        this.hashChannels = hashChannels;
        this.preComputedHashChannel = preComputedHashChannel;

        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);
        this.spillEnabled = spillEnabled;
        this.spillToHdfsEnabled = spillToHdfsEnabled;
        this.expectedValues = expectedPositions * 10L;

        /*this.aggrFinishInProgress = SettableFuture.create();
        this.aggrFinishInProgress.set(null);*/

        requireNonNull(operatorContext, "operatorContext is null");
        this.memoryContext = operatorContext.localUserMemoryContext();
        if (aggregator.isUseSystemMemory()) {
            this.memoryContext = operatorContext.localSystemMemoryContext();
        }

        this.aggregator = aggregator;
        this.aggrOnAggregator = aggrOnAggregator;
        this.buildFinalOutputSymbols = buildFinalOutputSymbols;
        this.buildFinalOutputChannels = buildFinalOutputChannels;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        if (state == State.CONSUMING_INPUT) {
            if (/*aggregationFinishing || */outputPages != null) {
                return false;
            }
            else if (aggregationBuilder != null && aggregationBuilder.isFull()) {
                return false;
            }
            else if (lookupSourceFactoryDestroyed.isDone()/*aggrFinishInProgress.isDone()*/) {
                return false;
            }
            else {
                // TODO Vineet Need to move this out of needsInput and need to make it light weight.
                if (unfinishedAggrWork != null) {
                    boolean workDone = unfinishedAggrWork.process();
                    aggregationBuilder.updateMemory();
                    if (!workDone) {
                        return false;
                    }
                    unfinishedAggrWork = null;
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        checkState(unfinishedAggrWork == null, "Operator has unfinished work");
        //checkState(!aggregationFinishing, "Operator is already finishing");
        checkState(state == State.CONSUMING_INPUT, "Operator is not in Consuming Input state");
        /*aggregationInputProcessed = true;*/

        if (aggregationBuilder == null) {
            createAggregationBuilder();
        }
        else {
            checkState(!aggregationBuilder.isFull(), "Aggregation buffer is full");
        }

        // process the current page; save the unfinished work if we are waiting for memory
        unfinishedAggrWork = aggregationBuilder.processPage(page);
        if (unfinishedAggrWork.process()) {
            unfinishedAggrWork = null;
        }
        aggregationBuilder.updateMemory();
        numberOfInputRowsProcessed += page.getPositionCount();
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
            // TODO: We ignore spillEnabled here if any aggregate has ORDER BY clause or DISTINCT because they are not yet implemented for spilling.
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
            aggrOnAggregationBuilder = new InMemoryHashAggregationBuilderWithReset(
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
                        memoryContext.setBytes(((InMemoryHashAggregationBuilder) aggrOnAggregationBuilder).getSizeInMemory());
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

    public Page processAggregation()
    {
        if (state == State.AGGR_FINISHED) {
            return null;
        }

        // process unfinished work if one exists
        if (unfinishedAggrWork != null) {
            boolean workDone = unfinishedAggrWork.process();
            aggregationBuilder.updateMemory();
            if (!workDone) {
                return null;
            }
            unfinishedAggrWork = null;
        }

        if (outputPages == null) {
            //if (state == State.AGGR_FINISHING/*aggregationFinishing*/) {
            if (/*!aggregationInputProcessed && */aggregator.isProduceDefaultOutput()) {
                // global aggregations always generate an output row with the default aggregation output (e.g. 0 for COUNT, NULL for SUM)
                state = State.AGGR_FINISHED; //aggregationFinished = true;
                return aggregator.getGlobalAggregationOutput();
            }

            if (aggregationBuilder == null) {
                state = State.AGGR_FINISHED; //aggregationFinished = true;
                return null;
            }
            //}

            // only flush if we are finishing or the aggregation builder is full
            if (/*!aggregationFinishing && */(/*aggregationBuilder == null || */!aggregationBuilder.isFull())) {
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
        memoryContext.setBytes(0);
        aggregator.getPartialAggregationController().ifPresent(
                controller -> controller.onFlush(numberOfInputRowsProcessed, numberOfUniqueRowsProduced));
        numberOfInputRowsProcessed = 0;
        numberOfUniqueRowsProduced = 0;
    }

    @Override
    public void finish()
    {
        /*aggregationFinishing = true;*/
        if (state == State.CONSUMING_INPUT) {
            state = State.AGGR_FINISHING;
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
                return;
            case AGGR_FINISHING:
                // finish all aggregation operation first
                finishAggregation();
                return;
            case AGGR_FINISHED:
                finishInput();
                return;
            case LOOKUP_SOURCE_BUILT:
                disposeLookupSourceIfRequested();
                return;

            case CLOSED:
                // no-op
                return;
        }

        throw new IllegalStateException("Unhandled state: " + state);
    }

    private void finishAggregation()
    {
        Page page = processAggregation();
        while (page != null) {
            updateIndex(page);
            page = processAggregation();
        }
    }

    private void finishInput()
    {
        checkState(state == State.AGGR_FINISHED);
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

    private LookupSourceSupplier buildLookupSource()
    {
        LookupSourceSupplier partition = index.createLookupSourceSupplier(operatorContext.getSession(), hashChannels, preComputedHashChannel,
                filterFunctionFactory, sortChannel, searchFunctionFactories, Optional.of(outputChannels),
                countChannel, Optional.ofNullable(aggrOnAggregationBuilder));
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

    @Override
    public void close()
    {
        if (state == State.CONSUMING_INPUT) {
            closeAggregationBuilder();
        }
        if (state == State.CLOSED) {
            return;
        }
        // close() can be called in any state, due for example to query failure, and must clean resource up unconditionally

        lookupSourceSupplier = null;
        state = State.CLOSED;
        finishMemoryRevoke = finishMemoryRevoke.map(ifPresent -> () -> {});

        try (Closer closer = Closer.create()) {
            closer.register(index::clear);
            closer.register(() -> localUserMemoryContext.setBytes(0));
            closer.register(() -> localRevocableMemoryContext.setBytes(0));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public State getState()
    {
        return state;
    }
}
