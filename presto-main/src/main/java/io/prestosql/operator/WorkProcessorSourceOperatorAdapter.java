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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.metadata.Split;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static io.prestosql.operator.ReuseExchangeOperator.REUSE_STRATEGY_CONSUMER;
import static io.prestosql.operator.ReuseExchangeOperator.REUSE_STRATEGY_PRODUCER;
import static io.prestosql.operator.WorkProcessor.ProcessState.blocked;
import static io.prestosql.operator.WorkProcessor.ProcessState.finished;
import static io.prestosql.operator.WorkProcessor.ProcessState.ofResult;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class WorkProcessorSourceOperatorAdapter
        implements SourceOperator
{
    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final WorkProcessorSourceOperator sourceOperator;
    private final WorkProcessor<Page> pages;
    private final SplitBuffer splitBuffer;

    private boolean operatorFinishing;

    private long previousPhysicalInputBytes;
    private long previousPhysicalInputPositions;
    private long previousInternalNetworkInputBytes;
    private long previousInternalNetworkPositions;
    private long previousInputBytes;
    private long previousInputPositions;
    private long previousReadTimeNanos;

    private Integer strategy;
    private Integer slot;
    private static ConcurrentMap<String, Integer> indexes;
    private static ConcurrentMap<Integer, List<Page>> pageCaches;

    // sourceIdString is required, as multiple resue nodes can be there with the same slot. It needs
    // to differentiate by concatenating SourceId and Slot.
    private final String sourceIdString;

    public int getStrategy()
    {
        return strategy;
    }

    public WorkProcessorSourceOperatorAdapter(OperatorContext operatorContext, WorkProcessorSourceOperatorFactory sourceOperatorFactory,
                                                Integer strategy, Integer slot)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceOperatorFactory, "sourceOperatorFactory is null").getSourceId();
        this.splitBuffer = new SplitBuffer();
        this.sourceOperator = sourceOperatorFactory
                .create(
                        operatorContext.getSession(),
                        new MemoryTrackingContext(
                                operatorContext.aggregateUserMemoryContext(),
                                operatorContext.aggregateRevocableMemoryContext(),
                                operatorContext.aggregateSystemMemoryContext()),
                        operatorContext.getDriverContext().getYieldSignal(),
                        WorkProcessor.create(splitBuffer));
        this.pages = sourceOperator.getOutputPages()
                .map(Page::getLoadedPage)
                .withProcessStateMonitor(state -> updateOperatorStats())
                .finishWhen(() -> operatorFinishing);
        this.strategy = strategy;
        this.slot = slot;
        synchronized (WorkProcessorSourceOperatorAdapter.class) {
            if (pageCaches == null) {
                pageCaches = new ConcurrentHashMap<>();
                indexes = new ConcurrentHashMap<>();
            }

            sourceIdString = sourceId.toString().concat(slot.toString());
        }
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        if (operatorFinishing) {
            return Optional::empty;
        }

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(() -> new SplitOperatorInfo(splitInfo));
        }

        splitBuffer.add(split);
        return sourceOperator.getUpdatablePageSourceSupplier();
    }

    @Override
    public void noMoreSplits()
    {
        splitBuffer.noMoreSplits();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!pages.isBlocked()) {
            return NOT_BLOCKED;
        }

        return pages.getBlockedFuture();
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (strategy == REUSE_STRATEGY_CONSUMER) {
            return getPage();
        }

        if (!pages.process()) {
            return null;
        }

        if (pages.isFinished()) {
            //In-case result is empty it will never initialize and reuse will keep on waiting.
            if (strategy == REUSE_STRATEGY_PRODUCER) {
                initPageCache();
            }
            return null;
        }

        Page page = pages.getResult();
        if (strategy == REUSE_STRATEGY_PRODUCER && page != null) {
            setPage(page);
        }

        return page;
    }

    public static synchronized void releaseCache(Integer slot)
    {
        if (pageCaches != null) {
            pageCaches.remove(slot);
        }
    }

    @Override
    public void finish()
    {
        operatorFinishing = true;
        noMoreSplits();
    }

    @Override
    public boolean isFinished()
    {
        if (strategy == REUSE_STRATEGY_CONSUMER) {
            return checkFinished();
        }

        return pages.isFinished();
    }

    @Override
    public void close()
            throws Exception
    {
        sourceOperator.close();
    }

    private void updateOperatorStats()
    {
        long currentPhysicalInputBytes = sourceOperator.getPhysicalInputDataSize().toBytes();
        long currentPhysicalInputPositions = sourceOperator.getPhysicalInputPositions();
        long currentReadTimeNanos = sourceOperator.getReadTime().roundTo(NANOSECONDS);

        long currentInternalNetworkInputBytes = sourceOperator.getInternalNetworkInputDataSize().toBytes();
        long currentInternalNetworkPositions = sourceOperator.getInternalNetworkPositions();

        long currentInputBytes = sourceOperator.getInputDataSize().toBytes();
        long currentInputPositions = sourceOperator.getInputPositions();

        if (currentPhysicalInputBytes != previousPhysicalInputBytes
                || currentPhysicalInputPositions != previousPhysicalInputPositions
                || currentReadTimeNanos != previousReadTimeNanos) {
            operatorContext.recordPhysicalInputWithTiming(
                    currentPhysicalInputBytes - previousPhysicalInputBytes,
                    currentPhysicalInputPositions - previousPhysicalInputPositions,
                    currentReadTimeNanos - previousReadTimeNanos);

            previousPhysicalInputBytes = currentPhysicalInputBytes;
            previousPhysicalInputPositions = currentPhysicalInputPositions;
            previousReadTimeNanos = currentReadTimeNanos;
        }

        if (currentInternalNetworkInputBytes != previousInternalNetworkInputBytes
                || currentInternalNetworkPositions != previousInternalNetworkPositions) {
            operatorContext.recordNetworkInput(
                    currentInternalNetworkInputBytes - previousInternalNetworkInputBytes,
                    currentInternalNetworkPositions - previousInternalNetworkPositions);

            previousInternalNetworkInputBytes = currentInternalNetworkInputBytes;
            previousInternalNetworkPositions = currentInternalNetworkPositions;
        }

        if (currentInputBytes != previousInputBytes
                || currentInputPositions != previousInputPositions) {
            operatorContext.recordProcessedInput(
                    currentInputBytes - previousInputBytes,
                    currentInputPositions - previousInputPositions);

            previousInputBytes = currentInputBytes;
            previousInputPositions = currentInputPositions;
        }
    }

    private Page getPage()
    {
        synchronized (WorkProcessorSourceOperatorAdapter.class) {
            initIndex();
            if (indexes.get(sourceIdString) == null || pageCaches.get(slot) == null || indexes.get(sourceIdString) >= pageCaches.get(slot).size()) {
                return null;
            }

            Page newPage = pageCaches.get(slot).get(indexes.get(sourceIdString));
            indexes.merge(sourceIdString, 1, Integer::sum);
            return newPage;
        }
    }

    private void setPage(Page page)
    {
        synchronized (WorkProcessorSourceOperatorAdapter.class) {
            initPageCache(); // Actually it should not be required but somehow TSO Strategy-2 is get scheduled in parallel and clear the cache.
            pageCaches.get(slot).add(page);
        }
    }

    private Boolean checkFinished()
    {
        synchronized (WorkProcessorSourceOperatorAdapter.class) {
            return indexes.get(sourceIdString) != null && pageCaches.get(slot) != null
                    && indexes.get(sourceIdString) >= pageCaches.get(slot).size();
        }
    }

    private void initPageCache()
    {
        synchronized (WorkProcessorSourceOperatorAdapter.class) {
            pageCaches.putIfAbsent(slot, new ArrayList<>());
        }
    }

    private void initIndex()
    {
        synchronized (WorkProcessorSourceOperatorAdapter.class) {
            indexes.putIfAbsent(sourceIdString, 0);
        }
    }

    private class SplitBuffer
            implements WorkProcessor.Process<Split>
    {
        private final List<Split> pendingSplits = new ArrayList<>();

        private SettableFuture<?> blockedOnSplits = SettableFuture.create();
        private boolean noMoreSplits;

        @Override
        public WorkProcessor.ProcessState<Split> process()
        {
            if (pendingSplits.isEmpty()) {
                if (noMoreSplits) {
                    return finished();
                }

                blockedOnSplits = SettableFuture.create();
                return blocked(blockedOnSplits);
            }

            return ofResult(pendingSplits.remove(0));
        }

        void add(Split split)
        {
            pendingSplits.add(split);
            blockedOnSplits.set(null);
        }

        void noMoreSplits()
        {
            noMoreSplits = true;
            blockedOnSplits.set(null);
        }
    }
}
