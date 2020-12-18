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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.metadata.Split;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.GenericSpiller;
import io.prestosql.spiller.Spiller;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER;
import static io.prestosql.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT;
import static io.prestosql.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER;
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

    private ReuseExchangeOperator.STRATEGY strategy;
    private Integer reuseTableScanMappingId;
    private static ConcurrentMap<String, Integer> sourceReuseTableScanMappingIdPositionIndexMap;
    private final Optional<SpillerFactory> spillerFactory;
    private final List<Type> projectionTypes;
    private ListenableFuture<?> spillInProgress = immediateFuture(null);
    private boolean spillEnabled;
    private final long spillThreshold;
    private static ConcurrentMap<Integer, ReuseExchangeTableScanMappingIdState> reuseExchangeTableScanMappingIdUtilsMap = new ConcurrentHashMap<>();
    private ReuseExchangeTableScanMappingIdState reuseExchangeTableScanMappingIdState;

    private enum READSTATE {READ_MEMORY, READ_DISK}

    // sourceIdString is required, as multiple resue nodes can be there with the same reuseTableScanMappingId. It needs
    // to differentiate by concatenating SourceId and reuseTableScanMappingId.
    private String sourceIdString;

    public WorkProcessorSourceOperatorAdapter(OperatorContext operatorContext, WorkProcessorSourceOperatorFactory sourceOperatorFactory,
                                              ReuseExchangeOperator.STRATEGY strategy, Integer reuseTableScanMappingId, boolean spillEnabled, List<Type> projectionTypes,
                                              Optional<SpillerFactory> spillerFactory, Integer spillerThreshold, Integer consumerTableScanNodeCount)
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
        this.reuseTableScanMappingId = reuseTableScanMappingId;
        this.spillEnabled = spillEnabled;
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.spillThreshold = spillerThreshold;
        this.projectionTypes = requireNonNull(projectionTypes, "types is null");

        if (!strategy.equals(REUSE_STRATEGY_DEFAULT)) {
            synchronized (WorkProcessorSourceOperatorAdapter.class) {
                if (strategy.equals(REUSE_STRATEGY_PRODUCER) && !reuseExchangeTableScanMappingIdUtilsMap.containsKey(reuseTableScanMappingId)) {
                    ReuseExchangeTableScanMappingIdState reuseExchangeTableScanMappingIdState = new ReuseExchangeTableScanMappingIdState(strategy, reuseTableScanMappingId, operatorContext, consumerTableScanNodeCount);
                    reuseExchangeTableScanMappingIdUtilsMap.put(reuseTableScanMappingId, reuseExchangeTableScanMappingIdState);
                }

                this.reuseExchangeTableScanMappingIdState = reuseExchangeTableScanMappingIdUtilsMap.get(reuseTableScanMappingId);

                if (strategy.equals(REUSE_STRATEGY_CONSUMER)) {
                    sourceIdString = sourceId.toString().concat(reuseTableScanMappingId.toString());
                    reuseExchangeTableScanMappingIdUtilsMap.get(reuseTableScanMappingId).addToSourceNodeModifiedIdList(sourceIdString);
                }

                if (sourceReuseTableScanMappingIdPositionIndexMap == null) {
                    sourceReuseTableScanMappingIdPositionIndexMap = new ConcurrentHashMap<>();
                }
            }
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

    public ReuseExchangeOperator.STRATEGY getStrategy()
    {
        return strategy;
    }

    public int getReuseTableScanMappingId()
    {
        return reuseTableScanMappingId;
    }

    public boolean isNotSpilled()
    {
        int pagesWrittenCount = reuseExchangeTableScanMappingIdState.getPagesWrittenCount();

        if (pagesWrittenCount == 0) {
            // there was no spilling of data- either spilling is not used, or not enough data to spill
            return true;
        }
        return false;
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
        if (strategy.equals(REUSE_STRATEGY_CONSUMER)) {
            return getPage();
        }

        if (!pages.process()) {
            return null;
        }

        if (pages.isFinished()) {
            //In-case result is empty it will never initialize and reuse will keep on waiting.
            if (strategy.equals(REUSE_STRATEGY_PRODUCER)) {
                initPageCache();
            }
            return null;
        }

        Page page = pages.getResult();
        if (strategy.equals(REUSE_STRATEGY_PRODUCER) && page != null) {
            setPage(page);
        }

        return page;
    }

    public static synchronized void releaseCache(Integer reuseTableScanMappingId)
    {
        if (reuseExchangeTableScanMappingIdUtilsMap.containsKey(reuseTableScanMappingId)) {
            if (reuseExchangeTableScanMappingIdUtilsMap.get(reuseTableScanMappingId).getPageCaches() != null) {
                reuseExchangeTableScanMappingIdUtilsMap.get(reuseTableScanMappingId).setPageCaches(null);
            }
        }
    }

    public static synchronized void deleteSpilledFiles(Integer reuseTableScanMappingId)
    {
        if (reuseExchangeTableScanMappingIdUtilsMap.containsKey(reuseTableScanMappingId)) {
            if (reuseExchangeTableScanMappingIdUtilsMap.get(reuseTableScanMappingId).getSpiller().isPresent()) {
                GenericSpiller spillerObject = (GenericSpiller) reuseExchangeTableScanMappingIdUtilsMap.get(reuseTableScanMappingId).getSpiller().get();
                if (spillerObject != null) {
                    spillerObject.deleteAllStreams();
                }
            }
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
        if (strategy.equals(REUSE_STRATEGY_CONSUMER)) {
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
        synchronized (reuseExchangeTableScanMappingIdState) {
            Page newPage = null;
            READSTATE readState = READSTATE.READ_MEMORY;
            initIndex();

            if (!reuseExchangeTableScanMappingIdState.getPagesToSpill().isEmpty()) {
                // some pages are leftover to spill because the size is < spillThreshold/2
                List<Page> inMemoryPages = reuseExchangeTableScanMappingIdState.getPageCaches();
                inMemoryPages.addAll(reuseExchangeTableScanMappingIdState.getPagesToSpill());
                reuseExchangeTableScanMappingIdState.setPageCaches(inMemoryPages);
                reuseExchangeTableScanMappingIdState.clearPagesToSpill();
            }

            if (sourceReuseTableScanMappingIdPositionIndexMap.get(sourceIdString) == null || reuseExchangeTableScanMappingIdState.getPageCaches() == null
                    || sourceReuseTableScanMappingIdPositionIndexMap.get(sourceIdString) >= reuseExchangeTableScanMappingIdState.getPageCaches().size()) {
                if (reuseExchangeTableScanMappingIdState.getPagesWrittenCount() != 0) {
                    if (reuseExchangeTableScanMappingIdState.getCurConsumerScanNodeRefCount() > 0
                            && reuseExchangeTableScanMappingIdState.getPageCaches().size() != 0) {
                        return null;
                    }
                    readState = READSTATE.READ_DISK;
                }
                else {
                    return null;
                }
            }

            if (!reuseExchangeTableScanMappingIdState.getPageCaches().isEmpty() && readState == READSTATE.READ_MEMORY) {
                newPage = reuseExchangeTableScanMappingIdState.getPageCaches().get(sourceReuseTableScanMappingIdPositionIndexMap.get(sourceIdString));
                sourceReuseTableScanMappingIdPositionIndexMap.merge(sourceIdString, 1, Integer::sum);

                if (sourceReuseTableScanMappingIdPositionIndexMap.get(sourceIdString) >= reuseExchangeTableScanMappingIdState.getPageCaches().size()) {
                    int consumerTableScanNodeCount = reuseExchangeTableScanMappingIdState.getCurConsumerScanNodeRefCount() - 1;
                    reuseExchangeTableScanMappingIdState.setCurConsumerScanNodeRefCount(consumerTableScanNodeCount);
                    if (consumerTableScanNodeCount == 0) {
                        reuseExchangeTableScanMappingIdState.getPageCaches().clear();
                    }
                }
            }
            else if (readState == READSTATE.READ_DISK) {
                // no page available in memory, unspill from disk and read
                List<Iterator<Page>> spilledPages = getSpilledPages();
                Iterator<Page> readPages;
                List<Page> pagesRead = new ArrayList<>();
                if (!spilledPages.isEmpty()) {
                    for (int i = 0; i < spilledPages.size(); ++i) {
                        readPages = spilledPages.get(i);
                        readPages.forEachRemaining(pagesRead::add);
                    }
                    reuseExchangeTableScanMappingIdState.setPageCaches(pagesRead);

                    for (String sourceId : reuseExchangeTableScanMappingIdState.getSourceNodeModifiedIdList()) {
                        //reinitialize indexes for all consumers in this reuseTableScanMappingId here
                        sourceReuseTableScanMappingIdPositionIndexMap.put(sourceId, 0);
                    }

                    //restore original count so that all consumers are now active again
                    reuseExchangeTableScanMappingIdState.setCurConsumerScanNodeRefCount(reuseExchangeTableScanMappingIdState.getTotalConsumerScanNodeCount());
                    reuseExchangeTableScanMappingIdState.setPagesWritten(reuseExchangeTableScanMappingIdState.getPagesWrittenCount() - pagesRead.size());

                    if (reuseExchangeTableScanMappingIdState.getPagesWrittenCount() < 0) {
                        throw new ArrayIndexOutOfBoundsException("MORE PAGES READ THAN WRITTEN");
                    }

                    if (reuseExchangeTableScanMappingIdState.getPagesWrittenCount() == 0) {
                        reuseExchangeTableScanMappingIdState.getOperatorContext().destroy();
                        deleteSpilledFiles(reuseTableScanMappingId);
                        //destroy context for producer from here.
                    }
                    newPage = reuseExchangeTableScanMappingIdState.getPageCaches().get(sourceReuseTableScanMappingIdPositionIndexMap.get(sourceIdString));
                    sourceReuseTableScanMappingIdPositionIndexMap.merge(sourceIdString, 1, Integer::sum);
                }
            }
            return newPage;
        }
    }

    private void setPage(Page page)
    {
        synchronized (reuseExchangeTableScanMappingIdState) {
            initPageCache(); // Actually it should not be required but somehow TSO Strategy-2 is get scheduled in parallel and clear the cache.
            if (!spillEnabled) {
                //spilling is not enabled so keep adding pages to cache in-memory
                List<Page> pageCachesList = reuseExchangeTableScanMappingIdState.getPageCaches();
                pageCachesList.add(page);
                reuseExchangeTableScanMappingIdState.setPageCaches(pageCachesList);
            }
            else {
                if (totalPageSize(reuseExchangeTableScanMappingIdState.getPageCaches()) < (spillThreshold / 2)) {
                    // if pageCaches hasn't reached spillThreshold/2, keep adding pages to it.
                    List<Page> pageCachesList = reuseExchangeTableScanMappingIdState.getPageCaches();
                    pageCachesList.add(page);
                    reuseExchangeTableScanMappingIdState.setPageCaches(pageCachesList);
                }
                else {
                    // no more space available in memory to store pages. pages will be spilled now
                    List<Page> pageSpilledList = reuseExchangeTableScanMappingIdState.getPagesToSpill();
                    pageSpilledList.add(page);
                    reuseExchangeTableScanMappingIdState.setPagesToSpill(pageSpilledList);

                    if (totalPageSize(pageSpilledList) >= (spillThreshold / 2)) {
                        if (!reuseExchangeTableScanMappingIdState.getSpiller().isPresent()) {
                            Optional<Spiller> spillObject = Optional.of(spillerFactory.get().create(projectionTypes, operatorContext.getSpillContext(),
                                    operatorContext.newAggregateSystemMemoryContext()));
                            reuseExchangeTableScanMappingIdState.setSpiller(spillObject);
                        }

                        spillInProgress = reuseExchangeTableScanMappingIdState.getSpiller().get().spill(pageSpilledList.iterator());

                        reuseExchangeTableScanMappingIdState.setPagesWritten(reuseExchangeTableScanMappingIdState.getPagesWrittenCount() + pageSpilledList.size());

                        try {
                            // blocking call to ensure spilling completes before we move forward
                            spillInProgress.get();
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        catch (ExecutionException e) {
                            e.printStackTrace();
                        }

                        reuseExchangeTableScanMappingIdState.clearPagesToSpill(); //clear the memory pressure once the data is spilled to disk
                    }
                }
            }
        }
    }

    private Boolean checkFinished()
    {
        synchronized (WorkProcessorSourceOperatorAdapter.class) {
            return sourceReuseTableScanMappingIdPositionIndexMap.get(sourceIdString) != null && reuseExchangeTableScanMappingIdState.getPageCaches() != null
                    && sourceReuseTableScanMappingIdPositionIndexMap.get(sourceIdString) >= reuseExchangeTableScanMappingIdState.getPageCaches().size()
                    && reuseExchangeTableScanMappingIdState.getPagesWrittenCount() == 0;
        }
    }

    private void initPageCache()
    {
        synchronized (reuseExchangeTableScanMappingIdState) {
            if (reuseExchangeTableScanMappingIdState.getPageCaches() == null) {
                reuseExchangeTableScanMappingIdState.setPageCaches(new ArrayList<>());
            }

            if (reuseExchangeTableScanMappingIdState.getPagesToSpill() == null) {
                reuseExchangeTableScanMappingIdState.setPagesToSpill(new ArrayList<>());
            }
        }
    }

    private List<Iterator<Page>> getSpilledPages()
    {
        if (!reuseExchangeTableScanMappingIdState.getSpiller().isPresent()) {
            return ImmutableList.of();
        }
        return reuseExchangeTableScanMappingIdState.getSpiller().get().getSpills().stream().collect(toImmutableList());
    }

    private long totalPageSize(List<Page> pageList)
    {
        if (pageList != null && pageList.size() > 0) {
            long totalSize = 0;
            for (Page page : pageList) {
                totalSize += page.getSizeInBytes();
            }
            return totalSize;
        }
        return 0;
    }

    private void initIndex()
    {
        synchronized (WorkProcessorSourceOperatorAdapter.class) {
            sourceReuseTableScanMappingIdPositionIndexMap.putIfAbsent(sourceIdString, 0);
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
