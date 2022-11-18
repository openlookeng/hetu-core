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

package io.prestosql.operator;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.block.BlockJsonSerde;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.snapshot.RestorableConfig;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import javax.annotation.concurrent.GuardedBy;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.operator.Operator.NOT_BLOCKED;

@RestorableConfig()
public class CommonTableExecutionContext
    implements Restorable
{
    private static final Logger LOG = Logger.get(CommonTableExecutionContext.class);
    private final String name;
    private int queueCnt;
    private final PlanNodeId feederId;
    private boolean isFeederInitialized;
    private List<Integer> feeders = Collections.synchronizedList(new ArrayList<>());

    private Map<PlanNodeId, LinkedList<Page>> consumerQueues;
    private AtomicInteger size = new AtomicInteger(0);
    private ConcurrentLinkedQueue<Page> prefetchedQueue;
    private final Executor notificationExecutor;
    @GuardedBy("this")
    private SettableFuture<?> blockedFuture;
    private int taskCount;
    private int maxMainQueueSize;
    private int maxPrefetchQueueSize;

    public CommonTableExecutionContext(String name, Set<PlanNodeId> consumers, PlanNodeId feederId, Executor notificationExecutor,
                                                int taskCount, int maxMainQueueSize, int maxPrefetchQueueSize)
    {
        this.name = name;
        this.feederId = feederId;
        this.consumerQueues = consumers.stream().collect(Collectors.toMap(x -> x, x -> new LinkedList<>()));
        this.prefetchedQueue = new ConcurrentLinkedQueue<Page>();
        this.queueCnt = consumers.size();
        this.notificationExecutor = notificationExecutor;
        blockedFuture = SettableFuture.create();
        blockedFuture.set(null);
        this.taskCount = taskCount;
        this.maxMainQueueSize = maxMainQueueSize;
        this.maxPrefetchQueueSize = maxPrefetchQueueSize;
    }

    public void addPage(Page page)
    {
        synchronized (consumerQueues.get(feederId)) {
            checkArgument(!isMaxLimitReached(), "No more pages can be added");
            if (isConsumerQueueFull() && !isPrefetchQueueFull()) {
                // Main queue is full, prefetch some more pages and keep it in a separate queue.
                prefetchedQueue.add(page);
                LOG.debug("CTE(" + name + ") Page added with " + page.getPositionCount() + " rows in prefetched queue");
                return;
            }
        }

        // Add current page to main queue.
        addPageToQueues(page);

        // If we see slots enough to accomodate entry in each queue, remove from prefetch queue and insert in main queue
        while (!isConsumerQueueFull() && !prefetchedQueue.isEmpty()) {
            addPageToQueues(prefetchedQueue.poll());
        }
    }

    // Add given page to all consumer queue.
    private void addPageToQueues(Page page)
    {
        if (page != null) {
            consumerQueues.entrySet().stream().forEach(e -> {
                synchronized (e.getValue()) {
                    size.incrementAndGet();
                    e.getValue().add(page);
                }
            });
            LOG.debug("CTE(" + name + ") Page added with " + page.getPositionCount() + " rows");
        }
    }

    private Page getPageFromQueue(LinkedList<Page> consumerQ)
    {
        size.decrementAndGet();
        Page page = consumerQ.removeLast();
        if (!blockedFuture.isDone() && (!isConsumerQueueFull() || !isPrefetchQueueFull())) {
            SettableFuture<?> future = this.blockedFuture;
            notificationExecutor.execute(() -> future.set(null));
            LOG.debug("operator is unblocked");
        }

        return page;
    }

    public Page getPage(PlanNodeId id) throws CTEDoneException
    {
        LinkedList<Page> consumerQ = consumerQueues.get(id);
        synchronized (consumerQ) {
            if (consumerQ.size() > 0) {
                return getPageFromQueue(consumerQ);
            }
            else if (isDone() && prefetchedQueue.isEmpty()) {
                // Its possible some other thread would have populated the main queue after check of consumerQ.size()
                // and before checking prefetchedQueue.isEmpty(). So even though there are some data in queue, it would
                // have consider this to be done.
                // So once we are here we should check again consumerQ.size().
                if (consumerQ.size() > 0) {
                    return getPageFromQueue(consumerQ);
                }

                LOG.debug("prefetched page size " + prefetchedQueue.size() + " main queue " + consumerQ.size() + "for consumer " + id.toString());
                throw new CTEDoneException();
            }
        }

        // Either not yet done or there are some entries in prefetched queue.
        // If it is second case, then populate main consumer queue from prefetch queue.
        boolean isPageAdded = false;
        synchronized (consumerQueues.get(feederId)) {
            while (!isConsumerQueueFull() && !prefetchedQueue.isEmpty()) {
                addPageToQueues(prefetchedQueue.poll());
                isPageAdded = true;
            }
        }

        if (isPageAdded) {
            return getPage(id);
        }
        else {
            return null;
        }
    }

    private boolean isConsumerQueueFull()
    {
        return size.get() >= ((maxMainQueueSize - taskCount) * queueCnt);
    }

    private boolean isPrefetchQueueFull()
    {
        return prefetchedQueue.size() >= (maxPrefetchQueueSize - taskCount);
    }

    private boolean isMaxLimitReached()
    {
        return (size.get() >= (maxMainQueueSize * queueCnt)) && (prefetchedQueue.size() >= maxPrefetchQueueSize);
    }

    public synchronized boolean isFeeder(PlanNodeId planNodeId)
    {
        return planNodeId.equals(feederId);
    }

    public synchronized void setFeederState(PlanNodeId planNodeId, Integer opeartorInstance, boolean initialized)
    {
        if (planNodeId.equals(feederId)) {
            if (initialized) {
                if (!feeders.contains(opeartorInstance)) {
                    feeders.add(opeartorInstance);

                    if (isFeederInitialized == false) {
                        isFeederInitialized = true;
                    }
                }
            }
            else {
                feeders.remove(opeartorInstance);
            }
        }
    }

    private boolean isDone()
    {
        return isFeederInitialized && feeders.size() == 0;
    }

    public String getName()
    {
        return name;
    }

    public ListenableFuture<?> isBlocked(PlanNodeId planNodeId)
    {
        // proxy CTE operator will never block
        if (!isFeeder(planNodeId)) {
            return NOT_BLOCKED;
        }

        synchronized (consumerQueues.get(planNodeId)) {
            // If main queue as well as prefetch queue has got filled then block this operator.
            // We add taskCount here as it may happen two threads of feeder will pass this check one after another and
            // both will go to add pages even though space worth 1 page only left. So we account for all task count space.
            if (isConsumerQueueFull() && isPrefetchQueueFull() && blockedFuture.isDone()) {
                blockedFuture = SettableFuture.create();
                return blockedFuture;
            }
            else if (!isConsumerQueueFull() || !isPrefetchQueueFull()) {
                return NOT_BLOCKED;
            }

            return blockedFuture;
        }
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CommonTableExecutionContext that = (CommonTableExecutionContext) o;
        return name.equals(that.name);
    }

    @Override
    public String toString()
    {
        return "CTE Feeder { id-" + name + ", size: " + size + ", capacity: " + maxMainQueueSize + " }";
    }

    public static class CTEDoneException
            extends Exception
    {
        public CTEDoneException()
        {
            super();
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider) {
        BlockEncodingSerde blockSerde = serdeProvider.getBlockEncodingSerde();
        CommonTableExecutionContextState myState = new CommonTableExecutionContextState();
        PagesSerdeFactory pagesSerdeFactory = new PagesSerdeFactory(blockSerde,false);
        PagesSerde pagesSerde  =  pagesSerdeFactory.createPagesSerde();

        myState.queueCnt = queueCnt;
        myState.maxPrefetchQueueSize = maxPrefetchQueueSize;
        myState.maxMainQueueSize = maxMainQueueSize;
        myState.taskCount = taskCount;
        myState.feeders = new Object[feeders.size()];
        for (int i = 0; i < feeders.size(); i++) {
            myState.feeders[i] = feeders.get(i);
        }
        myState.prefetchedQueue = new Object[prefetchedQueue.size()];
        Iterator iterator = prefetchedQueue.iterator();
        while (iterator.hasNext()) {
                Page page = (Page) iterator.next();
                SerializedPage serializedPage = pagesSerde.serialize(page);
                int p = 0;
                myState.prefetchedQueue[p] = serializedPage;
                p++;
        }
        myState.consumerQueues = new Object[consumerQueues.size()][2];
        if (consumerQueues != null) {
            int count = 0;
            for (Map.Entry<PlanNodeId, LinkedList<Page>> entry : consumerQueues.entrySet()) {
                myState.consumerQueues[count][0] = entry.getKey();
                LinkedList<Page> pages = entry.getValue();
                LinkedList<SerializedPage> serializedPages = pages.stream().map(page -> pagesSerde.serialize(page)).collect(Collectors.toCollection(LinkedList::new));
                myState.consumerQueues[count][1] = serializedPages;
                count ++;
            }
        }
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        BlockEncodingSerde blockSerde = serdeProvider.getBlockEncodingSerde();
        CommonTableExecutionContextState myState = (CommonTableExecutionContextState) state;

        PagesSerdeFactory pagesSerdeFactory = new PagesSerdeFactory(blockSerde,false);
        PagesSerde pagesSerde  =  pagesSerdeFactory.createPagesSerde();

        this.queueCnt = myState.queueCnt;
        this.maxPrefetchQueueSize = myState.maxPrefetchQueueSize;
        this.maxMainQueueSize = myState.maxMainQueueSize;
        this.taskCount = myState.taskCount;
        for (int i = 0; i < myState.feeders.length; i++) {
            feeders.add((Integer) myState.feeders[i]);
        }
        for (int i = 0; i < myState.prefetchedQueue.length; i++) {
            SerializedPage serializedPage = (SerializedPage) myState.prefetchedQueue[i];
            Page page =  pagesSerde.deserialize(serializedPage);
            prefetchedQueue.add(page);
        }
        for (int i = 0; i < myState.consumerQueues.length; i++) {
            LinkedList<SerializedPage>  pages = (LinkedList<SerializedPage>) myState.consumerQueues[i][1];
            LinkedList<Page> depages = pages.stream().map(page -> pagesSerde.deserialize(page)).collect(Collectors.toCollection(LinkedList::new));
            consumerQueues.put((PlanNodeId) myState.consumerQueues[i][0], depages);
        }

    }

    private static class CommonTableExecutionContextState
            implements Serializable
    {

        private int queueCnt;
        private int maxPrefetchQueueSize;
        private int maxMainQueueSize;
        private int taskCount;
        private Object[] feeders;
        private Object[] prefetchedQueue;
        private Object[][] consumerQueues;
    }


}
