/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.operator.Operator.NOT_BLOCKED;

public class CommonTableExecutionContext
{
    private static final Logger LOG = Logger.get(CommonTableExecutionContext.class);
    private final String name;
    private final int queueCnt;
    private final PlanNodeId producerId;
    private boolean isProducerInitialized;
    private List<Integer> producers = Collections.synchronizedList(new ArrayList<>());

    private Map<PlanNodeId, LinkedList<Page>> consumerQueues;
    private AtomicInteger size = new AtomicInteger(0);
    private ConcurrentLinkedQueue<Page> prefetchedQueue;
    private final Executor notificationExecutor;
    @GuardedBy("this")
    private SettableFuture<?> blockedFuture;
    private final int taskCount;
    private final int maxMainQueueSize;
    private final int maxPrefetchQueueSize;

    public CommonTableExecutionContext(String name, Set<PlanNodeId> consumers, PlanNodeId producerId, Executor notificationExecutor,
                                                int taskCount, int maxMainQueueSize, int maxPrefetchQueueSize)
    {
        this.name = name;
        this.producerId = producerId;
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
        synchronized (consumerQueues.get(producerId)) {
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
        synchronized (consumerQueues.get(producerId)) {
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

    public synchronized boolean isProducer(PlanNodeId planNodeId)
    {
        return planNodeId.equals(producerId);
    }

    public synchronized void setProducerState(PlanNodeId planNodeId, Integer opeartorInstance, boolean initialized)
    {
        if (planNodeId.equals(producerId)) {
            if (initialized) {
                if (!producers.contains(opeartorInstance)) {
                    producers.add(opeartorInstance);

                    if (isProducerInitialized == false) {
                        isProducerInitialized = true;
                    }
                }
            }
            else {
                producers.remove(opeartorInstance);
            }
        }
    }

    private boolean isDone()
    {
        return isProducerInitialized && producers.size() == 0;
    }

    public String getName()
    {
        return name;
    }

    public ListenableFuture<?> isBlocked(PlanNodeId planNodeId)
    {
        // proxy CTE operator will never block
        if (!isProducer(planNodeId)) {
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
        return "CTE Producer { id-" + name + ", size: " + size + ", capacity: " + maxMainQueueSize + " }";
    }

    public static class CTEDoneException
            extends Exception
    {
        public CTEDoneException()
        {
            super();
        }
    }
}
