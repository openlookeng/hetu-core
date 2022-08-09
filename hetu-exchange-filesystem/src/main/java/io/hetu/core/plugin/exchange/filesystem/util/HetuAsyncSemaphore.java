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
package io.hetu.core.plugin.exchange.filesystem.util;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateCancelledFuture;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static java.util.Objects.requireNonNull;

public class HetuAsyncSemaphore<T, R>
{
    private final Queue<QueuedTask<T, R>> queuedTasks = new ConcurrentLinkedQueue<>();
    private final AtomicInteger counter = new AtomicInteger();
    private final Runnable runNextTask = this::runNext;
    private final int maxPermits;
    private final Executor submitExecutor;
    private final Function<T, ListenableFuture<R>> submitter;

    public static <T, R> ListenableFuture<List<R>> processAll(List<T> tasks, Function<T, ListenableFuture<R>> submitter, int maxConcurrency, Executor submitExecutor)
    {
        SettableFuture<List<R>> resultFuture = SettableFuture.create();
        HetuAsyncSemaphore<T, R> semaphore = new HetuAsyncSemaphore<>(maxConcurrency, submitExecutor, task -> {
            if (resultFuture.isCancelled()) {
                return immediateCancelledFuture();
            }
            return submitter.apply(task);
        });
        resultFuture.setFuture(allAsListWithCancellationOnFailure(tasks.stream()
                .map(semaphore::submit)
                .collect(toImmutableList())));
        return resultFuture;
    }

    public ListenableFuture<R> submit(T task)
    {
        QueuedTask<T, R> queuedTask = new QueuedTask<>(task);
        queuedTasks.add(queuedTask);
        acquirePermit();
        return queuedTask.getCompletionFuture();
    }

    private void acquirePermit()
    {
        if (counter.incrementAndGet() <= maxPermits) {
            submitExecutor.execute(runNextTask);
        }
    }

    private void releasePermit()
    {
        if (counter.getAndDecrement() > maxPermits) {
            submitExecutor.execute(runNextTask);
        }
    }

    private static <V> ListenableFuture<List<V>> allAsListWithCancellationOnFailure(Iterable<? extends ListenableFuture<? extends V>> futures)
    {
        List<ListenableFuture<? extends V>> futuresSnapshot = ImmutableList.copyOf(futures);
        ListenableFuture<List<V>> listFuture = allAsList(futuresSnapshot);
        addExceptionCallback(listFuture, () -> futuresSnapshot.forEach(future -> future.cancel(true)));
        return listFuture;
    }

    public HetuAsyncSemaphore(int maxPermits, Executor submitExecutor, Function<T, ListenableFuture<R>> submitter)
    {
        checkArgument(maxPermits > 0, "must have at least one permit");
        this.maxPermits = maxPermits;
        this.submitExecutor = requireNonNull(submitExecutor, "submitExecutor is null");
        this.submitter = requireNonNull(submitter, "submitter is null");
    }

    private void runNext()
    {
        QueuedTask<T, R> queuedTask = queuedTasks.poll();
        verify(queuedTask != null);
        if (!queuedTask.getCompletionFuture().isDone()) {
            queuedTask.setFuture(submitTask(queuedTask.getTask()));
        }
        queuedTask.getCompletionFuture().addListener(this::releasePermit, directExecutor());
    }

    private ListenableFuture<R> submitTask(T task)
    {
        try {
            ListenableFuture<R> future = submitter.apply(task);
            if (future == null) {
                return immediateFailedFuture(new NullPointerException("Submitter returned a null future for task " + task));
            }
            return future;
        }
        catch (Exception e) {
            return immediateFailedFuture(e);
        }
    }

    private static class QueuedTask<T, R>
    {
        private final T task;
        private final SettableFuture<R> settableFuture = SettableFuture.create();

        private QueuedTask(T task)
        {
            this.task = requireNonNull(task, "task is null");
        }

        public T getTask()
        {
            return task;
        }

        public void setFuture(ListenableFuture<R> future)
        {
            settableFuture.setFuture(future);
        }

        public ListenableFuture<R> getCompletionFuture()
        {
            return settableFuture;
        }
    }
}
