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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PageCodecMarker;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.exchange.ExchangeManagerRegistry;
import io.prestosql.execution.StageId;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.exchange.ExchangeHandleResolver;
import io.prestosql.spi.exchange.RetryPolicy;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.util.concurrent.Futures.getUnchecked;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static io.prestosql.spi.exchange.ExchangeId.createRandomExchangeId;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDeduplicatingDirectExchangeBuffer
{
    private static final DataSize DEFAULT_BUFFER_CAPACITY = new DataSize(32, MEGABYTE);

    @Test
    public void testIsBlocked()
    {
        // immediate close
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            ListenableFuture<?> blocked = buffer.isBlocked();
            assertBlocked(blocked);
            buffer.close();
            assertNotBlocked(blocked);
        }

        // empty set of tasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            ListenableFuture<?> blocked = buffer.isBlocked();
            assertBlocked(blocked);
            buffer.noMoreTasks();
            assertNotBlocked(blocked);
        }

        // single task finishes before noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            ListenableFuture<?> blocked = buffer.isBlocked();
            assertBlocked(blocked);

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertBlocked(blocked);

            buffer.taskFinished(taskId);
            assertBlocked(blocked);

            buffer.noMoreTasks();
            assertNotBlocked(blocked);
        }

        // single task finishes after noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            ListenableFuture<?> blocked = buffer.isBlocked();
            assertBlocked(blocked);

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertBlocked(blocked);

            buffer.noMoreTasks();
            assertBlocked(blocked);

            buffer.taskFinished(taskId);
            assertNotBlocked(blocked);
        }

        // single task fails before noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            ListenableFuture<?> blocked = buffer.isBlocked();
            assertBlocked(blocked);

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertBlocked(blocked);

            buffer.taskFailed(taskId, new RuntimeException());
            assertBlocked(blocked);

            buffer.noMoreTasks();
            assertNotBlocked(blocked);
        }

        // single task fails after noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            ListenableFuture<?> blocked = buffer.isBlocked();
            assertBlocked(blocked);

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertBlocked(blocked);

            buffer.noMoreTasks();
            assertBlocked(blocked);

            buffer.taskFailed(taskId, new RuntimeException());
            assertNotBlocked(blocked);
        }

        // cancelled blocked future doesn't affect other blocked futures
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            ListenableFuture<?> blocked1 = buffer.isBlocked();
            ListenableFuture<?> blocked2 = buffer.isBlocked();
            assertBlocked(blocked1);
            assertBlocked(blocked2);

            blocked2.cancel(true);

            assertBlocked(blocked1);
            assertNotBlocked(blocked2);
        }
    }

    @Test
    public void testPollPagesTaskLevelRetry()
    {
        // 0 pages
        testPollPages(
                RetryPolicy.TASK,
                ImmutableListMultimap.of(),
                ImmutableMap.of(),
                DEFAULT_BUFFER_CAPACITY,
                0,
                ImmutableList.of());

        // single page, no spilling
        testPollPages(
                RetryPolicy.TASK,
                ImmutableListMultimap.<TaskId, SerializedPage>builder()
                        .put(createTaskId(0, 0), getSerializedPage())
                        .build(),
                ImmutableMap.of(),
                new DataSize(1, KILOBYTE),
                0,
                ImmutableList.of(getSerializedPage()));

        // discard single page, with no spilling
        testPollPages(
                RetryPolicy.TASK,
                ImmutableListMultimap.<TaskId, SerializedPage>builder()
                        .put(createTaskId(0, 0), getSerializedPage())
                        .put(createTaskId(0, 1), getSerializedPage())
                        .build(),
                ImmutableMap.of(),
                new DataSize(10, KILOBYTE),
                0,
                ImmutableList.of(getSerializedPage()));

        // multiple pages, no spilling
        testPollPages(
                RetryPolicy.TASK,
                ImmutableListMultimap.<TaskId, SerializedPage>builder()
                        .put(createTaskId(0, 0), getSerializedPage())
                        .put(createTaskId(1, 0), getSerializedPage())
                        .put(createTaskId(0, 1), getSerializedPage())
                        .build(),
                ImmutableMap.of(),
                new DataSize(5, KILOBYTE),
                0,
                ImmutableList.of(getSerializedPage(), getSerializedPage()));

        // failure in a task that produced no pages, no spilling
        testPollPages(
                RetryPolicy.TASK,
                ImmutableListMultimap.<TaskId, SerializedPage>builder()
                        .put(createTaskId(0, 0), getSerializedPage())
                        .put(createTaskId(0, 1), getSerializedPage())
                        .put(createTaskId(1, 1), getSerializedPage())
                        .build(),
                ImmutableMap.<TaskId, RuntimeException>builder()
                        .put(createTaskId(1, 0), new RuntimeException("error"))
                        .build(),
                new DataSize(10, KILOBYTE),
                0,
                ImmutableList.of(getSerializedPage(), getSerializedPage()));

        // failure in a task that produced no pages, with spilling
        testPollPages(
                RetryPolicy.TASK,
                ImmutableListMultimap.<TaskId, SerializedPage>builder()
                        .put(createTaskId(0, 0), getSerializedPage())
                        .put(createTaskId(0, 1), getSerializedPage())
                        .put(createTaskId(1, 1), getSerializedPage())
                        .build(),
                ImmutableMap.<TaskId, RuntimeException>builder()
                        .put(createTaskId(1, 0), new RuntimeException("error"))
                        .build(),
                new DataSize(2, KILOBYTE),
                0,
                ImmutableList.of(getSerializedPage(), getSerializedPage()));

        RuntimeException error = new RuntimeException("error");

        // buffer failure in a task that produced no pages, no spilling
        testPollPagesFailure(
                RetryPolicy.TASK,
                ImmutableListMultimap.<TaskId, SerializedPage>builder()
                        .put(createTaskId(0, 0), getSerializedPage())
                        .put(createTaskId(0, 1), getSerializedPage())
                        .put(createTaskId(1, 0), getSerializedPage())
                        .build(),
                ImmutableMap.<TaskId, RuntimeException>builder()
                        .put(createTaskId(2, 2), error)
                        .build(),
                new DataSize(5, KILOBYTE),
                0,
                error);

        // buffer failure in a task that produced some pages, no spilling
        testPollPagesFailure(
                RetryPolicy.TASK,
                ImmutableListMultimap.<TaskId, SerializedPage>builder()
                        .put(createTaskId(0, 1), getSerializedPage())
                        .put(createTaskId(1, 0), getSerializedPage())
                        .build(),
                ImmutableMap.<TaskId, RuntimeException>builder()
                        .put(createTaskId(0, 1), error)
                        .build(),
                new DataSize(5, KILOBYTE),
                0,
                error);
    }

    private void testPollPages(
            RetryPolicy retryPolicy,
            Multimap<TaskId, SerializedPage> pages,
            Map<TaskId, RuntimeException> failures,
            DataSize bufferCapacity,
            int expectedSpilledPageCount,
            List<SerializedPage> expectedOutput)
    {
        List<SerializedPage> actualOutput = pollPages(retryPolicy, pages, failures, bufferCapacity, expectedSpilledPageCount);
        assertEquals(actualOutput.size(), expectedOutput.size());
    }

    private void testPollPagesFailure(
            RetryPolicy retryPolicy,
            Multimap<TaskId, SerializedPage> pages,
            Map<TaskId, RuntimeException> failures,
            DataSize bufferCapacity,
            int expectedSpilledPageCount,
            Throwable expectedFailure)
    {
        assertThatThrownBy(() -> pollPages(retryPolicy, pages, failures, bufferCapacity, expectedSpilledPageCount)).isEqualTo(expectedFailure);
    }

    private List<SerializedPage> pollPages(
            RetryPolicy retryPolicy,
            Multimap<TaskId, SerializedPage> pages,
            Map<TaskId, RuntimeException> failures,
            DataSize bufferCapacity,
            int expectedSpilledPageCount)
    {
        Set<TaskId> addedTasks = new HashSet<>();
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(bufferCapacity, retryPolicy)) {
            for (Map.Entry<TaskId, SerializedPage> page : pages.entries()) {
                if (addedTasks.add(page.getKey())) {
                    buffer.addTask(page.getKey());
                }
                buffer.addPages(page.getKey(), ImmutableList.of(page.getValue()));
            }
            for (Map.Entry<TaskId, RuntimeException> failure : failures.entrySet()) {
                if (addedTasks.add(failure.getKey())) {
                    buffer.addTask(failure.getKey());
                }
                buffer.taskFailed(failure.getKey(), failure.getValue());
            }
            for (TaskId taskId : Sets.difference(pages.keySet(), failures.keySet())) {
                buffer.taskFinished(taskId);
            }
            buffer.noMoreTasks();

            ImmutableList.Builder<SerializedPage> result = ImmutableList.builder();
            while (!buffer.isFinished()) {
                // wait for blocked
                getUnchecked(buffer.isBlocked());
                SerializedPage page = buffer.pollPage();
                if (page == null) {
                    continue;
                }
                result.add(page);
            }
            assertTrue(buffer.isFinished());
            assertEquals(buffer.getSpilledPageCount(), expectedSpilledPageCount);
            return result.build();
        }
    }

    @Test
    public void testExchangeManagerNotConfigured()
    {
        // no overflow
        try (DirectExchangeBuffer buffer = new DeduplicatingDirectExchangeBuffer(
                directExecutor(),
                new DataSize(100, BYTE),
                RetryPolicy.TASK,
                new ExchangeManagerRegistry(new ExchangeHandleResolver()),
                new QueryId("query"),
                createRandomExchangeId())) {
            TaskId task = createTaskId(0, 0);
            SerializedPage page = getSerializedPage();

            buffer.addTask(task);
            buffer.addPages(task, ImmutableList.of(page));
            buffer.taskFinished(task);
            buffer.noMoreTasks();

            assertFalse(buffer.isFinished());
            assertNotBlocked(buffer.isBlocked());
            assertEquals(buffer.pollPage(), page);
            assertNull(buffer.pollPage());
            assertTrue(buffer.isFinished());
        }

        // overflow
        try (DirectExchangeBuffer buffer = new DeduplicatingDirectExchangeBuffer(
                directExecutor(),
                new DataSize(100, BYTE),
                RetryPolicy.TASK,
                new ExchangeManagerRegistry(new ExchangeHandleResolver()),
                new QueryId("query"),
                createRandomExchangeId())) {
            TaskId task = createTaskId(0, 0);

            SerializedPage page1 = getSerializedPage();
            SerializedPage page2 = getSerializedPage();

            buffer.addTask(task);
            buffer.addPages(task, ImmutableList.of(page1));

            assertFalse(buffer.isFinished());
            assertBlocked(buffer.isBlocked());

            buffer.addPages(task, ImmutableList.of(page2));
            assertFalse(buffer.isFinished());
            assertTrue(buffer.isFailed());
            assertNotBlocked(buffer.isBlocked());
            assertEquals(buffer.getRetainedSizeInBytes(), 0);
            assertEquals(buffer.getBufferedPageCount(), 0);

            assertThatThrownBy(buffer::pollPage)
                    .isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    public void testIsFinished()
    {
        // close right away
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            assertFalse(buffer.isFinished());
            buffer.close();
            assertTrue(buffer.isFinished());
        }

        // 0 tasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            assertFalse(buffer.isFinished());
            buffer.noMoreTasks();
            assertTrue(buffer.isFinished());
        }

        // single task producing no results, finish before noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(buffer.isFinished());

            buffer.taskFinished(taskId);
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertTrue(buffer.isFinished());
        }

        // single task producing no results, finish after noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            buffer.taskFinished(taskId);
            assertTrue(buffer.isFinished());
        }

        // single task producing no results, fail before noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(buffer.isFinished());

            buffer.taskFailed(taskId, new RuntimeException());
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());
            assertTrue(buffer.isFailed());
        }

        // single task producing no results, fail after noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            buffer.taskFailed(taskId, new RuntimeException());
            assertFalse(buffer.isFinished());
            assertTrue(buffer.isFailed());
        }

        // single task producing one page, fail after noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(getSerializedPage()));
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            buffer.taskFailed(taskId, new RuntimeException());
            assertFalse(buffer.isFinished());
            assertTrue(buffer.isFailed());
        }

        // single task producing one page, finish after noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(getSerializedPage()));
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            buffer.taskFinished(taskId);
            assertFalse(buffer.isFinished());

            assertNotNull(buffer.pollPage());
            assertTrue(buffer.isFinished());
        }

        // single task producing one page, finish before noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(getSerializedPage()));
            assertFalse(buffer.isFinished());

            buffer.taskFinished(taskId);
            assertFalse(buffer.isFinished());

            buffer.noMoreTasks();
            assertFalse(buffer.isFinished());

            assertNotNull(buffer.pollPage());
            assertTrue(buffer.isFinished());
        }
    }

    @Test
    public void testRemainingBufferCapacity()
    {
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, RetryPolicy.TASK)) {
            assertFalse(buffer.isFinished());

            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.addPages(taskId, ImmutableList.of(getSerializedPage()));

            assertEquals(buffer.getRemainingCapacityInBytes(), Long.MAX_VALUE);
        }
    }

    @Test
    public void testRemoteTaskFailedError()
    {
        testRemoteTaskFailedError(RetryPolicy.TASK);
    }

    private void testRemoteTaskFailedError(RetryPolicy retryPolicy)
    {
        // fail before noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, retryPolicy)) {
            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.taskFailed(taskId, new PrestoException(REMOTE_TASK_FAILED, "Remote task failed"));
            buffer.noMoreTasks();

            assertFalse(buffer.isFinished());
            assertFalse(buffer.isFailed());
            assertBlocked(buffer.isBlocked());
            assertNull(buffer.pollPage());
        }

        // fail after noMoreTasks
        try (DirectExchangeBuffer buffer = createDeduplicatingDirectExchangeBuffer(DEFAULT_BUFFER_CAPACITY, retryPolicy)) {
            TaskId taskId = createTaskId(0, 0);
            buffer.addTask(taskId);
            buffer.noMoreTasks();
            buffer.taskFailed(taskId, new PrestoException(REMOTE_TASK_FAILED, "Remote task failed"));

            assertFalse(buffer.isFinished());
            assertFalse(buffer.isFailed());
            assertBlocked(buffer.isBlocked());
            assertNull(buffer.pollPage());
        }
    }

    private DeduplicatingDirectExchangeBuffer createDeduplicatingDirectExchangeBuffer(DataSize bufferCapacity, RetryPolicy retryPolicy)
    {
        return new DeduplicatingDirectExchangeBuffer(
                directExecutor(),
                bufferCapacity,
                retryPolicy,
                mock(ExchangeManagerRegistry.class),
                new QueryId("query"),
                createRandomExchangeId());
    }

    private static TaskId createTaskId(int partition, int attempt)
    {
        return new TaskId(new StageId("query", 0), partition, attempt);
    }

    private static void assertNotBlocked(ListenableFuture<?> blocked)
    {
        assertTrue(blocked.isDone());
    }

    private static void assertBlocked(ListenableFuture<?> blocked)
    {
        assertFalse(blocked.isDone());
    }

    private SerializedPage getSerializedPage()
    {
        return new SerializedPage(utf8Slice("1000"), PageCodecMarker.MarkerSet.empty(), 0, 4);
    }
}
