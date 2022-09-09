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
package io.prestosql.execution.buffer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.hetu.core.transport.execution.buffer.PageCodecMarker;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.exchange.ExchangeSink;
import io.prestosql.exchange.ExchangeSinkInstanceHandle;
import io.prestosql.execution.StageId;
import io.prestosql.execution.TaskId;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.QueryId;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.execution.buffer.BufferState.ABORTED;
import static io.prestosql.execution.buffer.BufferState.FAILED;
import static io.prestosql.execution.buffer.BufferState.FINISHED;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_PAGES;
import static io.prestosql.execution.buffer.OutputBuffers.createSpoolingExchangeOutputBuffers;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSpoolingExchangeOutputBuffer
{
    @Test
    public void testIsFull()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);
        assertNotBlocked(outputBuffer.isFull());

        CompletableFuture<Void> blocked = new CompletableFuture<>();
        exchangeSink.setBlocked(blocked);

        ListenableFuture<?> full = outputBuffer.isFull();
        assertBlocked(full);

        blocked.complete(null);
        assertNotBlocked(full);
    }

    @Test
    public void testFinishSuccess()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);

        outputBuffer.setNoMorePages();
        // call it for the second time to verify that the buffer handles it correctly
        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), NO_MORE_PAGES);

        finish.complete(null);
        assertEquals(outputBuffer.getState(), FINISHED);
    }

    @Test
    public void testFinishFailure()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);

        outputBuffer.setNoMorePages();
        // call it for the second time to verify that the buffer handles it correctly
        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), NO_MORE_PAGES);

        RuntimeException failure = new RuntimeException("failure");
        finish.completeExceptionally(failure);
        assertEquals(outputBuffer.getState(), FAILED);
        assertEquals(outputBuffer.getFailureCause(), Optional.of(failure));
    }

    @Test
    public void testDestroyAfterFinishCompletion()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);

        outputBuffer.setNoMorePages();
        // call it for the second time to verify that the buffer handles it correctly
        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), NO_MORE_PAGES);

        finish.complete(null);
        assertEquals(outputBuffer.getState(), FINISHED);

        outputBuffer.destroy();
        assertEquals(outputBuffer.getState(), FINISHED);
    }

    @Test
    public void testDestroyBeforeFinishCompletion()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);

        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), NO_MORE_PAGES);

        outputBuffer.destroy();
        assertEquals(outputBuffer.getState(), ABORTED);

        finish.complete(null);
        assertEquals(outputBuffer.getState(), ABORTED);
    }

    @Test
    public void testAbortBeforeNoMorePages()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);

        outputBuffer.abort();
        assertEquals(outputBuffer.getState(), ABORTED);
        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), ABORTED);
    }

    @Test
    public void testAbortBeforeFinishCompletion()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);
        CompletableFuture<Void> abort = new CompletableFuture<>();
        exchangeSink.setAbort(abort);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);

        outputBuffer.setNoMorePages();
        // call it for the second time to verify that the buffer handles it correctly
        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), NO_MORE_PAGES);

        // if abort is called before finish completes it should abort the buffer
        outputBuffer.abort();
        assertEquals(outputBuffer.getState(), ABORTED);

        // abort failure shouldn't impact the buffer state
        abort.completeExceptionally(new RuntimeException("failure"));
        assertEquals(outputBuffer.getState(), ABORTED);
    }

    @Test
    public void testAbortAfterFinishCompletion()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);
        CompletableFuture<Void> abort = new CompletableFuture<>();
        exchangeSink.setAbort(abort);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);

        outputBuffer.setNoMorePages();
        // call it for the second time to verify that the buffer handles it correctly
        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), NO_MORE_PAGES);

        finish.complete(null);
        assertEquals(outputBuffer.getState(), FINISHED);

        // abort is no op
        outputBuffer.abort();
        assertEquals(outputBuffer.getState(), FINISHED);

        // abort success doesn't change the buffer state
        abort.complete(null);
        assertEquals(outputBuffer.getState(), FINISHED);
    }

    private SerializedPage getSerializedPage()
    {
        return new SerializedPage(utf8Slice("1000"), PageCodecMarker.MarkerSet.empty(), 0, 4);
    }

    @Test
    public void testEnqueueAfterFinish()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);

        outputBuffer.enqueue(0, ImmutableList.of(getSerializedPage()), "id");
        outputBuffer.enqueue(1, ImmutableList.of(getSerializedPage(), getSerializedPage()), "id");

        ListMultimap<Integer, SerializedPage> pageList = exchangeSink.getDataBuffer();
        assertEquals(pageList.size(), 3);

        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), NO_MORE_PAGES);
        // the buffer is flushing, this page is expected to be rejected
        outputBuffer.enqueue(0, ImmutableList.of(getSerializedPage()), "id");
        pageList = exchangeSink.getDataBuffer();
        assertEquals(pageList.size(), 3);

        finish.complete(null);
        assertEquals(outputBuffer.getState(), FINISHED);
        outputBuffer.enqueue(0, ImmutableList.of(getSerializedPage()), "id");
        pageList = exchangeSink.getDataBuffer();
        assertEquals(pageList.size(), 3);
    }

    @Test
    public void testEnqueueAfterAbort()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> abort = new CompletableFuture<>();
        exchangeSink.setAbort(abort);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);

        outputBuffer.enqueue(0, ImmutableList.of(getSerializedPage()), "id");
        outputBuffer.enqueue(1, ImmutableList.of(getSerializedPage(), getSerializedPage()), "id");

        ListMultimap<Integer, SerializedPage> pageList = exchangeSink.getDataBuffer();
        assertEquals(pageList.size(), 3);

        outputBuffer.abort();
        assertEquals(outputBuffer.getState(), ABORTED);
        // the buffer is flushing, this page is expected to be rejected
        outputBuffer.enqueue(0, ImmutableList.of(getSerializedPage()), "id");
        pageList = exchangeSink.getDataBuffer();
        assertEquals(pageList.size(), 3);

        abort.complete(null);
        assertEquals(outputBuffer.getState(), ABORTED);
        outputBuffer.enqueue(0, ImmutableList.of(getSerializedPage()), "id");
        pageList = exchangeSink.getDataBuffer();
        assertEquals(pageList.size(), 3);
    }

    private static SpoolingExchangeOutputBuffer createSpoolingExchangeOutputBuffer(ExchangeSink exchangeSink)
    {
        return new SpoolingExchangeOutputBuffer(
                new OutputBufferStateMachine(new TaskId(new StageId(new QueryId("query"), 0), 0, 0), directExecutor()),
                createSpoolingExchangeOutputBuffers(TestingExchangeSinkInstanceHandle.INSTANCE),
                exchangeSink,
                TestingLocalMemoryContext::new);
    }

    private static void assertNotBlocked(ListenableFuture<?> blocked)
    {
        assertTrue(blocked.isDone());
    }

    private static void assertBlocked(ListenableFuture<?> blocked)
    {
        assertFalse(blocked.isDone());
    }

    private static class TestingExchangeSink
            implements ExchangeSink
    {
        private final ListMultimap<Integer, SerializedPage> dataBuffer = ArrayListMultimap.create();
        private CompletableFuture<Void> blocked = CompletableFuture.completedFuture(null);
        private CompletableFuture<Void> finish = CompletableFuture.completedFuture(null);
        private CompletableFuture<Void> abort = CompletableFuture.completedFuture(null);

        private boolean finishCalled;
        private boolean abortCalled;

        @Override
        public CompletableFuture<Void> isBlocked()
        {
            return blocked;
        }

        public void setBlocked(CompletableFuture<Void> blocked)
        {
            this.blocked = requireNonNull(blocked, "blocked is null");
        }

        @Override
        public void add(int partitionId, SerializedPage page)
        {
            this.dataBuffer.put(partitionId, page);
        }

        @Override
        public void add(int partitionId, Page page, PagesSerde directSerde)
        {
        }

        public ListMultimap<Integer, SerializedPage> getDataBuffer()
        {
            return dataBuffer;
        }

        @Override

        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public CompletableFuture<Void> finish()
        {
            assertFalse(abortCalled);
            assertFalse(finishCalled);
            finishCalled = true;
            return finish;
        }

        public void setFinish(CompletableFuture<Void> finish)
        {
            this.finish = requireNonNull(finish, "finish is null");
        }

        @Override
        public CompletableFuture<Void> abort()
        {
            assertFalse(abortCalled);
            abortCalled = true;
            return abort;
        }

        public void setAbort(CompletableFuture<Void> abort)
        {
            this.abort = requireNonNull(abort, "abort is null");
        }
    }

    private enum TestingExchangeSinkInstanceHandle
            implements ExchangeSinkInstanceHandle
    {
        INSTANCE
    }

    private static class TestingLocalMemoryContext
            implements LocalMemoryContext
    {
        @Override
        public long getBytes()
        {
            return 0;
        }

        @Override
        public ListenableFuture<Void> setBytes(long bytes)
        {
            return Futures.immediateFuture(null);
        }

        @Override
        public boolean trySetBytes(long bytes)
        {
            return true;
        }

        @Override
        public void close()
        {
        }
    }
}
