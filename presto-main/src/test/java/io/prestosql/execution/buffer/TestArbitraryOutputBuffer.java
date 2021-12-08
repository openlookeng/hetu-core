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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.snapshot.MultiInputRestorable;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.SnapshotTestUtil;
import io.prestosql.spi.type.BigintType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static io.prestosql.execution.buffer.BufferResult.emptyResults;
import static io.prestosql.execution.buffer.BufferState.OPEN;
import static io.prestosql.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static io.prestosql.execution.buffer.BufferTestUtils.MAX_WAIT;
import static io.prestosql.execution.buffer.BufferTestUtils.NO_WAIT;
import static io.prestosql.execution.buffer.BufferTestUtils.acknowledgeBufferResult;
import static io.prestosql.execution.buffer.BufferTestUtils.addPage;
import static io.prestosql.execution.buffer.BufferTestUtils.assertBufferResultEquals;
import static io.prestosql.execution.buffer.BufferTestUtils.assertFinished;
import static io.prestosql.execution.buffer.BufferTestUtils.assertFutureIsDone;
import static io.prestosql.execution.buffer.BufferTestUtils.createBufferResult;
import static io.prestosql.execution.buffer.BufferTestUtils.createPage;
import static io.prestosql.execution.buffer.BufferTestUtils.enqueuePage;
import static io.prestosql.execution.buffer.BufferTestUtils.getFuture;
import static io.prestosql.execution.buffer.BufferTestUtils.sizeOfPages;
import static io.prestosql.execution.buffer.OutputBuffers.BROADCAST_PARTITION_ID;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.ARBITRARY;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.BROADCAST;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static io.prestosql.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestArbitraryOutputBuffer
{
    private static final ImmutableList<BigintType> TYPES = ImmutableList.of(BIGINT);
    private static final OutputBufferId FIRST = new OutputBufferId(0);
    private static final OutputBufferId SECOND = new OutputBufferId(1);
    private static final OutputBufferId THIRD = new OutputBufferId(2);

    private ScheduledExecutorService stateNotificationExecutor;

    @BeforeClass
    public void setUp()
    {
        stateNotificationExecutor = newScheduledThreadPool(5, daemonThreadsNamed("test-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (stateNotificationExecutor != null) {
            stateNotificationExecutor.shutdownNow();
            stateNotificationExecutor = null;
        }
    }

    @Test
    public void testInvalidConstructorArg()
    {
        try {
            createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, BROADCAST_PARTITION_ID).withNoMoreBufferIds(), new DataSize(0, BYTE));
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException ignored) {
        }
        try {
            createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), new DataSize(0, BYTE));
            fail("Expected IllegalStateException");
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testSimple()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY);
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        outputBuffers = outputBuffers.withBuffer(FIRST, BROADCAST_PARTITION_ID);

        // add a queue
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, 3, FIRST, 0, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, 0, FIRST, 3, 0);

        // acknowledge first three pages
        buffer.get(FIRST, 3, sizeOfPages(1)).cancel(true);
        // pages now acknowledged
        assertQueueState(buffer, 0, FIRST, 0, 3);

        // fill the buffer, so that it has 10 buffered pages
        for (int i = 3; i < 13; i++) {
            addPage(buffer, createPage(i));
        }
        // there is a pending read from above so one page will be assigned to the first buffer
        assertQueueState(buffer, 9, FIRST, 1, 3);

        // try to add one more page, which should block
        ListenableFuture<?> future = enqueuePage(buffer, createPage(13));
        assertFalse(future.isDone());
        assertQueueState(buffer, 10, FIRST, 1, 3);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 3, sizeOfPages(1), NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertQueueState(buffer, 10, FIRST, 1, 3);

        // we should still be blocked
        assertFalse(future.isDone());

        //
        // add another buffer and verify it sees buffered pages
        outputBuffers = outputBuffers.withBuffer(SECOND, BROADCAST_PARTITION_ID);
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, 10, SECOND, 0, 0);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(10), NO_WAIT), bufferResult(0,
                createPage(4),
                createPage(5),
                createPage(6),
                createPage(7),
                createPage(8),
                createPage(9),
                createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        // page not acknowledged yet so sent count is still zero
        assertQueueState(buffer, 0, SECOND, 10, 0);
        // acknowledge the 10 pages
        buffer.get(SECOND, 10, sizeOfPages(10)).cancel(true);
        assertQueueState(buffer, 0, SECOND, 0, 10);

        //
        // tell shared buffer there will be no more queues
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        buffer.setOutputBuffers(outputBuffers);

        // buffers should see the same stats and the blocked page future from above should be done
        assertQueueState(buffer, 0, FIRST, 1, 3);
        assertQueueState(buffer, 0, SECOND, 0, 10);
        assertFutureIsDone(future);

        // add 3 more pages, buffers always show the same stats
        addPage(buffer, createPage(14));
        addPage(buffer, createPage(15));
        addPage(buffer, createPage(16));
        assertQueueState(buffer, 2, FIRST, 1, 3);
        assertQueueState(buffer, 2, SECOND, 1, 10);

        // pull one page from the second buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 10, sizeOfPages(1), NO_WAIT), bufferResult(10, createPage(14)));
        assertQueueState(buffer, 2, FIRST, 1, 3);
        assertQueueState(buffer, 2, SECOND, 1, 10);

        // acknowledge the page in the first buffer and pull remaining ones
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 4, sizeOfPages(10), NO_WAIT), bufferResult(4, createPage(15), createPage(16)));
        assertQueueState(buffer, 0, FIRST, 2, 4);
        assertQueueState(buffer, 0, SECOND, 1, 10);

        //
        // finish the buffer
        assertFalse(buffer.isFinished());
        buffer.setNoMorePages();
        assertQueueState(buffer, 0, FIRST, 2, 4);
        assertQueueState(buffer, 0, SECOND, 1, 10);

        // not fully finished until all pages are consumed
        assertFalse(buffer.isFinished());

        // acknowledge the pages from the first buffer; buffer should not close automatically
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 6, sizeOfPages(10), NO_WAIT), emptyResults(6, true));
        assertQueueState(buffer, 0, FIRST, 0, 6);
        assertQueueState(buffer, 0, SECOND, 1, 10);
        assertFalse(buffer.isFinished());

        // finish first queue
        buffer.abort(FIRST);
        assertQueueClosed(buffer, 0, FIRST, 6);
        assertQueueState(buffer, 0, SECOND, 1, 10);
        assertFalse(buffer.isFinished());

        // acknowledge a page from the second queue; queue should not close automatically
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 11, sizeOfPages(1), NO_WAIT), emptyResults(11, true));
        assertQueueState(buffer, 0, SECOND, 0, 11);
        assertFalse(buffer.isFinished());

        // finish second queue
        buffer.abort(SECOND);
        assertQueueClosed(buffer, 0, FIRST, 6);
        assertQueueClosed(buffer, 0, SECOND, 11);
        assertFinished(buffer);

        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 6, sizeOfPages(10), NO_WAIT), emptyResults(6, true));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 11, sizeOfPages(10), NO_WAIT), emptyResults(11, true));
    }

    @Test
    public void testSnapshot()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY);
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        outputBuffers = outputBuffers.withBuffer(FIRST, BROADCAST_PARTITION_ID);

        // add a queue
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, 3, FIRST, 0, 0);

        //Capture and Compare with expected
        Object snapshot = buffer.capture(null);
        Map<String, Object> actual = SnapshotTestUtil.toSimpleSnapshotMapping(snapshot);
        assertEquals(actual, createExpectedMapping());

        addPage(buffer, createPage(3));

        //Restore then Capture again to Compare with expected
        buffer.restore(snapshot, null);
        snapshot = buffer.capture(null);
        actual = SnapshotTestUtil.toSimpleSnapshotMapping(snapshot);

        assertEquals(actual, createExpectedMapping());
    }

    private Map<String, Object> createExpectedMapping()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("totalPagesAdded", 3L);
        expectedMapping.put("totalRowsAdded", 3L);
        return expectedMapping;
    }

    @Test
    public void testMarkers()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY)
                .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                .withBuffer(SECOND, BROADCAST_PARTITION_ID);
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(5));

        ScheduledExecutorService scheduler = newScheduledThreadPool(4, daemonThreadsNamed("test-%s"));
        ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        TaskContext taskContext = createTaskContext(scheduler, scheduledExecutor, TEST_SNAPSHOT_SESSION);
        buffer.setTaskContext(taskContext);
        buffer.addInputChannel("id");
        buffer.setNoMoreInputChannels();

        MarkerPage marker1 = MarkerPage.snapshotPage(1);
        MarkerPage marker2 = MarkerPage.snapshotPage(2);

        // add one item
        addPage(buffer, createPage(0));
        // broadcast 2 pages
        addPage(buffer, marker1, true);
        addPage(buffer, marker2, true);
        // target clients: ?, 1, 2, 1, 2

        // first client gets all 3 elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(3), NO_WAIT),
                bufferResult(0, createPage(0), marker1, marker2));
        assertEquals(buffer.getInfo().getBuffers().stream().map(BufferInfo::getBufferedPages).collect(Collectors.toList()), Arrays.asList(3, 0));
        assertEquals(buffer.getInfo().getBuffers().stream().map(BufferInfo::getPagesSent).collect(Collectors.toList()), Arrays.asList(0L, 0L));
        // acknowledge
        buffer.get(FIRST, 3, sizeOfPages(1)).cancel(true);
        assertQueueState(buffer, 2, FIRST, 0, 3);
        assertEquals(buffer.getInfo().getBuffers().stream().map(BufferInfo::getBufferedPages).collect(Collectors.toList()), Arrays.asList(0, 0));
        assertEquals(buffer.getInfo().getBuffers().stream().map(BufferInfo::getPagesSent).collect(Collectors.toList()), Arrays.asList(3L, 0L));

        // second client gets 2 elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(3), NO_WAIT),
                bufferResult(0, marker1, marker2));
        assertEquals(buffer.getInfo().getBuffers().stream().map(BufferInfo::getBufferedPages).collect(Collectors.toList()), Arrays.asList(0, 2));
        assertEquals(buffer.getInfo().getBuffers().stream().map(BufferInfo::getPagesSent).collect(Collectors.toList()), Arrays.asList(3L, 0L));
        // acknowledge
        buffer.get(SECOND, 2, sizeOfPages(1)).cancel(true);
        assertEquals(buffer.getInfo().getBuffers().stream().map(BufferInfo::getBufferedPages).collect(Collectors.toList()), Arrays.asList(0, 0));
        assertEquals(buffer.getInfo().getBuffers().stream().map(BufferInfo::getPagesSent).collect(Collectors.toList()), Arrays.asList(3L, 2L));

        // New buffer receives pending markers
        OutputBuffers outputBuffers1 = outputBuffers.withBuffer(THIRD, BROADCAST_PARTITION_ID);
        assertNotNull(outputBuffers1);
        buffer.setOutputBuffers(outputBuffers);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, THIRD, 0, sizeOfPages(3), NO_WAIT),
                bufferResult(0, marker1, marker2));
        assertEquals(buffer.getInfo().getBuffers().stream().map(BufferInfo::getBufferedPages).collect(Collectors.toList()), Arrays.asList(0, 0, 2));
        assertEquals(buffer.getInfo().getBuffers().stream().map(BufferInfo::getPagesSent).collect(Collectors.toList()), Arrays.asList(3L, 2L, 0L));
        // acknowledge
        buffer.get(THIRD, 2, sizeOfPages(1)).cancel(true);
        assertEquals(buffer.getInfo().getBuffers().stream().map(BufferInfo::getBufferedPages).collect(Collectors.toList()), Arrays.asList(0, 0, 0));
        assertEquals(buffer.getInfo().getBuffers().stream().map(BufferInfo::getPagesSent).collect(Collectors.toList()), Arrays.asList(3L, 2L, 2L));

        // finish
        buffer.setNoMorePages();
        buffer.abort(FIRST);
        buffer.abort(SECOND);
        buffer.abort(THIRD);
        assertQueueClosed(buffer, 0, FIRST, 3);
        assertQueueClosed(buffer, 0, SECOND, 2);
        assertQueueClosed(buffer, 0, THIRD, 2);
        assertFinished(buffer);
    }

    // TODO: remove this after PR is landed: https://github.com/prestodb/presto/pull/7987
    @Test
    public void testAcknowledge()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY);
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, BROADCAST_PARTITION_ID);

        // add a queue
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, 3, FIRST, 0, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // acknowledge pages 0 and 1
        acknowledgeBufferResult(buffer, FIRST, 2);
        // only page 2 is not removed
        assertQueueState(buffer, 0, FIRST, 1, 2);
        // acknowledge page 2
        acknowledgeBufferResult(buffer, FIRST, 3);
        // nothing left
        assertQueueState(buffer, 0, FIRST, 0, 3);
        // acknowledge more pages will fail
        try {
            acknowledgeBufferResult(buffer, FIRST, 4);
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid sequence id");
        }

        // fill the buffer
        for (int i = 3; i < 6; i++) {
            addPage(buffer, createPage(i));
        }
        assertQueueState(buffer, 3, FIRST, 0, 3);

        // getting new pages will again acknowledge the previously acknowledged pages but this is ok
        buffer.get(FIRST, 3, sizeOfPages(1)).cancel(true);
        assertQueueState(buffer, 2, FIRST, 1, 3);
    }

    @Test
    public void testBufferFull()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(buffer, createPage(1));
        addPage(buffer, createPage(2));

        // third page is blocked
        enqueuePage(buffer, createPage(3));
    }

    @Test
    public void testDuplicateRequests()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        // add a queue
        assertQueueState(buffer, 3, FIRST, 0, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, 0, FIRST, 3, 0);

        // get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, 0, FIRST, 3, 0);

        // acknowledge the pages
        buffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);

        // attempt to get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(0, false));
        // pages are acknowledged
        assertQueueState(buffer, 0, FIRST, 0, 3);
    }

    @Test
    public void testAddQueueAfterCreation()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        assertFalse(buffer.isFinished());

        try {
            buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY)
                    .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                    .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                    .withNoMoreBufferIds());
            fail("Expected IllegalStateException from addQueue after noMoreQueues has been called");
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testAddAfterFinish()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        buffer.setNoMorePages();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(1));
        assertEquals(buffer.getInfo().getTotalPagesSent(), 0);
    }

    @Test
    public void testAddQueueAfterNoMoreQueues()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // tell buffer no more queues will be added
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        // set no more queues a second time to assure that we don't get an exception or such
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        // set no more queues a third time to assure that we don't get an exception or such
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        try {
            OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY)
                    .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                    .withNoMoreBufferIds();

            buffer.setOutputBuffers(outputBuffers);
            fail("Expected IllegalStateException from addQueue after noMoreQueues has been called");
        }
        catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testAddAfterDestroy()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        buffer.destroy();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(1));
        assertEquals(buffer.getInfo().getTotalPagesSent(), 0);
    }

    @Test
    public void testGetBeforeCreate()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0L, sizeOfPages(1));
        assertFalse(future.isDone());

        // add a page and verify the future is complete
        addPage(buffer, createPage(33));
        assertTrue(future.isDone());
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(33)));
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No more buffers already set")
    public void testUseUndeclaredBufferAfterFinalBuffersSet()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that was not declared, which will fail
        buffer.get(SECOND, 0L, sizeOfPages(1));
    }

    @Test
    public void testAbortBeforeCreate()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        assertFalse(buffer.isFinished());

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0L, sizeOfPages(1));
        assertFalse(future.isDone());

        // abort that buffer, and verify the future is finishd
        buffer.abort(FIRST);
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(0, false));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(0, true));

        // add a page and verify the future is not complete
        addPage(buffer, createPage(33));

        // add the buffer and verify we did not get the page
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, 0));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(0, true));
    }

    @Test
    public void testFullBufferBlocksWriter()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(buffer, createPage(1));
        addPage(buffer, createPage(2));

        // third page is blocked
        enqueuePage(buffer, createPage(3));
    }

    @Test
    public void testAbort()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 10; i++) {
            addPage(buffer, createPage(i));
        }
        buffer.setNoMorePages();

        // add one output buffer
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, 0);
        buffer.setOutputBuffers(outputBuffers);

        // read a page from the first buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));

        // abort buffer, and verify page cannot be acknowledged
        buffer.abort(FIRST);
        assertQueueClosed(buffer, 9, FIRST, 0);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(1), NO_WAIT), emptyResults(0, true));

        outputBuffers = outputBuffers.withBuffer(SECOND, 0).withNoMoreBufferIds();
        buffer.setOutputBuffers(outputBuffers);

        // first page is lost because the first buffer was aborted
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(1)));
        buffer.abort(SECOND);
        assertQueueClosed(buffer, 0, SECOND, 0);
        assertFinished(buffer);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 1, sizeOfPages(1), NO_WAIT), emptyResults(0, true));
    }

    @Test
    public void testFinishClosesEmptyQueues()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // finish while queues are empty
        buffer.setNoMorePages();

        assertQueueState(buffer, 0, FIRST, 0, 0);
        assertQueueState(buffer, 0, SECOND, 0, 0);

        buffer.abort(FIRST);
        buffer.abort(SECOND);

        assertQueueClosed(buffer, 0, FIRST, 0);
        assertQueueClosed(buffer, 0, SECOND, 0);
    }

    @Test
    public void testAbortFreesReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, 0));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // abort the buffer
        buffer.abort(FIRST);
        assertQueueClosed(buffer, 0, FIRST, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(1, false));
    }

    @Test
    public void testFinishFreesReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, 0));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // finish the buffer
        assertQueueState(buffer, 0, FIRST, 0, 1);
        buffer.abort(FIRST);
        assertQueueClosed(buffer, 0, FIRST, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(1, false));
    }

    @Test
    public void testFinishFreesWriter()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(5));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY)
                .withBuffer(FIRST, 0)
                .withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // enqueue the addition two pages more pages
        ListenableFuture<?> firstEnqueuePage = enqueuePage(buffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(buffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        buffer.get(FIRST, 1, sizeOfPages(100)).cancel(true);

        // verify we are still blocked because the buffer is full
        assertFalse(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // finish the query
        buffer.setNoMorePages();
        assertFalse(buffer.isFinished());

        // verify futures are complete
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);

        // get and acknowledge the last 5 pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(100), NO_WAIT),
                bufferResult(1, createPage(1), createPage(2), createPage(3), createPage(4), createPage(5), createPage(6)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 7, sizeOfPages(100), NO_WAIT), emptyResults(7, true));

        // verify not finished
        assertFalse(buffer.isFinished());

        // finish the queue
        buffer.abort(FIRST);

        // verify finished
        assertFinished(buffer);
    }

    @Test
    public void testDestroyFreesReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(5));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY)
                .withBuffer(FIRST, 0)
                .withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one page
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // destroy the buffer
        buffer.destroy();
        assertQueueClosed(buffer, 0, FIRST, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(1, false));
    }

    @Test
    public void testDestroyFreesWriter()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(5));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY)
                .withBuffer(FIRST, 0)
                .withNoMoreBufferIds());
        assertFalse(buffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // add two pages to the buffer queue
        ListenableFuture<?> firstEnqueuePage = enqueuePage(buffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(buffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        buffer.get(FIRST, 1, sizeOfPages(1)).cancel(true);

        // verify we are still blocked because the buffer is full
        assertFalse(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // destroy the buffer (i.e., cancel the query)
        buffer.destroy();
        assertFinished(buffer);

        // verify the futures are completed
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);
    }

    @Test
    public void testFailDoesNotFreeReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one page
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());

        // fail the buffer
        buffer.fail();

        // future should have not finished
        assertFalse(future.isDone());

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());
    }

    @Test
    public void testFailFreesWriter()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                createInitialEmptyOutputBuffers(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertFalse(buffer.isFinished());

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // add two pages to the buffer queue
        ListenableFuture<?> firstEnqueuePage = enqueuePage(buffer, createPage(5));
        ListenableFuture<?> secondEnqueuePage = enqueuePage(buffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        buffer.get(FIRST, 1, sizeOfPages(1)).cancel(true);

        // verify we are still blocked because the buffer is full
        assertFalse(firstEnqueuePage.isDone());
        assertFalse(secondEnqueuePage.isDone());

        // fail the buffer (i.e., cancel the query)
        buffer.fail();
        assertFalse(buffer.isFinished());

        // verify the futures are completed
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);
    }

    @Test
    public void testAddBufferAfterFail()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY)
                .withBuffer(FIRST, BROADCAST_PARTITION_ID);
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(5));
        assertFalse(buffer.isFinished());

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one page
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // fail the buffer
        buffer.fail();

        // add a buffer
        outputBuffers = outputBuffers.withBuffer(SECOND, BROADCAST_PARTITION_ID);
        buffer.setOutputBuffers(outputBuffers);

        // attempt to get page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());
        future = buffer.get(SECOND, 0, sizeOfPages(10));
        assertFalse(future.isDone());

        // set no more buffers
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        buffer.setOutputBuffers(outputBuffers);

        // attempt to get page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertFalse(future.isDone());
        future = buffer.get(SECOND, 0, sizeOfPages(10));
        assertFalse(future.isDone());
    }

    @Test
    public void testBufferCompletion()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(5));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY)
                .withBuffer(FIRST, 0)
                .withNoMoreBufferIds());

        assertFalse(buffer.isFinished());

        // fill the buffer
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Page page = createPage(i);
            addPage(buffer, page);
            pages.add(page);
        }

        buffer.setNoMorePages();

        // get and acknowledge 5 pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(5), MAX_WAIT), createBufferResult(0, pages));

        // buffer is not finished
        assertFalse(buffer.isFinished());

        // there are no more pages and no more buffers, but buffer is not finished because it didn't receive an acknowledgement yet
        assertFalse(buffer.isFinished());

        // ask the buffer to finish
        buffer.abort(FIRST);

        // verify that the buffer is finished
        assertTrue(buffer.isFinished());
    }

    @Test
    public void testNoMorePagesFreesReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        buffer.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, 0));
        assertFalse(buffer.isFinished());

        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));
        assertFalse(future.isDone());

        buffer.setNoMorePages();

        assertTrue(future.isDone());
        assertTrue(buffer.get(FIRST, 0, sizeOfPages(10)).isDone());
    }

    @Test
    public void testFinishBeforeNoMoreBuffers()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }
        buffer.setNoMorePages();
        assertFalse(buffer.isFinished());

        // add one output buffer
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY).withBuffer(FIRST, 0);
        buffer.setOutputBuffers(outputBuffers);
        assertFalse(buffer.isFinished());

        // read a page from the first buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        assertFalse(buffer.isFinished());

        // read remaining pages from the first buffer and acknowledge
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(10), NO_WAIT), bufferResult(1, createPage(1), createPage(2)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 3, sizeOfPages(1), NO_WAIT), emptyResults(3, true));
        assertFalse(buffer.isFinished());

        // finish first queue
        buffer.abort(FIRST);
        assertQueueClosed(buffer, 0, FIRST, 3);
        assertFinished(buffer);

        // add another buffer after finish
        outputBuffers = outputBuffers.withBuffer(SECOND, 0);
        buffer.setOutputBuffers(outputBuffers);

        // verify second buffer has no results
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(1), NO_WAIT), emptyResults(0, true));
    }

    @Test
    public void testForceFreeMemory()
            throws Throwable
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(createInitialEmptyOutputBuffers(ARBITRARY), sizeOfPages(10));
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }
        OutputBufferMemoryManager memoryManager = buffer.getMemoryManager();
        assertTrue(memoryManager.getBufferedBytes() > 0);
        buffer.forceFreeMemory();
        assertEquals(memoryManager.getBufferedBytes(), 0);
        // adding a page after forceFreeMemory() should be NOOP
        addPage(buffer, createPage(1));
        assertEquals(memoryManager.getBufferedBytes(), 0);
    }

    private static BufferResult getBufferResult(OutputBuffer buffer, OutputBufferId bufferId, long sequenceId, DataSize maxSize, Duration maxWait)
    {
        ListenableFuture<BufferResult> future = buffer.get(bufferId, sequenceId, maxSize);
        return getFuture(future, maxWait);
    }

    private static void assertQueueState(
            OutputBuffer buffer,
            int unassignedPages,
            OutputBufferId bufferId,
            int bufferedPages,
            int pagesSent)
    {
        OutputBufferInfo outputBufferInfo = buffer.getInfo();

        long assignedPages = outputBufferInfo.getBuffers().stream().mapToInt(BufferInfo::getBufferedPages).sum();

        assertEquals(
                outputBufferInfo.getTotalBufferedPages() - assignedPages,
                unassignedPages,
                "unassignedPages");

        BufferInfo bufferInfo = outputBufferInfo.getBuffers().stream()
                .filter(info -> info.getBufferId().equals(bufferId))
                .findAny()
                .orElse(null);

        assertEquals(
                bufferInfo,
                new BufferInfo(
                        bufferId,
                        false,
                        bufferedPages,
                        pagesSent,
                        new PageBufferInfo(
                                bufferId.getId(),
                                bufferedPages,
                                sizeOfPages(bufferedPages).toBytes(),
                                bufferedPages + pagesSent, // every page has one row
                                bufferedPages + pagesSent)));
    }

    @SuppressWarnings("ConstantConditions")
    private static void assertQueueClosed(OutputBuffer buffer, int unassignedPages, OutputBufferId bufferId, int pagesSent)
    {
        OutputBufferInfo outputBufferInfo = buffer.getInfo();

        long assignedPages = outputBufferInfo.getBuffers().stream().mapToInt(BufferInfo::getBufferedPages).sum();
        assertEquals(
                outputBufferInfo.getTotalBufferedPages() - assignedPages,
                unassignedPages,
                "unassignedPages");

        BufferInfo bufferInfo = outputBufferInfo.getBuffers().stream()
                .filter(info -> info.getBufferId().equals(bufferId))
                .findAny()
                .orElse(null);

        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getPagesSent(), pagesSent);
        assertTrue(bufferInfo.isFinished());
    }

    private ArbitraryOutputBuffer createArbitraryBuffer(OutputBuffers buffers, DataSize dataSize)
    {
        ArbitraryOutputBuffer buffer = new ArbitraryOutputBuffer(
                new StateMachine<>("bufferState", stateNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES),
                dataSize,
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                stateNotificationExecutor);
        buffer.setOutputBuffers(buffers);
        return buffer;
    }

    private static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return createBufferResult(token, pages);
    }

    // The following tests apply equally to all 3 types of output buffers, so only specify them here once

    @DataProvider
    public static Object[][] bufferTypes()
    {
        return new Object[][] {{ARBITRARY}, {BROADCAST}, {PARTITIONED}};
    }

    @Test(expectedExceptions = IllegalArgumentException.class, dataProvider = "bufferTypes")
    public void testSetNullTaskContext(OutputBuffers.BufferType bufferType)
    {
        OutputBuffer buffer = createOutputBuffer(bufferType);
        buffer.setTaskContext(null);
    }

    @Test(expectedExceptions = IllegalStateException.class, dataProvider = "bufferTypes")
    public void testSetMultipleTaskContext(OutputBuffers.BufferType bufferType)
    {
        OutputBuffer buffer = createOutputBuffer(bufferType);
        TaskContext taskContext = BufferTestUtils.newTestingTaskContext();
        buffer.setTaskContext(taskContext);
        buffer.setTaskContext(taskContext);
    }

    @Test(expectedExceptions = IllegalStateException.class, dataProvider = "bufferTypes")
    public void testSetMultipleNoMoreInputChannels(OutputBuffers.BufferType bufferType)
    {
        OutputBuffer buffer = createOutputBuffer(bufferType);
        buffer.addInputChannel("1");
        buffer.setNoMoreInputChannels();
        buffer.addInputChannel("2");
        buffer.setNoMoreInputChannels();
    }

    @Test(dataProvider = "bufferTypes")
    public void testGetInputChannels(OutputBuffers.BufferType bufferType)
    {
        OutputBuffer buffer = createOutputBuffer(bufferType);

        buffer.addInputChannel("1");
        Optional<Set<String>> inputChannels = ((MultiInputRestorable) buffer).getInputChannels();
        assertFalse(inputChannels.isPresent());

        buffer.addInputChannel("2");
        buffer.setNoMoreInputChannels();
        inputChannels = ((MultiInputRestorable) buffer).getInputChannels();
        assertTrue(inputChannels.isPresent());
        assertEquals(inputChannels.get(), Sets.newHashSet("1", "2"));
    }

    @Test(dataProvider = "bufferTypes")
    public void testSnapshotState(OutputBuffers.BufferType bufferType)
    {
        OutputBuffer buffer = createOutputBuffer(bufferType);
        TaskContext taskContext = BufferTestUtils.newTestingTaskContext();
        buffer.setTaskContext(taskContext);

        buffer.addInputChannel("1");
        buffer.addInputChannel("2");
        buffer.setNoMoreInputChannels();
        MarkerPage markerPage = MarkerPage.snapshotPage(1);
        SerializedPage marker = SerializedPage.forMarker(markerPage);

        buffer.enqueue(Collections.singletonList(marker), "1");
        List<SerializedPage> pages = getBufferResult(buffer, FIRST, 0, sizeOfPages(3), NO_WAIT).getSerializedPages();
        assertEquals(pages.size(), 1);
        assertTrue(pages.get(0).isMarkerPage());
        // Acknowledge
        buffer.get(FIRST, 1, sizeOfPages(1)).cancel(true);

        buffer.enqueue(Collections.singletonList(marker), "2");
        pages = getBufferResult(buffer, FIRST, 0, sizeOfPages(3), NO_WAIT).getSerializedPages();
        assertEquals(pages.size(), 0);
    }

    private OutputBuffer createOutputBuffer(OutputBuffers.BufferType bufferType)
    {
        OutputBuffers buffers = createInitialEmptyOutputBuffers(bufferType).withBuffer(FIRST, BROADCAST_PARTITION_ID).withNoMoreBufferIds();
        OutputBuffer buffer;
        switch (bufferType) {
            case ARBITRARY:
                buffer = new ArbitraryOutputBuffer(
                        new StateMachine<>("bufferState", stateNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES),
                        sizeOfPages(5),
                        () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                        stateNotificationExecutor);
                buffer.setOutputBuffers(buffers);
                break;
            case BROADCAST:
                buffer = new BroadcastOutputBuffer(
                        new StateMachine<>("bufferState", stateNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES),
                        sizeOfPages(5),
                        () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                        stateNotificationExecutor);
                buffer.setOutputBuffers(buffers);
                break;
            case PARTITIONED:
                buffer = new PartitionedOutputBuffer(
                        new StateMachine<>("bufferState", stateNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES),
                        buffers,
                        sizeOfPages(5),
                        () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                        stateNotificationExecutor);
                break;
            default:
                throw new RuntimeException("Unexpected bufferType: " + bufferType);
        }
        return buffer;
    }
}
