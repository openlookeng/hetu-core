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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.block.BlockAssertions;
import io.prestosql.failuredetector.NoOpFailureDetector;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.snapshot.MarkerPage;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.testing.TestingPagesSerdeFactory.testingPagesSerde;
import static io.prestosql.testing.TestingSnapshotUtils.NOOP_SNAPSHOT_UTILS;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestExchangeClient
{
    private ScheduledExecutorService scheduler;
    private ExecutorService pageBufferClientCallbackExecutor;

    private static final PagesSerde PAGES_SERDE = testingPagesSerde();

    @BeforeClass
    public void setUp()
    {
        scheduler = newScheduledThreadPool(4, daemonThreadsNamed("test-%s"));
        pageBufferClientCallbackExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        if (pageBufferClientCallbackExecutor != null) {
            pageBufferClientCallbackExecutor.shutdownNow();
            pageBufferClientCallbackExecutor = null;
        }
    }

    @Test
    public void testHappyPath()
    {
        DataSize maxResponseSize = new DataSize(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        String instanceId = "testing instance id";
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(32, Unit.MEGABYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor, new NoOpFailureDetector());

        exchangeClient.addLocation(new TaskLocation(location, instanceId));
        exchangeClient.noMoreLocations();

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(1));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(2));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(3));
        assertNull(getNextPage(exchangeClient));
        assertEquals(exchangeClient.isClosed(), true);

        ExchangeClientStatus status = exchangeClient.getStatus();
        assertEquals(status.getBufferedPages(), 0);
        assertEquals(status.getBufferedBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        assertStatus(status.getPageBufferClientStatuses().get(0), location, "closed", 3, 3, 3, "not scheduled");
    }

    @Test
    public void testMarkers()
    {
        DataSize maxResponseSize = new DataSize(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        String instanceId = "testing instance id";
        processor.addPage(location, createPage(1));
        MarkerPage marker = MarkerPage.snapshotPage(1);
        processor.addPage(location, marker);
        processor.addPage(location, createPage(2));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(32, Unit.MEGABYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor, new NoOpFailureDetector());
        exchangeClient.setSnapshotEnabled(NOOP_SNAPSHOT_UTILS.getQuerySnapshotManager(new QueryId("query")));

        final String target1 = "target1";
        final String target2 = "target2";
        exchangeClient.addLocation(new TaskLocation(location, instanceId));
        exchangeClient.noMoreLocations();
        exchangeClient.addTarget(target1);
        exchangeClient.addTarget(target2);

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient, target1), createPage(1));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient, target2), marker);
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient, target2), createPage(2));
        assertEquals(exchangeClient.isClosed(), false);
        // Target2 won't get any page, but because there are pending markers, it won't cause the client to close either
        assertTrue(exchangeClient.isBlocked().isDone());
        assertNull(getNextPage(exchangeClient, target2));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient, target1), marker);

        // New target receives pending marker
        assertEquals(exchangeClient.isClosed(), false);
        final String target3 = "target3";
        exchangeClient.addTarget(target3);
        assertPageEquals(getNextPage(exchangeClient, target3), marker);
        assertFalse(exchangeClient.isBlocked().isDone());
        assertEquals(exchangeClient.isClosed(), false);

        exchangeClient.noMoreTargets();
        assertNull(getNextPage(exchangeClient));
        assertEquals(exchangeClient.isClosed(), true);
        ExchangeClientStatus status = exchangeClient.getStatus();
        assertEquals(status.getBufferedPages(), 0);
        assertEquals(status.getBufferedBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        assertStatus(status.getPageBufferClientStatuses().get(0), location, "closed", 3, 3, 3, "not scheduled");
    }

    @Test
    public void testAddTarget()
    {
        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(32, Unit.MEGABYTE),
                new DataSize(10, Unit.MEGABYTE),
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                mock(HttpClient.class),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor, new NoOpFailureDetector());
        exchangeClient.setSnapshotEnabled(NOOP_SNAPSHOT_UTILS.getQuerySnapshotManager(new QueryId("query")));

        String origin1 = "location1";
        String origin2 = "location2";
        final String target1 = "target1";
        final String target2 = "target2";

        PagesSerde serde = testingPagesSerde();
        exchangeClient.addTarget(target1);
        exchangeClient.addPages(ImmutableList.of(serde.serialize(createPage(1))), origin1);
        MarkerPage marker = MarkerPage.snapshotPage(1);
        exchangeClient.addPages(ImmutableList.of(SerializedPage.forMarker(marker)), origin1);
        exchangeClient.addPages(ImmutableList.of(serde.serialize(createPage(2))), origin2);

        assertPageEquals(getNextPage(exchangeClient, target1), createPage(1));
        assertPageEquals(getNextPage(exchangeClient, target1), marker);

        exchangeClient.addTarget(target2);
        assertPageEquals(getNextPage(exchangeClient, target2, origin1), marker);
        assertPageEquals(getNextPage(exchangeClient, target1, origin2), createPage(2));
    }

    @Test(timeOut = 10000)
    public void testAddLocation()
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(32, Unit.MEGABYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor, new NoOpFailureDetector());

        URI location1 = URI.create("http://localhost:8081/foo");
        String instanceId1 = "testing instance id";
        processor.addPage(location1, createPage(1));
        processor.addPage(location1, createPage(2));
        processor.addPage(location1, createPage(3));
        processor.setComplete(location1);
        exchangeClient.addLocation(new TaskLocation(location1, instanceId1));

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(1));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(2));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(3));

        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());
        assertEquals(exchangeClient.isClosed(), false);

        URI location2 = URI.create("http://localhost:8082/bar");
        String instanceId2 = "testing instance id2";
        processor.addPage(location2, createPage(4));
        processor.addPage(location2, createPage(5));
        processor.addPage(location2, createPage(6));
        processor.setComplete(location2);
        exchangeClient.addLocation(new TaskLocation(location2, instanceId2));

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(4));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(5));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(6));

        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());
        assertEquals(exchangeClient.isClosed(), false);

        exchangeClient.noMoreLocations();
        // The transition to closed may happen asynchronously, since it requires that all the HTTP clients
        // receive a final GONE response, so just spin until it's closed or the test times out.
        while (!exchangeClient.isClosed()) {
            Thread.sleep(1);
        }

        ImmutableMap<URI, PageBufferClientStatus> statuses = uniqueIndex(exchangeClient.getStatus().getPageBufferClientStatuses(), PageBufferClientStatus::getUri);
        assertStatus(statuses.get(location1), location1, "closed", 3, 3, 3, "not scheduled");
        assertStatus(statuses.get(location2), location2, "closed", 3, 3, 3, "not scheduled");
    }

    @Test
    public void testBufferLimit()
    {
        DataSize maxResponseSize = new DataSize(1, Unit.BYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        String instanceId = "testing instance id";

        // add a pages
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(1, Unit.BYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor, new NoOpFailureDetector());

        exchangeClient.addLocation(new TaskLocation(location, instanceId));
        exchangeClient.noMoreLocations();
        assertEquals(exchangeClient.isClosed(), false);

        long start = System.nanoTime();

        // start fetching pages
        exchangeClient.scheduleRequestIfNecessary();
        // wait for a page to be fetched
        do {
            // there is no thread coordination here, so sleep is the best we can do
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (exchangeClient.getStatus().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() > 0);
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "queued", 1, 1, 1, "not scheduled");

        // remove the page and wait for the client to fetch another page
        assertPageEquals(exchangeClient.pollPage(null).getLeft(), createPage(1));
        do {
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (exchangeClient.getStatus().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "queued", 2, 2, 2, "not scheduled");
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() > 0);

        // remove the page and wait for the client to fetch another page
        assertPageEquals(exchangeClient.pollPage(null).getLeft(), createPage(2));
        do {
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (exchangeClient.getStatus().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "queued", 3, 3, 3, "not scheduled");
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() > 0);

        // remove last page
        assertPageEquals(getNextPage(exchangeClient), createPage(3));

        //  wait for client to decide there are no more pages
        assertNull(getNextPage(exchangeClient));
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() == 0);
        assertEquals(exchangeClient.isClosed(), true);
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "closed", 3, 5, 5, "not scheduled");
    }

    @Test
    public void testClose()
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(1, Unit.BYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        String instanceId = "testing instance id";
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(1, Unit.BYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor, new NoOpFailureDetector());
        exchangeClient.addLocation(new TaskLocation(location, instanceId));
        exchangeClient.noMoreLocations();

        // fetch a page
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(1));

        // close client while pages are still available
        exchangeClient.close();
        while (!exchangeClient.isFinished()) {
            MILLISECONDS.sleep(10);
        }
        assertEquals(exchangeClient.isClosed(), true);
        Pair<SerializedPage, String> pair = exchangeClient.pollPage(null);
        assertNotNull(pair);
        assertNull(pair.getLeft());
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);
        assertEquals(exchangeClient.getStatus().getBufferedBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        PageBufferClientStatus clientStatus = exchangeClient.getStatus().getPageBufferClientStatuses().get(0);
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), "closed", "status");
        assertEquals(clientStatus.getHttpRequestState(), "not scheduled", "httpRequestState");
    }

    private static Page createPage(int size)
    {
        return new Page(BlockAssertions.createLongSequenceBlock(0, size));
    }

    private static SerializedPage getNextPage(ExchangeClient exchangeClient)
    {
        return getNextPage(exchangeClient, null);
    }

    private static SerializedPage getNextPage(ExchangeClient exchangeClient, String target)
    {
        ListenableFuture<SerializedPage> futurePage = Futures.transform(exchangeClient.isBlocked(), ignored -> exchangeClient.pollPage(target).getLeft(), directExecutor());
        return tryGetFutureValue(futurePage, 100, TimeUnit.SECONDS).orElse(null);
    }

    private static SerializedPage getNextPage(ExchangeClient exchangeClient, String target, String expectedOrigin)
    {
        ListenableFuture<SerializedPage> futurePage = Futures.transform(exchangeClient.isBlocked(), ignored -> {
            Pair<SerializedPage, String> result = exchangeClient.pollPage(target);
            assertEquals(result.getRight(), expectedOrigin);
            return result.getLeft();
        }, directExecutor());
        return tryGetFutureValue(futurePage, 100, TimeUnit.SECONDS).orElse(null);
    }

    private static void assertPageEquals(SerializedPage actualPage, Page expectedPage)
    {
        assertNotNull(actualPage);
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        assertEquals(PAGES_SERDE.deserialize(actualPage).getChannelCount(), expectedPage.getChannelCount());
    }

    private static void assertStatus(PageBufferClientStatus clientStatus,
            URI location,
            String status,
            int pagesReceived,
            int requestsScheduled,
            int requestsCompleted,
            String httpRequestState)
    {
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), status, "status");
        assertEquals(clientStatus.getPagesReceived(), pagesReceived, "pagesReceived");
        assertEquals(clientStatus.getRequestsScheduled(), requestsScheduled, "requestsScheduled");
        assertEquals(clientStatus.getRequestsCompleted(), requestsCompleted, "requestsCompleted");
        assertEquals(clientStatus.getHttpRequestState(), httpRequestState, "httpRequestState");
    }
}
