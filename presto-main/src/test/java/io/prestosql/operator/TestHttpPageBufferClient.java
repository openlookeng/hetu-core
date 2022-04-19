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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.failuredetector.FailureDetector;
import io.prestosql.failuredetector.NoOpFailureDetector;
import io.prestosql.operator.HttpPageBufferClient.ClientCallback;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Page;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertContains;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.PrestoMediaTypes.PRESTO_PAGES;
import static io.prestosql.spi.StandardErrorCode.PAGE_TOO_LARGE;
import static io.prestosql.spi.StandardErrorCode.PAGE_TRANSPORT_ERROR;
import static io.prestosql.spi.StandardErrorCode.PAGE_TRANSPORT_TIMEOUT;
import static io.prestosql.testing.TestingPagesSerdeFactory.testingPagesSerde;
import static io.prestosql.util.Failures.WORKER_NODE_ERROR;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

public class TestHttpPageBufferClient
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
            throws Exception
    {
        Page expectedPage = new Page(100);

        DataSize expectedMaxSize = new DataSize(11, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(expectedMaxSize);

        CyclicBarrier requestComplete = new CyclicBarrier(2);

        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        String instanceId = "testing instance id";
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                expectedMaxSize,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TaskLocation(location, instanceId),
                callback,
                scheduler,
                pageBufferClientCallbackExecutor,
                false,
                null, new NoOpFailureDetector(), false, 10);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled");

        // fetch a page and verify
        processor.addPage(location, expectedPage);
        callback.resetStats();
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 1);
        assertPageEquals(expectedPage, callback.getPages().get(0));
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertStatus(client, location, "queued", 1, 1, 1, 0, "not scheduled");

        // fetch no data and verify
        callback.resetStats();
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertStatus(client, location, "queued", 1, 2, 2, 0, "not scheduled");

        // fetch two more pages and verify
        processor.addPage(location, expectedPage);
        processor.addPage(location, expectedPage);
        callback.resetStats();
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 2);
        assertPageEquals(expectedPage, callback.getPages().get(0));
        assertPageEquals(expectedPage, callback.getPages().get(1));
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 0);
        callback.resetStats();
        assertStatus(client, location, "queued", 3, 3, 3, 0, "not scheduled");

        // finish and verify
        callback.resetStats();
        processor.setComplete(location);
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);

        // get the buffer complete signal
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);

        // schedule the delete call to the buffer
        callback.resetStats();
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getFinishedBuffers(), 1);

        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 0);
        assertEquals(callback.getFailedBuffers(), 0);

        assertStatus(client, location, "closed", 3, 5, 5, 0, "not scheduled");
    }

    @Test
    public void testLifecycle()
            throws Exception
    {
        CyclicBarrier beforeRequest = new CyclicBarrier(2);
        CyclicBarrier afterRequest = new CyclicBarrier(2);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);
        processor.setResponse(new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]));

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        String instanceId = "testing instance id";
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                new DataSize(10, Unit.MEGABYTE),
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TaskLocation(location, instanceId),
                callback,
                scheduler,
                pageBufferClientCallbackExecutor,
                false,
                null, new NoOpFailureDetector(), false, 10);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled");

        client.scheduleRequest();
        beforeRequest.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "running", 0, 1, 0, 0, "PROCESSING_REQUEST");
        assertEquals(client.isRunning(), true);
        afterRequest.await(10, TimeUnit.SECONDS);

        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "queued", 0, 1, 1, 1, "not scheduled");

        client.close();
        beforeRequest.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 1, 1, 1, "PROCESSING_REQUEST");
        afterRequest.await(10, TimeUnit.SECONDS);
        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 1, 2, 1, "not scheduled");
    }

    @Test
    public void testInvalidResponses()
            throws Exception
    {
        CyclicBarrier beforeRequest = new CyclicBarrier(1);
        CyclicBarrier afterRequest = new CyclicBarrier(1);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        String instanceId = "testing instance id";
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                new DataSize(10, Unit.MEGABYTE),
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TaskLocation(location, instanceId),
                callback,
                scheduler,
                pageBufferClientCallbackExecutor,
                false,
                null, new NoOpFailureDetector(), false, 10);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled");

        // send not found response and verify response was ignored
        processor.setResponse(new TestingResponse(HttpStatus.NOT_FOUND, ImmutableListMultimap.of(CONTENT_TYPE,
                PRESTO_PAGES), new byte[0]));
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), "Expected response code to be 200, but was 404");
        assertStatus(client, location, "queued", 0, 1, 1, 1, "not scheduled");

        // send invalid content type response and verify response was ignored
        callback.resetStats();
        processor.setResponse(new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, "INVALID_TYPE"), new byte[0]));
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), "Expected application/x-presto-pages response from server but got INVALID_TYPE");
        assertStatus(client, location, "queued", 0, 2, 2, 2, "not scheduled");

        // send unexpected content type response and verify response was ignored
        callback.resetStats();
        processor.setResponse(new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, "text/plain"), new byte[0]));
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), "Expected application/x-presto-pages response from server but got text/plain");
        assertStatus(client, location, "queued", 0, 3, 3, 3, "not scheduled");

        // close client and verify
        client.close();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 3, 4, 3, "not scheduled");
    }

    @Test
    public void testCloseDuringPendingRequest()
            throws Exception
    {
        CyclicBarrier beforeRequest = new CyclicBarrier(2);
        CyclicBarrier afterRequest = new CyclicBarrier(2);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);
        processor.setResponse(new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]));

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        String instanceId = "testing instance id";
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                new DataSize(10, Unit.MEGABYTE),
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TaskLocation(location, instanceId),
                callback,
                scheduler,
                pageBufferClientCallbackExecutor,
                false,
                null, new NoOpFailureDetector(), false, 10);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled");

        // send request
        client.scheduleRequest();
        beforeRequest.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "running", 0, 1, 0, 0, "PROCESSING_REQUEST");
        assertEquals(client.isRunning(), true);
        // request is pending, now close it
        client.close();

        try {
            requestComplete.await(10, TimeUnit.SECONDS);
        }
        catch (BrokenBarrierException ignored) {
            // the exception could be ignored
        }
        try {
            afterRequest.await(10, TimeUnit.SECONDS);
        }
        catch (BrokenBarrierException ignored) {
            afterRequest.reset();
        }
        // client.close() triggers a DELETE request, so wait for it to finish
        beforeRequest.await(10, TimeUnit.SECONDS);
        afterRequest.await(10, TimeUnit.SECONDS);
        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 1, 2, 1, "not scheduled");
    }

    @Test
    public void testExceptionFromResponseHandler()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        AtomicReference<Duration> tickerIncrement = new AtomicReference<>(new Duration(0, TimeUnit.SECONDS));

        TestingHttpClient.Processor processor = (input) -> {
            Duration delta = tickerIncrement.get();
            ticker.increment(delta.toMillis(), TimeUnit.MILLISECONDS);
            throw new RuntimeException("Foo");
        };

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        String instanceId = "testing instance id";
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                new DataSize(10, Unit.MEGABYTE),
                new Duration(30, TimeUnit.SECONDS),
                true,
                new TaskLocation(location, instanceId),
                callback,
                scheduler,
                ticker,
                pageBufferClientCallbackExecutor,
                false,
                null, new NoOpFailureDetector(), true, 10);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled");

        // request processor will throw exception, verify the request is marked a completed
        // this starts the error stopwatch
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 0);
        assertStatus(client, location, "queued", 0, 1, 1, 1, "not scheduled");

        // advance time forward, but not enough to fail the client
        tickerIncrement.set(new Duration(30, TimeUnit.SECONDS));

        // verify that the client has not failed
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 2);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 0);
        assertStatus(client, location, "queued", 0, 2, 2, 2, "not scheduled");

        // advance time forward beyond the minimum error duration
        tickerIncrement.set(new Duration(31, TimeUnit.SECONDS));

        // verify that the client has failed
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 3);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportTimeoutException.class);
        assertContains(callback.getFailure().getMessage(), WORKER_NODE_ERROR + " (http://localhost:8080/0 - 3 failures, failure duration 31.00s, total failed request time 31.00s)");
        assertStatus(client, location, "queued", 0, 3, 3, 3, "not scheduled");
    }

    @Test
    public void testMaxRetryFailedRemoteHostGone()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        AtomicReference<Duration> tickerIncrement = new AtomicReference<>(new Duration(0, TimeUnit.SECONDS));

        TestingHttpClient.Processor processor = (input) -> {
            Duration delta = tickerIncrement.get();
            ticker.increment(delta.toMillis(), TimeUnit.MILLISECONDS);
            throw new RuntimeException("Foo");
        };

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        String instanceId = "testing instance id";
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                new DataSize(10, Unit.MEGABYTE),
                new Duration(300, TimeUnit.SECONDS),
                true,
                new TaskLocation(location, instanceId),
                callback,
                scheduler,
                ticker,
                pageBufferClientCallbackExecutor,
                false,
                null, new MockNodeCrashFailureDetector(), false, 100);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled");

        for (int i = 0; i < 101; i++) {
            client.scheduleRequest();
            requestComplete.await(10, TimeUnit.SECONDS);
            tickerIncrement.set(new Duration(1, TimeUnit.SECONDS));
        }
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 101);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 2);
        assertInstanceOf(callback.getFailure(), PageTransportTimeoutException.class);
        assertContains(callback.getFailure().getMessage(), WORKER_NODE_ERROR + " (http://localhost:8080/0 - 100 failures,");
        assertStatus(client, location, "queued", 0, 101, 101, 101, "not scheduled");
    }

    @Test
    public void testMaxRetryFailedRemoteHostAliveThenTimeout()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        AtomicReference<Duration> tickerIncrement = new AtomicReference<>(new Duration(0, TimeUnit.SECONDS));

        TestingHttpClient.Processor processor = (input) -> {
            Duration delta = tickerIncrement.get();
            ticker.increment(delta.toMillis(), TimeUnit.MILLISECONDS);
            throw new RuntimeException("Foo");
        };

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        String instanceId = "testing instance id";
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                new DataSize(10, Unit.MEGABYTE),
                new Duration(300, TimeUnit.SECONDS),
                true,
                new TaskLocation(location, instanceId),
                callback,
                scheduler,
                ticker,
                pageBufferClientCallbackExecutor,
                false,
                null, new MockActiveFailureDetector(), false, 100);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled");

        for (int i = 0; i < 101; i++) {
            client.scheduleRequest();
            requestComplete.await(10, TimeUnit.SECONDS);
            tickerIncrement.set(new Duration(1, TimeUnit.SECONDS));
        }
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 101);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 0);

        assertStatus(client, location, "queued", 0, 101, 101, 101, "not scheduled");

        tickerIncrement.set(new Duration(301, TimeUnit.SECONDS));
        client.scheduleRequest();
        requestComplete.await(10, TimeUnit.SECONDS);

        assertEquals(callback.getCompletedRequests(), 102);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportTimeoutException.class);
        assertContains(callback.getFailure().getMessage(), WORKER_NODE_ERROR + " (http://localhost:8080/0 - 102 failures,");
    }

    @Test
    public void testErrorCodes()
    {
        assertEquals(new PageTooLargeException().getErrorCode(), PAGE_TOO_LARGE.toErrorCode());
        assertEquals(new PageTransportErrorException("").getErrorCode(), PAGE_TRANSPORT_ERROR.toErrorCode());
        assertEquals(new PageTransportTimeoutException(HostAddress.fromParts("127.0.0.1", 8080), "", null).getErrorCode(), PAGE_TRANSPORT_TIMEOUT.toErrorCode());
    }

    private static void assertStatus(
            HttpPageBufferClient client,
            URI location, String status,
            int pagesReceived,
            int requestsScheduled,
            int requestsCompleted,
            int requestsFailed,
            String httpRequestState)
    {
        PageBufferClientStatus actualStatus = client.getStatus();
        assertEquals(actualStatus.getUri(), location);
        assertEquals(actualStatus.getState(), status, "status");
        assertEquals(actualStatus.getPagesReceived(), pagesReceived, "pagesReceived");
        assertEquals(actualStatus.getRequestsScheduled(), requestsScheduled, "requestsScheduled");
        assertEquals(actualStatus.getRequestsCompleted(), requestsCompleted, "requestsCompleted");
        assertEquals(actualStatus.getRequestsFailed(), requestsFailed, "requestsFailed");
        assertEquals(actualStatus.getHttpRequestState(), httpRequestState, "httpRequestState");
    }

    private static void assertPageEquals(Page expectedPage, Page actualPage)
    {
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
    }

    private static class MockNodeCrashFailureDetector
            implements FailureDetector
    {
        @Override
        public Set<ServiceDescriptor> getFailed()
        {
            return ImmutableSet.of();
        }

        @Override
        public State getState(HostAddress hostAddress)
        {
            return State.GONE;
        }
    }

    private static class MockActiveFailureDetector
            implements FailureDetector
    {
        @Override
        public Set<ServiceDescriptor> getFailed()
        {
            return ImmutableSet.of();
        }

        @Override
        public State getState(HostAddress hostAddress)
        {
            return State.ALIVE;
        }
    }

    private static class TestingClientCallback
            implements ClientCallback
    {
        private final CyclicBarrier done;
        private final List<SerializedPage> pages = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger completedRequests = new AtomicInteger();
        private final AtomicInteger finishedBuffers = new AtomicInteger();
        private final AtomicInteger failedBuffers = new AtomicInteger();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        public TestingClientCallback(CyclicBarrier done)
        {
            this.done = done;
        }

        public List<Page> getPages()
        {
            return pages.stream()
                    .map(PAGES_SERDE::deserialize)
                    .collect(Collectors.toList());
        }

        private int getCompletedRequests()
        {
            return completedRequests.get();
        }

        private int getFinishedBuffers()
        {
            return finishedBuffers.get();
        }

        public int getFailedBuffers()
        {
            return failedBuffers.get();
        }

        public Throwable getFailure()
        {
            return failure.get();
        }

        @Override
        public boolean addPages(HttpPageBufferClient client, List<SerializedPage> pages)
        {
            this.pages.addAll(pages);
            return true;
        }

        @Override
        public void requestComplete(HttpPageBufferClient client)
        {
            completedRequests.getAndIncrement();
            awaitDone();
        }

        @Override
        public void clientFinished(HttpPageBufferClient client)
        {
            finishedBuffers.getAndIncrement();
            awaitDone();
        }

        @Override
        public void clientFailed(HttpPageBufferClient client, Throwable cause)
        {
            failedBuffers.getAndIncrement();
            failure.compareAndSet(null, cause);
            // requestComplete() will be called after this
        }

        public void resetStats()
        {
            pages.clear();
            completedRequests.set(0);
            finishedBuffers.set(0);
            failedBuffers.set(0);
            failure.set(null);
        }

        private void awaitDone()
        {
            try {
                done.await(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class StaticRequestProcessor
            implements TestingHttpClient.Processor
    {
        private final AtomicReference<Response> response = new AtomicReference<>();
        private final CyclicBarrier beforeRequest;
        private final CyclicBarrier afterRequest;

        private StaticRequestProcessor(CyclicBarrier beforeRequest, CyclicBarrier afterRequest)
        {
            this.beforeRequest = beforeRequest;
            this.afterRequest = afterRequest;
        }

        private void setResponse(Response response)
        {
            this.response.set(response);
        }

        @SuppressWarnings({"ThrowFromFinallyBlock", "Finally"})
        @Override
        public Response handle(Request request)
                throws Exception
        {
            beforeRequest.await(10, TimeUnit.SECONDS);

            try {
                return response.get();
            }
            finally {
                afterRequest.await(10, TimeUnit.SECONDS);
            }
        }
    }
}
