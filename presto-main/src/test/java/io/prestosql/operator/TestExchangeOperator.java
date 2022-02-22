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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.Lifespan;
import io.prestosql.failuredetector.NoOpFailureDetector;
import io.prestosql.metadata.Split;
import io.prestosql.operator.ExchangeOperator.ExchangeOperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.type.Type;
import io.prestosql.split.RemoteSplit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static io.prestosql.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.operator.TestingTaskBuffer.PAGE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestExchangeOperator
{
    private static final List<Type> TYPES = ImmutableList.of(VARCHAR);

    private static final String TASK_1_ID = "task1";
    private static final String TASK_2_ID = "task2";
    private static final String TASK_3_ID = "task3";

    private final LoadingCache<String, TestingTaskBuffer> taskBuffers = CacheBuilder.newBuilder().build(CacheLoader.from(TestingTaskBuffer::new));

    private ScheduledExecutorService scheduler;
    private ScheduledExecutorService scheduledExecutor;
    private HttpClient httpClient;
    private ExchangeClientSupplier exchangeClientSupplier;
    private ExecutorService pageBufferClientCallbackExecutor;

    @SuppressWarnings("resource")
    @BeforeClass
    public void setUp()
    {
        scheduler = newScheduledThreadPool(4, daemonThreadsNamed("test-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        pageBufferClientCallbackExecutor = Executors.newSingleThreadExecutor();
        httpClient = new TestingHttpClient(new TestingExchangeHttpClientHandler(taskBuffers), scheduler);

        exchangeClientSupplier = (systemMemoryUsageListener) -> new ExchangeClient(
                new DataSize(32, MEGABYTE),
                new DataSize(10, MEGABYTE),
                3,
                new Duration(1, TimeUnit.MINUTES),
                true,
                httpClient,
                scheduler,
                systemMemoryUsageListener,
                pageBufferClientCallbackExecutor, new NoOpFailureDetector(), true, 3);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        httpClient.close();
        httpClient = null;

        scheduler.shutdownNow();
        scheduler = null;

        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;

        pageBufferClientCallbackExecutor.shutdownNow();
        pageBufferClientCallbackExecutor = null;
    }

    @BeforeMethod
    public void setUpMethod()
    {
        taskBuffers.invalidateAll();
    }

    @Test
    public void testSimple()
            throws Exception
    {
        SourceOperator operator = createExchangeOperator();

        operator.addSplit(newRemoteSplit(TASK_1_ID));
        operator.addSplit(newRemoteSplit(TASK_2_ID));
        operator.addSplit(newRemoteSplit(TASK_3_ID));
        operator.noMoreSplits();

        // add pages and close the buffers
        taskBuffers.getUnchecked(TASK_1_ID).addPages(10, true);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(10, true);
        taskBuffers.getUnchecked(TASK_3_ID).addPages(10, true);

        // read the pages
        waitForPages(operator, 30);

        // wait for finished
        waitForFinished(operator);
    }

    //TODO-cp-I2DSGR: currently only has operator context so it's not tested.

    @Test
    public void testGetInputChannels()
            throws ExecutionException, InterruptedException
    {
        ExchangeOperator operator = (ExchangeOperator) createExchangeOperator(TEST_SNAPSHOT_SESSION);

        // Not enough channels are known
        assertFalse(operator.isBlocked().isDone());

        operator.addSplit(newRemoteSplit(TASK_1_ID));
        // Not all channels are known
        assertFalse(operator.isBlocked().isDone());
        // Not enough channels are known
        assertFalse(operator.isBlocked().isDone());
        taskBuffers.getUnchecked(TASK_1_ID).addPages(1, true);

        operator.addSplit(newRemoteSplit(TASK_2_ID));
        // Not all channels are known
        assertFalse(operator.isBlocked().isDone());
        // At least expected channels are known
        assertFalse(operator.isBlocked().isDone());
        taskBuffers.getUnchecked(TASK_2_ID).addPages(1, true);

        operator.noMoreSplits();
        operator.isBlocked().get();
        Optional<Set<String>> channels = operator.getInputChannels();
        assertTrue(channels.isPresent());
        assertEquals(channels.get().size(), 2);

        Optional<Set<String>> channels1 = operator.getInputChannels();
        assertTrue(channels == channels1);

        // read the pages
        waitForPages(operator, 2);
        // wait for finished
        waitForFinished(operator);
    }

    @Test
    public void testMarkers()
            throws Exception
    {
        SourceOperator operator = createExchangeOperator(TEST_SNAPSHOT_SESSION);

        operator.addSplit(newRemoteSplit(TASK_1_ID));
        operator.addSplit(newRemoteSplit(TASK_2_ID));
        operator.noMoreSplits();

        MarkerPage marker = MarkerPage.snapshotPage(1);
        taskBuffers.getUnchecked(TASK_1_ID).addPages(3, false);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(3, false);
        taskBuffers.getUnchecked(TASK_1_ID).addPage(marker, false);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(3, false);
        taskBuffers.getUnchecked(TASK_2_ID).addPage(marker, false);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(3, true);
        taskBuffers.getUnchecked(TASK_1_ID).addPages(3, true);

        // read the pages
        waitForPages(operator, 16);

        // wait for finished
        waitForFinished(operator);
    }

    private static Split newRemoteSplit(String taskId)
    {
        return new Split(REMOTE_CONNECTOR_ID, new RemoteSplit(URI.create("http://localhost/" + taskId), "new split test instance id"), Lifespan.taskWide());
    }

    @Test
    public void testWaitForClose()
            throws Exception
    {
        SourceOperator operator = createExchangeOperator();

        operator.addSplit(newRemoteSplit(TASK_1_ID));
        operator.addSplit(newRemoteSplit(TASK_2_ID));
        operator.addSplit(newRemoteSplit(TASK_3_ID));
        operator.noMoreSplits();

        // add pages and leave buffers open
        taskBuffers.getUnchecked(TASK_1_ID).addPages(1, false);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(1, false);
        taskBuffers.getUnchecked(TASK_3_ID).addPages(1, false);

        // read 3 pages
        waitForPages(operator, 3);

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);

        // add more pages and close the buffers
        taskBuffers.getUnchecked(TASK_1_ID).addPages(2, true);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(2, true);
        taskBuffers.getUnchecked(TASK_3_ID).addPages(2, true);

        // read all pages
        waitForPages(operator, 6);

        // wait for finished
        waitForFinished(operator);
    }

    @Test
    public void testWaitForNoMoreSplits()
            throws Exception
    {
        SourceOperator operator = createExchangeOperator();

        // add a buffer location containing one page and close the buffer
        operator.addSplit(newRemoteSplit(TASK_1_ID));
        // add pages and leave buffers open
        taskBuffers.getUnchecked(TASK_1_ID).addPages(1, true);

        // read page
        waitForPages(operator, 1);

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);

        // add a buffer location
        operator.addSplit(newRemoteSplit(TASK_2_ID));
        // set no more splits (buffer locations)
        operator.noMoreSplits();
        // add two pages and close the last buffer
        taskBuffers.getUnchecked(TASK_2_ID).addPages(2, true);

        // read all pages
        waitForPages(operator, 2);

        // wait for finished
        waitForFinished(operator);
    }

    @Test
    public void testFinish()
            throws Exception
    {
        SourceOperator operator = createExchangeOperator();

        operator.addSplit(newRemoteSplit(TASK_1_ID));
        operator.addSplit(newRemoteSplit(TASK_2_ID));
        operator.addSplit(newRemoteSplit(TASK_3_ID));
        operator.noMoreSplits();

        // add pages and leave buffers open
        taskBuffers.getUnchecked(TASK_1_ID).addPages(1, false);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(1, false);
        taskBuffers.getUnchecked(TASK_3_ID).addPages(1, false);

        // read 3 pages
        waitForPages(operator, 3);

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);

        // finish without closing buffers
        operator.finish();

        // wait for finished
        waitForFinished(operator);
    }

    private SourceOperator createExchangeOperator()
    {
        return createExchangeOperator(TEST_SESSION);
    }

    private SourceOperator createExchangeOperator(Session session)
    {
        ExchangeOperatorFactory operatorFactory = new ExchangeOperatorFactory(0, new PlanNodeId("test"), exchangeClientSupplier);

        DriverContext driverContext = createTaskContext(scheduler, scheduledExecutor, session)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        SourceOperator operator = operatorFactory.createOperator(driverContext);
        operatorFactory.noMoreOperators();
        assertEquals(operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes(), 0);
        return operator;
    }

    private static List<Page> waitForPages(Operator operator, int expectedPageCount)
            throws InterruptedException
    {
        // read expected pages or until 10 seconds has passed
        long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        List<Page> outputPages = new ArrayList<>();

        boolean greaterThanZero = false;
        while (System.nanoTime() - endTime < 0) {
            if (operator.isFinished()) {
                break;
            }

            if (operator.getOperatorContext().getDriverContext().getPipelineContext().getPipelineStats().getSystemMemoryReservation().toBytes() > 0) {
                greaterThanZero = true;
                break;
            }
            else {
                Thread.sleep(10);
            }
        }
        assertTrue(greaterThanZero);

        while (outputPages.size() < expectedPageCount && System.nanoTime() < endTime) {
            assertEquals(operator.needsInput(), false);
            if (operator.isFinished()) {
                break;
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
            else {
                Thread.sleep(10);
            }
        }

        // sleep for a bit to make sure that there aren't extra pages on the way
        Thread.sleep(10);

        // verify state
        assertEquals(operator.needsInput(), false);
        assertNull(operator.getOutput());

        // verify pages
        assertEquals(outputPages.size(), expectedPageCount);
        for (Page page : outputPages) {
            if (!(page instanceof MarkerPage)) {
                assertPageEquals(TYPES, page, PAGE);
            }
        }

        assertEquals(operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes(), 0);

        return outputPages;
    }

    private static void waitForFinished(Operator operator)
            throws InterruptedException
    {
        // wait for finished or until 10 seconds has passed
        long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() - endTime < 0) {
            assertEquals(operator.needsInput(), false);
            assertNull(operator.getOutput());
            if (operator.isFinished()) {
                break;
            }
            Thread.sleep(10);
        }

        // verify final state
        assertEquals(operator.isFinished(), true);
        assertEquals(operator.needsInput(), false);
        assertNull(operator.getOutput());
        assertEquals(operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes(), 0);
    }
}
