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
package io.prestosql.operator.exchange;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PageAssertions;
import io.prestosql.operator.PipelineContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PartitioningHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static io.prestosql.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestLocalExchangeSourceOperator
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final DataSize LOCAL_EXCHANGE_MAX_BUFFERED_BYTES = new DataSize(32, DataSize.Unit.MEGABYTE);

    private PipelineContext pipelineContext;

    @BeforeMethod
    public void setup()
    {
        pipelineContext = null;
    }

    @DataProvider
    public static Object[][] markerDistributions()
    {
        return new Object[][] {
                {FIXED_BROADCAST_DISTRIBUTION},
                {FIXED_ARBITRARY_DISTRIBUTION},
                {FIXED_PASSTHROUGH_DISTRIBUTION},
                {FIXED_HASH_DISTRIBUTION}
        };
    }

    @Test(dataProvider = "markerDistributions")
    public void testGetInputChannels(PartitioningHandle partitioningHandle)
    {
        List<Integer> partitionChannels = partitioningHandle == FIXED_HASH_DISTRIBUTION ? ImmutableList.of(0) : ImmutableList.of();
        LocalExchange.LocalExchangeFactory localExchangeFactory = new LocalExchange.LocalExchangeFactory(
                partitioningHandle,
                2,
                TYPES,
                partitionChannels,
                Optional.empty(),
                UNGROUPED_EXECUTION,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES);
        LocalExchange.LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        LocalExchangeSourceOperator operatorA = createOperator(localExchangeFactory, 2, TEST_SNAPSHOT_SESSION);
        LocalExchangeSourceOperator operatorB = createOperator(localExchangeFactory, 2, TEST_SNAPSHOT_SESSION);

        LocalExchange exchange = localExchangeFactory.getLocalExchange(Lifespan.taskWide());

        assertFalse(operatorA.getInputChannels().isPresent());
        assertFalse(operatorB.getInputChannels().isPresent());

        LocalExchange.LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
        final String sinkAId = "sinkA";
        LocalExchangeSink sinkA = sinkFactory.createSink(sinkAId);
        assertFalse(operatorA.getInputChannels().isPresent());
        assertFalse(operatorB.getInputChannels().isPresent());

        final String sinkBId = "sinkB";
        LocalExchangeSink sinkB = sinkFactory.createSink(sinkBId);
        assertTrue(operatorA.getInputChannels().isPresent());
        assertEquals(operatorA.getInputChannels().get(), Sets.newHashSet(sinkAId, sinkBId));
        assertTrue(operatorB.getInputChannels().isPresent());
        assertEquals(operatorB.getInputChannels().get(), Sets.newHashSet(sinkAId, sinkBId));

        sinkFactory.close();
        sinkFactory.noMoreSinkFactories();

        sinkA.finish();
        sinkB.finish();
    }

    @Test(dataProvider = "markerDistributions")
    public void testMarkerBroadcast(PartitioningHandle partitioningHandle)
    {
        List<Integer> partitionChannels = partitioningHandle == FIXED_HASH_DISTRIBUTION ? ImmutableList.of(0) : ImmutableList.of();
        LocalExchange.LocalExchangeFactory localExchangeFactory = new LocalExchange.LocalExchangeFactory(
                partitioningHandle,
                2,
                TYPES,
                partitionChannels,
                Optional.empty(),
                UNGROUPED_EXECUTION,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES);
        LocalExchange.LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        Operator operatorA = createOperator(localExchangeFactory, 2, TEST_SNAPSHOT_SESSION, 0);
        Operator operatorB = createOperator(localExchangeFactory, 2, TEST_SNAPSHOT_SESSION, 1);

        LocalExchange exchange = localExchangeFactory.getLocalExchange(Lifespan.taskWide());

        final String sinkAId = "sinkA";
        final String sinkBId = "sinkB";
        LocalExchange.LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
        LocalExchangeSink sinkA = sinkFactory.createSink(sinkAId);
        LocalExchangeSink sinkB = sinkFactory.createSink(sinkBId);
        sinkFactory.close();
        sinkFactory.noMoreSinkFactories();

        MarkerPage marker = MarkerPage.snapshotPage(1);
        sinkA.addPage(MarkerPage.snapshotPage(1), sinkAId);
        sinkA.finish();
        sinkB.addPage(MarkerPage.snapshotPage(1), sinkBId);
        sinkB.finish();

        Page page = operatorA.getOutput();
        PageAssertions.assertPageEquals(TYPES, page, marker);
        assertNull(operatorA.getOutput());

        page = operatorB.getOutput();
        PageAssertions.assertPageEquals(TYPES, page, marker);
        assertNull(operatorB.getOutput());
    }

    private LocalExchangeSourceOperator createOperator(LocalExchange.LocalExchangeFactory localExchangeFactory, int totalInputChannels, Session session)
    {
        return createOperator(localExchangeFactory, totalInputChannels, session, 0);
    }

    private LocalExchangeSourceOperator createOperator(LocalExchange.LocalExchangeFactory localExchangeFactory, int totalInputChannels, Session session, int driverId)
    {
        if (pipelineContext == null) {
            ScheduledExecutorService scheduler = newScheduledThreadPool(4, daemonThreadsNamed("test-%s"));
            ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
            OperatorFactory operatorFactory = new LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory(0, new PlanNodeId("test"), localExchangeFactory, totalInputChannels);

            pipelineContext = createTaskContext(scheduler, scheduledExecutor, session)
                    .addPipelineContext(0, true, true, false);
        }
        DriverContext driverContext = pipelineContext.addDriverContext(Lifespan.taskWide(), driverId);

        OperatorFactory operatorFactory = new LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory(0, new PlanNodeId("test"), localExchangeFactory, totalInputChannels);
        Operator operator = operatorFactory.createOperator(driverContext);
        assertEquals(operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes(), 0);
        return (LocalExchangeSourceOperator) operator;
    }
}
