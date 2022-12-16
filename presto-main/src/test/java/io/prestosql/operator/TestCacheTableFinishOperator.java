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
import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.execution.TableExecuteContext;
import io.prestosql.execution.TableExecuteContextManager;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.statistics.ColumnStatisticMetadata;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.StatisticAggregationsDescriptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCacheTableFinishOperator
{
    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test
    public void testCacheTableFinishOperator()
    {
        TestCacheTableFinisher testCacheTableFinisher = new TestCacheTableFinisher();
        CacheTableFinishOperator operator = createCacheTableFinishOperator(testCacheTableFinisher);

        assertFalse(operator.isFinished());
        assertTrue(operator.needsInput());
        operator.addInput(rowPagesBuilder(BIGINT).row(1).build().get(0));

        assertFalse(operator.isFinished());
        assertTrue(operator.needsInput());

        // add second page
        operator.addInput(rowPagesBuilder(BIGINT).row(2).build().get(0));

        assertFalse(operator.isFinished());
        assertTrue(operator.needsInput());

        // finish operator, state hasn't changed
        operator.finish();
        assertFalse(operator.isFinished());
        assertFalse(operator.needsInput());

        // and getOutput which actually finishes the operator
        List<Type> expectedTypes = ImmutableList.of(BIGINT);
        assertPageEquals(expectedTypes,
                operator.getOutput(),
                rowPagesBuilder(expectedTypes).row(1).build().get(0));
        assertPageEquals(expectedTypes,
                operator.getOutput(),
                rowPagesBuilder(expectedTypes).row(2).build().get(0));

        assertNull(operator.getOutput()); // no more pages will be returned. state should move to FINISHED.

        assertTrue(operator.isFinished());
        assertTrue(testCacheTableFinisher.isFinished());
        assertFalse(operator.needsInput());
    }

    private CacheTableFinishOperator createCacheTableFinishOperator(TestCacheTableFinisher testCacheTableFinisher)
    {
        Session session = testSessionBuilder().build();
        ColumnStatisticMetadata statisticMetadata = new ColumnStatisticMetadata("column", MAX_VALUE);
        StatisticAggregationsDescriptor<Integer> descriptor = new StatisticAggregationsDescriptor<>(
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(statisticMetadata, 0));
        io.prestosql.execution.TableExecuteContextManager tableExecuteContextManager = new TableExecuteContextManager();
        tableExecuteContextManager.registerTableExecuteContextForQuery(QueryId.valueOf("test_query"));
        DriverContext driverContext = createTaskContext(scheduledExecutor, scheduledExecutor, session)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        CacheTableFinishOperator.CacheTableFinishOperatorFactory cacheTableFinishOperatorFactory = new CacheTableFinishOperator.CacheTableFinishOperatorFactory(0,
                new PlanNodeId("test"),
                testCacheTableFinisher,
                descriptor,
                tableExecuteContextManager,
                session,
                100000,
                (endtime, size) -> null,
                () -> null);
        return (CacheTableFinishOperator) cacheTableFinishOperatorFactory.createOperator(driverContext);
    }

    private static class TestCacheTableFinisher
            implements TableFinishOperator.TableFinisher
    {
        private boolean finished;
        private Collection<Slice> fragments;
        private Collection<ComputedStatistics> computedStatistics;

        @Override
        public Optional<ConnectorOutputMetadata> finishTable(Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics, TableExecuteContext tableExecuteContext)
        {
            checkState(!finished, "already finished");
            finished = true;
            this.fragments = fragments;
            this.computedStatistics = computedStatistics;
            return Optional.empty();
        }

        public Collection<Slice> getFragments()
        {
            return fragments;
        }

        public Collection<ComputedStatistics> getComputedStatistics()
        {
            return computedStatistics;
        }

        public boolean isFinished()
        {
            return finished;
        }
    }
}
