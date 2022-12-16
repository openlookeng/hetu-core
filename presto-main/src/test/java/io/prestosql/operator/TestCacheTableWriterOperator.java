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
import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.metadata.OutputTableHandle;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.split.PageSinkManager;
import io.prestosql.sql.planner.plan.TableWriterNode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestCacheTableWriterOperator
{
    private static final CatalogName CONNECTOR_ID = new CatalogName("testConnectorId");
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testBlockedPageSink()
    {
        CacheWriterBlockingPageSink blockingPageSink = new CacheWriterBlockingPageSink();
        Operator operator = createCacheTableWriterOperator(blockingPageSink);

        // initial state validation
        assertTrue(operator.isBlocked().isDone());
        assertFalse(operator.isFinished());
        assertTrue(operator.needsInput());

        // blockingPageSink that will return blocked future
        operator.addInput(rowPagesBuilder(BIGINT).row(42).build().get(0));

        assertFalse(operator.isBlocked().isDone());
        assertFalse(operator.isFinished());
        assertFalse(operator.needsInput());
        assertNull(operator.getOutput());

        // complete previously blocked future
        blockingPageSink.complete();

        assertTrue(operator.isBlocked().isDone());
        assertFalse(operator.isFinished());
        assertTrue(operator.needsInput());

        // add second page
        operator.addInput(rowPagesBuilder(BIGINT).row(44).build().get(0));

        assertFalse(operator.isBlocked().isDone());
        assertFalse(operator.isFinished());
        assertFalse(operator.needsInput());

        // finish operator, state hasn't changed
        operator.finish();

        assertFalse(operator.isBlocked().isDone());
        assertFalse(operator.isFinished());
        assertFalse(operator.needsInput());

        // complete previously blocked future
        blockingPageSink.complete();
        // and getOutput which actually finishes the operator
        List<Type> expectedTypes = ImmutableList.of(BIGINT);
        assertPageEquals(expectedTypes,
                operator.getOutput(),
                rowPagesBuilder(expectedTypes).row(42).build().get(0));

        assertTrue(operator.isBlocked().isDone());

        assertPageEquals(expectedTypes,
                operator.getOutput(),
                rowPagesBuilder(expectedTypes).row(44).build().get(0));

        assertNull(operator.getOutput());

        assertTrue(operator.isBlocked().isDone());
        assertTrue(operator.isFinished());
        assertFalse(operator.needsInput());
    }

    private Operator createCacheTableWriterOperator(CacheWriterBlockingPageSink blockingPageSink)
    {
        PageSinkManager pageSinkManager = new PageSinkManager();
        pageSinkManager.addConnectorPageSinkProvider(CONNECTOR_ID, new CacheWriterConstantPageSinkProvider(blockingPageSink));
        return createCacheTableWriterOperator(pageSinkManager);
    }

    private Operator createCacheTableWriterOperator(PageSinkManager pageSinkManager)
    {
        return createCacheTableWriterOperator(pageSinkManager, TEST_SESSION);
    }

    private Operator createCacheTableWriterOperator(PageSinkManager pageSinkManager, Session session)
    {
        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, session)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        return createCacheTableWriterOperator(pageSinkManager, session, driverContext);
    }

    private Operator createCacheTableWriterOperator(
            PageSinkManager pageSinkManager,
            Session session,
            DriverContext driverContext)
    {
        CacheTableWriterOperator.CacheTableWriterOperatorFactory factory = new CacheTableWriterOperator.CacheTableWriterOperatorFactory(0,
                new PlanNodeId("test"),
                pageSinkManager,
                new TableWriterNode.CreateTarget(new OutputTableHandle(
                        CONNECTOR_ID,
                        new ConnectorTransactionHandle() {},
                        new ConnectorOutputTableHandle() {}),
                        new SchemaTableName("testSchema", "testTable")),
                ImmutableList.of(0),
                session,
                Optional.empty(),
                1000000);
        return factory.createOperator(driverContext);
    }

    private static class CacheWriterConstantPageSinkProvider
            implements ConnectorPageSinkProvider
    {
        private final ConnectorPageSink pageSink;

        private CacheWriterConstantPageSinkProvider(ConnectorPageSink pageSink)
        {
            this.pageSink = pageSink;
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
        {
            return pageSink;
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
        {
            return pageSink;
        }
    }

    private static class CacheWriterBlockingPageSink
            implements ConnectorPageSink
    {
        private CompletableFuture<?> future = new CompletableFuture<>();
        private CompletableFuture<Collection<Slice>> finishFuture = new CompletableFuture<>();

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            future = new CompletableFuture<>();
            return future;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            finishFuture = new CompletableFuture<>();
            return finishFuture;
        }

        @Override
        public void abort()
        {
        }

        void complete()
        {
            future.complete(null);
            finishFuture.complete(ImmutableList.of());
        }
    }
}
