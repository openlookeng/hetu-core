/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.tests;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.prestosql.Session;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskManager;
import io.prestosql.execution.TaskState;
import io.prestosql.metadata.NodeState;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.NodeStateChangeHandler;
import io.prestosql.server.testing.TestingPrestoServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.prestosql.execution.QueryState.FINISHED;
import static io.prestosql.memory.TestMemoryManager.createQueryRunner;
import static io.prestosql.server.NodeStateChangeHandler.isValidStateTransition;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestNodeStateChange
{
    private static final long SHUTDOWN_TIMEOUT_MILLIS1 = 20_000;
    private static final long SHUTDOWN_TIMEOUT_MILLIS2 = 240_000;

    private static final Session TINY_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();

    private static final Map<String, String> defaultProperties = ImmutableMap.<String, String>builder()
            .put("shutdown.grace-period", "5s")
            .put("node-scheduler.include-coordinator", "false")
            .build();
    private static final Map<String, String> properties = ImmutableMap.<String, String>builder()
            .put("node-scheduler.include-coordinator", "false")
            .put("shutdown.grace-period", "10s")
            .build();

    private ListeningExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = MoreExecutors.listeningDecorator(newCachedThreadPool());
    }

    @AfterClass(alwaysRun = true)
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testIsValidStateTransition()
    {
        // oldState = ACTIVE
        assertTrue(isValidStateTransition(NodeState.ACTIVE, NodeState.ACTIVE));
        assertFalse(isValidStateTransition(NodeState.ACTIVE, NodeState.INACTIVE));
        assertTrue(isValidStateTransition(NodeState.ACTIVE, NodeState.ISOLATING));
        assertTrue(isValidStateTransition(NodeState.ACTIVE, NodeState.ISOLATED));
        assertTrue(isValidStateTransition(NodeState.ACTIVE, NodeState.SHUTTING_DOWN));

        // oldState = INACTIVE
        assertFalse(isValidStateTransition(NodeState.INACTIVE, NodeState.ACTIVE));
        assertFalse(isValidStateTransition(NodeState.INACTIVE, NodeState.INACTIVE));
        assertFalse(isValidStateTransition(NodeState.INACTIVE, NodeState.ISOLATING));
        assertFalse(isValidStateTransition(NodeState.INACTIVE, NodeState.ISOLATED));
        assertTrue(isValidStateTransition(NodeState.INACTIVE, NodeState.SHUTTING_DOWN));

        // oldState = ISOLATING
        assertTrue(isValidStateTransition(NodeState.ISOLATING, NodeState.ACTIVE));
        assertFalse(isValidStateTransition(NodeState.ISOLATING, NodeState.INACTIVE));
        assertTrue(isValidStateTransition(NodeState.ISOLATING, NodeState.ISOLATING));
        assertTrue(isValidStateTransition(NodeState.ISOLATING, NodeState.ISOLATED));
        assertTrue(isValidStateTransition(NodeState.ISOLATING, NodeState.SHUTTING_DOWN));

        // oldState = ISOLATED
        assertTrue(isValidStateTransition(NodeState.ISOLATED, NodeState.ACTIVE));
        assertFalse(isValidStateTransition(NodeState.ISOLATED, NodeState.INACTIVE));
        assertTrue(isValidStateTransition(NodeState.ISOLATED, NodeState.ISOLATING));
        assertTrue(isValidStateTransition(NodeState.ISOLATED, NodeState.ISOLATED));
        assertTrue(isValidStateTransition(NodeState.ISOLATED, NodeState.SHUTTING_DOWN));

        // oldState = SHUTTING_DOWN
        assertFalse(isValidStateTransition(NodeState.SHUTTING_DOWN, NodeState.ACTIVE));
        assertFalse(isValidStateTransition(NodeState.SHUTTING_DOWN, NodeState.INACTIVE));
        assertFalse(isValidStateTransition(NodeState.SHUTTING_DOWN, NodeState.ISOLATING));
        assertFalse(isValidStateTransition(NodeState.SHUTTING_DOWN, NodeState.ISOLATED));
        assertTrue(isValidStateTransition(NodeState.SHUTTING_DOWN, NodeState.SHUTTING_DOWN));
    }

    @Test(timeOut = 30_000)
    public void testBasicDoStateTransition()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, defaultProperties)) {
            TestingPrestoServer worker = getWorker(queryRunner);

            NodeStateChangeHandler nodeStateChangeHandler = worker.getNodeStateChangeHandler();

            // current state is ACTIVE
            assertEquals(nodeStateChangeHandler.getState(), NodeState.ACTIVE);

            // graceful isolation, state will be changed to ISOLATED finally
            nodeStateChangeHandler.doStateTransition(NodeState.ISOLATING);
            while (nodeStateChangeHandler.getState().equals(NodeState.ISOLATING)) {
                Thread.sleep(1000);
            }
            assertEquals(nodeStateChangeHandler.getState(), NodeState.ISOLATED);

            // abort isolation, state change to ACTIVE
            nodeStateChangeHandler.doStateTransition(NodeState.ACTIVE);
            assertEquals(nodeStateChangeHandler.getState(), NodeState.ACTIVE);

            // graceful isolation, state will be changed to ISOLATING
            nodeStateChangeHandler.doStateTransition(NodeState.ISOLATING);
            assertEquals(nodeStateChangeHandler.getState(), NodeState.ISOLATING);
            // abort isolation, state change to ACTIVE
            nodeStateChangeHandler.doStateTransition(NodeState.ACTIVE);
            assertEquals(nodeStateChangeHandler.getState(), NodeState.ACTIVE);

            // force isolation, state change to ISOLATED immediately
            nodeStateChangeHandler.doStateTransition(NodeState.ISOLATED);
            assertEquals(nodeStateChangeHandler.getState(), NodeState.ISOLATED);

            // shutdown
            nodeStateChangeHandler.doStateTransition(NodeState.SHUTTING_DOWN);
            TestingPrestoServer.TestShutdownAction shutdownAction = (TestingPrestoServer.TestShutdownAction) worker.getShutdownAction();
            shutdownAction.waitForShutdownComplete(SHUTDOWN_TIMEOUT_MILLIS1);
            assertTrue(shutdownAction.isShutdown());
        }
    }

    @Test(timeOut = 30_000)
    public void testSuccessfulAbort()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, defaultProperties)) {
            TestingPrestoServer worker = getWorker(queryRunner);

            NodeStateChangeHandler nodeStateChangeHandler = worker.getNodeStateChangeHandler();

            // current state is ACTIVE
            assertEquals(nodeStateChangeHandler.getState(), NodeState.ACTIVE);

            nodeStateChangeHandler.doStateTransition(NodeState.ISOLATING);

            // Future task wait for 5 seconds to start
            Thread.sleep(6000);
            // abort isolation, state change to ACTIVE
            nodeStateChangeHandler.doStateTransition(NodeState.ACTIVE);
            assertEquals(nodeStateChangeHandler.getState(), NodeState.ACTIVE);

            // If future task wasn't cancelled successfully, it'll finish and may change state
            Thread.sleep(6000);
            // Verify state wasn't changed to isolated
            assertEquals(nodeStateChangeHandler.getState(), NodeState.ACTIVE);
        }
    }

    @Test(timeOut = 30_000)
    public void testWorkerIsolation()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, defaultProperties)) {
            List<ListenableFuture<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
            }

            TestingPrestoServer worker = queryRunner.getServers()
                    .stream()
                    .filter(server -> !server.isCoordinator())
                    .findFirst()
                    .get();

            assertEquals(worker.getNodeStateChangeHandler().getState(), NodeState.ACTIVE);

            TaskManager taskManager = worker.getTaskManager();

            // wait until tasks show up on the worker
            while (taskManager.getAllTaskInfo().isEmpty()) {
                MILLISECONDS.sleep(500);
            }

            worker.getNodeStateChangeHandler().doStateTransition(NodeState.ISOLATING);

            Futures.allAsList(queryFutures).get();

            List<TaskInfo> taskInfos = worker.getTaskManager().getAllTaskInfo();
            for (TaskInfo info : taskInfos) {
                assertEquals(info.getTaskStatus().getState(), TaskState.FINISHED);
            }

            while (!worker.getNodeStateChangeHandler().getState().equals(NodeState.ISOLATED)) {
                Thread.sleep(1000);
            }
        }
    }

    @Test(timeOut = 30_000)
    public void testCoordinatorIsolation()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, defaultProperties)) {
            List<ListenableFuture<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
            }

            TestingPrestoServer coordinator = queryRunner.getServers()
                    .stream()
                    .filter(TestingPrestoServer::isCoordinator)
                    .findFirst()
                    .get();

            QueryManager queryManager = coordinator.getQueryManager();

            // wait until queries show up on the coordinator
            while (queryManager.getQueries().isEmpty()) {
                MILLISECONDS.sleep(500);
            }

            assertEquals(coordinator.getNodeStateChangeHandler().getState(), NodeState.ACTIVE);

            coordinator.getNodeStateChangeHandler().doStateTransition(NodeState.ISOLATING);

            Futures.allAsList(queryFutures).get();

            List<BasicQueryInfo> queryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            for (BasicQueryInfo info : queryInfos) {
                assertEquals(info.getState(), FINISHED);
            }

            while (!coordinator.getNodeStateChangeHandler().getState().equals(NodeState.ISOLATED)) {
                Thread.sleep(1000);
            }
        }
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS2)
    public void testWorkerShutdown()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            List<ListenableFuture<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
            }

            TestingPrestoServer worker = queryRunner.getServers()
                    .stream()
                    .filter(server -> !server.isCoordinator())
                    .findFirst()
                    .get();

            TaskManager taskManager = worker.getTaskManager();

            // wait until tasks show up on the worker
            while (taskManager.getAllTaskInfo().isEmpty()) {
                MILLISECONDS.sleep(500);
            }

            worker.getNodeStateChangeHandler().doStateTransition(NodeState.SHUTTING_DOWN);

            Futures.allAsList(queryFutures).get();

            List<BasicQueryInfo> queryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            for (BasicQueryInfo info : queryInfos) {
                assertEquals(info.getState(), FINISHED);
            }

            TestingPrestoServer.TestShutdownAction shutdownAction = (TestingPrestoServer.TestShutdownAction) worker.getShutdownAction();
            shutdownAction.waitForShutdownComplete(SHUTDOWN_TIMEOUT_MILLIS2);
            assertTrue(shutdownAction.isShutdown());
        }
    }

    @Test
    public void testCoordinatorShutdown()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            List<ListenableFuture<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
            }
            TestingPrestoServer coordinator = queryRunner.getServers()
                    .stream()
                    .filter(TestingPrestoServer::isCoordinator)
                    .findFirst()
                    .get();
            QueryManager queryManager = coordinator.getQueryManager();
            // wait until queries show up on the coordinator
            while (queryManager.getQueries().isEmpty()) {
                MILLISECONDS.sleep(500);
            }

            coordinator.getNodeStateChangeHandler().doStateTransition(NodeState.SHUTTING_DOWN);

            Futures.allAsList(queryFutures).get();

            List<BasicQueryInfo> queryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            for (BasicQueryInfo info : queryInfos) {
                assertEquals(info.getState(), FINISHED);
            }

            TestingPrestoServer.TestShutdownAction shutdownAction = (TestingPrestoServer.TestShutdownAction) coordinator.getShutdownAction();
            shutdownAction.waitForShutdownComplete(SHUTDOWN_TIMEOUT_MILLIS2);
            assertTrue(shutdownAction.isShutdown());
        }
    }

    private TestingPrestoServer getWorker(DistributedQueryRunner queryRunner)
    {
        return queryRunner.getServers()
                .stream()
                .filter(server -> !server.isCoordinator())
                .findFirst()
                .get();
    }
}
