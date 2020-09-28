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
package io.prestosql.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskManager;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.metadata.NodeState;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.metadata.NodeState.ISOLATING;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NodeStateChangeHandler
{
    private static final Logger LOG = Logger.get(NodeStateChangeHandler.class);
    private static final Duration LIFECYCLE_STOP_TIMEOUT = new Duration(30, SECONDS);

    private final ScheduledExecutorService isolationShutdownHandler = newSingleThreadScheduledExecutor(threadsNamed("isolation-shutdown-handler-%s"));

    private final ExecutorService lifeCycleStopper = newSingleThreadExecutor(threadsNamed("lifecycle-stopper-%s"));
    private final LifeCycleManager lifeCycleManager;
    private final TaskManager sqlTaskManager;
    private final QueryManager sqlQueryManager;
    private final ShutdownAction shutdownAction;

    private final boolean isCoordinator;
    private final boolean allowTaskOnCoordinator;
    private final Duration gracePeriod;

    private ScheduledFuture future;

    @GuardedBy("this")
    private NodeState state = NodeState.ACTIVE;

    @Inject
    public NodeStateChangeHandler(
            TaskManager sqlTaskManager,
            QueryManager sqlQueryManager,
            ServerConfig serverConfig,
            NodeSchedulerConfig nodeSchedulerConfig,
            ShutdownAction shutdownAction,
            LifeCycleManager lifeCycleManager)
    {
        this.sqlTaskManager = requireNonNull(sqlTaskManager, "sqlTaskManager is null");
        this.sqlQueryManager = requireNonNull(sqlQueryManager, "sqlQueryManager is null");
        this.shutdownAction = requireNonNull(shutdownAction, "shutdownAction is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.isCoordinator = requireNonNull(serverConfig, "serverConfig is null").isCoordinator();
        this.allowTaskOnCoordinator = requireNonNull(nodeSchedulerConfig, "nodeScheulerConfig is null").isIncludeCoordinator();
        this.gracePeriod = serverConfig.getGracePeriod();
    }

    private synchronized void requestIsolation(boolean forced)
    {
        if (!forced) {
            future = isolationShutdownHandler.schedule(() -> {
                try {
                    if (isCoordinator) {
                        // wait all the queries to be finished
                        waitAllQueriesToFinish(true);
                    }
                    // worker OR coordinator with scheduling tasks
                    if (!isCoordinator || allowTaskOnCoordinator) {
                        // wait all the tasks to be finished
                        waitAllTasksToFinish(true);
                    }

                    // wait for another grace period for all workload results to be observed
                    Thread.sleep(gracePeriod.toMillis());

                    synchronized (NodeStateChangeHandler.this) {
                        if (state == NodeState.ISOLATING) {
                            LOG.info("Updating node state from ISOLATING to ISOLATED");
                            state = NodeState.ISOLATED;
                        }
                        else {
                            LOG.warn("Did not change state to ISOLATED since current state is no longer ISOLATING");
                        }
                    }
                }
                catch (InterruptedException e) {
                    currentThread().interrupt();
                }
            }, gracePeriod.toMillis(), MILLISECONDS);
        }
    }

    private synchronized void abortIsolation()
    {
        // stop isolation if it is in the progress
        if (future != null && !future.isDone()) {
            future.cancel(true);
        }
    }

    private synchronized void requestShutdown()
    {
        LOG.info("Shutdown requested");

        //wait for a grace period to start the shutdown sequence
        isolationShutdownHandler.schedule(() -> {
            // It's not expected to throw any interruptedException
            try {
                if (isCoordinator) {
                    waitAllQueriesToFinish(false);
                }

                // worker OR coordinator with scheduling tasks
                if (!isCoordinator || (isCoordinator && allowTaskOnCoordinator)) {
                    waitAllTasksToFinish(false);
                }
            }
            catch (InterruptedException e) {
                LOG.error("Unexpected interruption during shutdown preparation");
            }
            // wait for another grace period for all workload results to be observed
            sleepUninterruptibly(gracePeriod.toMillis(), MILLISECONDS);

            Future<?> shutdownFuture = lifeCycleStopper.submit(() -> {
                lifeCycleManager.stop();
                return null;
            });

            // terminate the jvm if life cycle cannot be stopped in a timely manner
            try {
                shutdownFuture.get(LIFECYCLE_STOP_TIMEOUT.toMillis(), MILLISECONDS);
            }
            catch (TimeoutException e) {
                LOG.warn(e, "Timed out waiting for the life cycle to stop");
            }
            catch (InterruptedException e) {
                LOG.warn(e, "Interrupted while waiting for the life cycle to stop");
                currentThread().interrupt();
            }
            catch (ExecutionException e) {
                LOG.warn(e, "Problem stopping the life cycle");
            }
            shutdownAction.onShutdown();
        }, gracePeriod.toMillis(), MILLISECONDS);
    }

    private List<TaskInfo> getActiveTasks()
    {
        return sqlTaskManager.getAllTaskInfo()
                .stream()
                .filter(taskInfo -> !taskInfo.getTaskStatus().getState().isDone())
                .collect(toImmutableList());
    }

    private List<BasicQueryInfo> getActiveQueries()
    {
        return sqlQueryManager.getQueries()
                .stream()
                .filter(queryinfo -> !queryinfo.getState().isDone())
                .collect(toImmutableList());
    }

    private void waitAllTasksToFinish(boolean allowInterrupt)
            throws InterruptedException
    {
        List<TaskInfo> activeTasks = getActiveTasks();
        while (activeTasks.size() > 0) {
            CountDownLatch countDownLatch = new CountDownLatch(activeTasks.size());

            for (TaskInfo taskInfo : activeTasks) {
                sqlTaskManager.addStateChangeListener(taskInfo.getTaskStatus().getTaskId(), newState -> {
                    if (newState.isDone()) {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            }
            catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for all tasks to finish");
                if (allowInterrupt) {
                    throw e;
                }
            }
            activeTasks = getActiveTasks();
        }
    }

    private void waitAllQueriesToFinish(boolean allowInterrupt)
            throws InterruptedException
    {
        List<BasicQueryInfo> activeQueries = getActiveQueries();
        while (activeQueries.size() > 0) {
            CountDownLatch countDownLatch = new CountDownLatch(activeQueries.size());

            for (BasicQueryInfo queryInfo : activeQueries) {
                sqlQueryManager.addStateChangeListener(queryInfo.getQueryId(), newState -> {
                    if (newState.isDone()) {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            }
            catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for all queries to finish");
                if (allowInterrupt) {
                    throw e;
                }
            }
            activeQueries = getActiveQueries();
        }
    }

    @VisibleForTesting
    public static boolean isValidStateTransition(NodeState oldState, NodeState newState)
    {
        switch (oldState) {
            case ACTIVE:
            case ISOLATING:
            case ISOLATED:
                switch (newState) {
                    case ACTIVE:
                    case ISOLATING:
                    case ISOLATED:
                    case SHUTTING_DOWN:
                        return true;
                    default:
                }
                break;
            case INACTIVE:
            case SHUTTING_DOWN:
                switch (newState) {
                    case SHUTTING_DOWN:
                        return true;
                    default:
                }
                break;
            default:
                break;
        }
        return false;
    }

    public synchronized boolean doStateTransition(NodeState newState)
            throws IllegalStateException
    {
        NodeState oldState = getState();

        if (!isValidStateTransition(oldState, newState)) {
            throw new IllegalStateException(format("Invalid state transition from  %s to %s", oldState, newState));
        }

        if (oldState == newState) {
            return false;
        }

        if (oldState == ISOLATING) {
            abortIsolation();
        }

        switch (newState) {
            case SHUTTING_DOWN:
                requestShutdown();
                break;
            case ISOLATING:
                requestIsolation(false);
                break;
            case ISOLATED:
                requestIsolation(true);
                break;
            default:
                break;
        }
        LOG.info(String.format("Updating node state from %s to %s", state, newState));
        state = newState;
        return true;
    }

    public synchronized NodeState getState()
    {
        return state;
    }
}
