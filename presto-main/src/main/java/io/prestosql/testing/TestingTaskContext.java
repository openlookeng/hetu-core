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
package io.prestosql.testing;

import io.airlift.stats.GcMonitor;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskStateMachine;
import io.prestosql.memory.MemoryPool;
import io.prestosql.memory.QueryContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.snapshot.SnapshotUtils;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spiller.SpillSpaceTracker;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.testing.TestingPagesSerdeFactory.TESTING_SERDE_FACTORY;
import static io.prestosql.testing.TestingSnapshotUtils.NOOP_SNAPSHOT_UTILS;

public final class TestingTaskContext
{
    // Don't start this monitor
    private static final GcMonitor GC_MONITOR = new TestingGcMonitor();

    private TestingTaskContext() {}

    public static TaskContext createTaskContext(Executor notificationExecutor, ScheduledExecutorService yieldExecutor, Session session)
    {
        return builder(notificationExecutor, yieldExecutor, session).build();
    }

    public static TaskContext createTaskContext(Executor notificationExecutor, ScheduledExecutorService yieldExecutor, Session session, SnapshotUtils snapshotUtils)
    {
        return builder(notificationExecutor, yieldExecutor, session, snapshotUtils).build();
    }

    public static TaskContext createTaskContext(Executor notificationExecutor, ScheduledExecutorService yieldExecutor, Session session, DataSize maxMemory)
    {
        return builder(notificationExecutor, yieldExecutor, session)
                .setQueryMaxMemory(maxMemory)
                .build();
    }

    public static TaskContext createTaskContext(Executor notificationExecutor, ScheduledExecutorService yieldExecutor, Session session, TaskStateMachine taskStateMachine)
    {
        return builder(notificationExecutor, yieldExecutor, session)
                .setTaskStateMachine(taskStateMachine)
                .build();
    }

    public static TaskContext createTaskContext(QueryContext queryContext, Executor executor, Session session)
    {
        return createTaskContext(queryContext, session, new TaskStateMachine(new TaskId("query", 0, 0), executor));
    }

    public static TaskContext createTaskContext(QueryContext queryContext, Executor executor, Session session, TaskId taskId)
    {
        return createTaskContext(queryContext, session, new TaskStateMachine(taskId, executor));
    }

    private static TaskContext createTaskContext(QueryContext queryContext, Session session, TaskStateMachine taskStateMachine)
    {
        TaskContext taskContext = queryContext.addTaskContext(
                taskStateMachine,
                session,
                true,
                true,
                OptionalInt.empty(),
                Optional.empty(),
                TESTING_SERDE_FACTORY);
        taskContext.getSnapshotManager().setTotalComponents(100);
        return taskContext;
    }

    public static Builder builder(Executor notificationExecutor, ScheduledExecutorService yieldExecutor, Session session)
    {
        return new Builder(notificationExecutor, yieldExecutor, session, NOOP_SNAPSHOT_UTILS);
    }

    public static Builder builder(Executor notificationExecutor, ScheduledExecutorService yieldExecutor, Session session, SnapshotUtils snapshotUtils)
    {
        return new Builder(notificationExecutor, yieldExecutor, session, snapshotUtils);
    }

    public static class Builder
    {
        private final Executor notificationExecutor;
        private final ScheduledExecutorService yieldExecutor;
        private final Session session;
        private QueryId queryId = new QueryId("test_query");
        private TaskStateMachine taskStateMachine;
        private DataSize queryMaxMemory = new DataSize(256, MEGABYTE);
        private final DataSize queryMaxTotalMemory = new DataSize(512, MEGABYTE);
        private DataSize memoryPoolSize = new DataSize(1, GIGABYTE);
        private DataSize maxSpillSize = new DataSize(1, GIGABYTE);
        private DataSize queryMaxSpillSize = new DataSize(1, GIGABYTE);
        private SnapshotUtils snapshotUtils;

        private Builder(Executor notificationExecutor, ScheduledExecutorService yieldExecutor, Session session, SnapshotUtils snapshotUtils)
        {
            this.notificationExecutor = notificationExecutor;
            this.yieldExecutor = yieldExecutor;
            this.session = session;
            this.taskStateMachine = new TaskStateMachine(new TaskId("query", 0, 0), notificationExecutor);
            this.snapshotUtils = snapshotUtils;
        }

        public Builder setTaskStateMachine(TaskStateMachine taskStateMachine)
        {
            this.taskStateMachine = taskStateMachine;
            return this;
        }

        public Builder setQueryMaxMemory(DataSize queryMaxMemory)
        {
            this.queryMaxMemory = queryMaxMemory;
            return this;
        }

        public Builder setMemoryPoolSize(DataSize memoryPoolSize)
        {
            this.memoryPoolSize = memoryPoolSize;
            return this;
        }

        public Builder setMaxSpillSize(DataSize maxSpillSize)
        {
            this.maxSpillSize = maxSpillSize;
            return this;
        }

        public Builder setQueryMaxSpillSize(DataSize queryMaxSpillSize)
        {
            this.queryMaxSpillSize = queryMaxSpillSize;
            return this;
        }

        public Builder setQueryId(QueryId queryId)
        {
            this.queryId = queryId;
            return this;
        }

        public Builder setSnapshotUtils(SnapshotUtils snapshotUtils)
        {
            this.snapshotUtils = snapshotUtils;
            return this;
        }

        public TaskContext build()
        {
            MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), memoryPoolSize);
            SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(maxSpillSize);
            QueryContext queryContext = new QueryContext(
                    queryId,
                    queryMaxMemory,
                    queryMaxTotalMemory,
                    memoryPool,
                    GC_MONITOR,
                    notificationExecutor,
                    yieldExecutor,
                    queryMaxSpillSize,
                    spillSpaceTracker,
                    snapshotUtils);

            return createTaskContext(queryContext, session, taskStateMachine);
        }
    }
}
