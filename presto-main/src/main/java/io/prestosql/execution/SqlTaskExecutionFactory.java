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
package io.prestosql.execution;

import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.prestosql.Session;
import io.prestosql.event.SplitMonitor;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.execution.executor.TaskExecutor;
import io.prestosql.memory.QueryContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.CommonTableExecutionContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.sql.planner.LocalExecutionPlanner;
import io.prestosql.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.TypeProvider;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Executor;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.prestosql.SystemSessionProperties.getExtensionExecutionPlannerClassPath;
import static io.prestosql.SystemSessionProperties.getExtensionExecutionPlannerJarPath;
import static io.prestosql.SystemSessionProperties.isExchangeCompressionEnabled;
import static io.prestosql.SystemSessionProperties.isExtensionExecutionPlannerEnabled;
import static io.prestosql.execution.SqlTaskExecution.createSqlTaskExecution;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class SqlTaskExecutionFactory
{
    private static final Logger log = Logger.get(SqlTaskExecutionFactory.class);
    private static LocalExecutionPlanner extensionPlanner;
    private static boolean extensionPlannerInitialized;
    private final Executor taskNotificationExecutor;

    private final TaskExecutor taskExecutor;

    private final LocalExecutionPlanner planner;
    private final SplitMonitor splitMonitor;
    private final boolean perOperatorCpuTimerEnabled;
    private final boolean cpuTimerEnabled;
    private final Metadata metadata;

    public SqlTaskExecutionFactory(
            Executor taskNotificationExecutor,
            TaskExecutor taskExecutor,
            LocalExecutionPlanner planner,
            SplitMonitor splitMonitor,
            TaskManagerConfig config,
            Metadata metadata)
    {
        this.taskNotificationExecutor = requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
        this.planner = requireNonNull(planner, "planner is null");
        this.splitMonitor = requireNonNull(splitMonitor, "splitMonitor is null");
        requireNonNull(config, "config is null");
        this.perOperatorCpuTimerEnabled = config.isPerOperatorCpuTimerEnabled();
        this.cpuTimerEnabled = config.isTaskCpuTimerEnabled();
        this.metadata = metadata;
    }

    public SqlTaskExecution create(String taskInstanceId, Session session, QueryContext queryContext, TaskStateMachine taskStateMachine, OutputBuffer outputBuffer, PlanFragment fragment, List<TaskSource> sources, OptionalInt totalPartitions, Optional<PlanNodeId> consumer,
            Map<String, CommonTableExecutionContext> cteCtx)
    {
        TaskContext taskContext = queryContext.addTaskContext(
                taskInstanceId,
                taskStateMachine,
                session,
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled,
                totalPartitions,
                consumer,
                new PagesSerdeFactory(metadata.getFunctionAndTypeManager().getBlockEncodingSerde(), isExchangeCompressionEnabled(session)));

        LocalExecutionPlan localExecutionPlan = null;
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskStateMachine.getTaskId())) {
            try {
                if (isExtensionExecutionPlannerEnabled(session)) {
                    String jarPath = getExtensionExecutionPlannerJarPath(session);
                    String classPath = getExtensionExecutionPlannerClassPath(session);
                    if (jarPath != null && !jarPath.equals("") && classPath != null && !classPath.equals("")) {
                        localExecutionPlan = loadExtensionLocalExecutionPlan(outputBuffer, fragment, taskContext, cteCtx, jarPath, classPath);
                    }
                    else {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Extension execution planner jar path or class path isn't configured correctly");
                    }
                }
                if (localExecutionPlan == null) {
                    localExecutionPlan = planner.plan(
                            taskContext,
                            fragment.getRoot(),
                            TypeProvider.copyOf(fragment.getSymbols()),
                            fragment.getPartitioningScheme(),
                            fragment.getStageExecutionDescriptor(),
                            fragment.getPartitionedSources(),
                            outputBuffer,
                            fragment.getFeederCTEId(),
                            fragment.getFeederCTEParentId(),
                            cteCtx);
                }
            }
            catch (Throwable e) {
                // planning failed
                taskStateMachine.failed(e);
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
        return createSqlTaskExecution(
                taskStateMachine,
                taskContext,
                outputBuffer,
                sources,
                localExecutionPlan,
                taskExecutor,
                taskNotificationExecutor,
                splitMonitor);
    }

    @Nullable
    private LocalExecutionPlan loadExtensionLocalExecutionPlan(OutputBuffer outputBuffer, PlanFragment fragment, TaskContext taskContext, Map<String, CommonTableExecutionContext> cteCtx, String jarPath, String classPath)
    {
        if (!extensionPlannerInitialized) {
            try {
                ExtensionClassLoader extensionClassLoader = new ExtensionClassLoader(jarPath, Thread.currentThread().getContextClassLoader());
                Thread.currentThread().setContextClassLoader(extensionClassLoader);
                Class<?> aClass = extensionClassLoader.loadClass(classPath);
                Constructor<?> constructor = aClass.getConstructor(LocalExecutionPlanner.class);
                extensionPlanner = (LocalExecutionPlanner) constructor.newInstance(planner);
                extensionPlannerInitialized = true;
            }
            catch (Throwable e) {
                log.warn("get extension LocalExecutionPlanner failed: %s", e.toString());
                throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
            }
        }
        if (extensionPlanner != null) {
            return extensionPlanner.plan(
                    taskContext,
                    fragment.getRoot(),
                    TypeProvider.copyOf(fragment.getSymbols()),
                    fragment.getPartitioningScheme(),
                    fragment.getStageExecutionDescriptor(),
                    fragment.getPartitionedSources(),
                    outputBuffer,
                    fragment.getFeederCTEId(),
                    fragment.getFeederCTEParentId(),
                    cteCtx);
        }
        return null;
    }

    public static class ExtensionClassLoader
            extends URLClassLoader
    {
        public ExtensionClassLoader(final String path, ClassLoader parent) throws MalformedURLException
        {
            super(new URL[] {new URL(path)}, parent);
        }
    }
}
