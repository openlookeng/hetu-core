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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.memory.VersionedMemoryPoolId;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.QueryId;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Statement;
import io.prestosql.transaction.TransactionManager;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class DataDefinitionExecution<T extends Statement>
        implements QueryExecution
{
    private final DataDefinitionTask<T> task;
    private final T statement;
    private final String slug;
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final QueryStateMachine stateMachine;
    private final List<Expression> parameters;
    private final HeuristicIndexerManager heuristicIndexerManager;

    private DataDefinitionExecution(
            DataDefinitionTask<T> task,
            T statement,
            String slug,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            HeuristicIndexerManager heuristicIndexerManager)
    {
        this.task = requireNonNull(task, "task is null");
        this.statement = requireNonNull(statement, "statement is null");
        this.slug = requireNonNull(slug, "slug is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.parameters = parameters;
        this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
    }

    @Override
    public String getSlug()
    {
        return slug;
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return stateMachine.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        stateMachine.setMemoryPool(poolId);
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }

    @Override
    public DateTime getCreateTime()
    {
        return stateMachine.getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return stateMachine.getExecutionStartTime();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, NANOSECONDS);
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return stateMachine.getFinalQueryInfo()
                .map(BasicQueryInfo::new)
                .orElseGet(() -> stateMachine.getBasicQueryInfo(Optional.empty()));
    }

    @Override
    public void start()
    {
        try {
            // transition to running
            if (!stateMachine.transitionToRunning()) {
                // query already running or finished
                return;
            }

            ListenableFuture<?> future = task.execute(statement, transactionManager, metadata, accessControl, stateMachine, parameters, heuristicIndexerManager);
            Futures.addCallback(future, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(@Nullable Object result)
                {
                    stateMachine.transitionToFinishing();
                }

                @Override
                public void onFailure(Throwable throwable)
                {
                    fail(throwable);
                }
            }, directExecutor());
        }
        catch (Throwable e) {
            fail(e);
            throwIfInstanceOf(e, Error.class);
        }
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        // DDL does not have an output
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return stateMachine.getStateChange(currentState);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        stateMachine.addQueryInfoStateChangeListener(stateChangeListener);
    }

    @Override
    public void fail(Throwable cause)
    {
        stateMachine.transitionToFailed(cause);
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void cancelQuery()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        // no-op
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        // no-op
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return stateMachine.getFinalQueryInfo().orElseGet(() -> stateMachine.updateQueryInfo(Optional.empty(), null));
    }

    @Override
    public Plan getQueryPlan()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    public List<Expression> getParameters()
    {
        return parameters;
    }

    public static class DataDefinitionExecutionFactory
            implements QueryExecutionFactory<DataDefinitionExecution<?>>
    {
        private final TransactionManager transactionManager;
        private final Metadata metadata;
        private final AccessControl accessControl;
        private final Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks;
        private final HeuristicIndexerManager heuristicIndexerManager;

        @Inject
        public DataDefinitionExecutionFactory(
                TransactionManager transactionManager,
                Metadata metadata,
                AccessControl accessControl,
                Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks,
                HeuristicIndexerManager heuristicIndexerManager)
        {
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.tasks = requireNonNull(tasks, "tasks is null");
            this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
        }

        @Override
        public DataDefinitionExecution<?> createQueryExecution(
                PreparedQuery preparedQuery,
                List<PreparedQuery> preparedQueryList,
                QueryStateMachine stateMachine,
                String slug,
                WarningCollector warningCollector)
        {
            return createDataDefinitionExecution(preparedQuery.getStatement(), preparedQuery.getParameters(), stateMachine, slug, heuristicIndexerManager);
        }

        private <T extends Statement> DataDefinitionExecution<T> createDataDefinitionExecution(
                T statement,
                List<Expression> parameters,
                QueryStateMachine stateMachine,
                String slug,
                HeuristicIndexerManager heuristicIndexerManager)
        {
            @SuppressWarnings("unchecked")
            DataDefinitionTask<T> localTask = (DataDefinitionTask<T>) tasks.get(statement.getClass());
            checkArgument(localTask != null, "no task for statement: %s", statement.getClass().getSimpleName());

            stateMachine.setUpdateType(localTask.getName());
            return new DataDefinitionExecution<>(localTask, statement, slug, transactionManager, metadata, accessControl, stateMachine, parameters, heuristicIndexerManager);
        }
    }
}
