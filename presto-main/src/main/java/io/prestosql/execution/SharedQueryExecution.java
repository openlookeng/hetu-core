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
package io.prestosql.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.memory.VersionedMemoryPoolId;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.QueryId;
import io.prestosql.sql.planner.Plan;
import io.prestosql.statestore.SharedQueryState;
import org.joda.time.DateTime;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * SharedQueryExecution is a dummy query execution that only used for OOMKiller when multiple coordinators are used
 *
 * @since 2019-11-29
 */
public class SharedQueryExecution
        implements QueryExecution
{
    private final SharedQueryState state;
    private final SessionPropertyManager sessionPropertyManager;
    private boolean toKill;

    public SharedQueryExecution(SharedQueryState state, SessionPropertyManager sessionPropertyManager)
    {
        this.state = state;
        this.sessionPropertyManager = sessionPropertyManager;
        this.toKill = false;
    }

    @Override
    public QueryState getState()
    {
        return state.getBasicQueryInfo().getState();
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Plan getQueryPlan()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return state.getBasicQueryInfo();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getSlug()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return state.getTotalCpuTime();
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return state.getUserMemoryReservation();
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return state.getTotalMemoryReservation();
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        // TODO: handle versioned memory pool
        return new VersionedMemoryPoolId(state.getBasicQueryInfo().getMemoryPool(), 0);
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        // TODO: handle versioned memory pool
    }

    @Override
    public void start()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void cancelQuery()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void recordHeartbeat()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void addFinalQueryInfoListener(StateMachine.StateChangeListener<QueryInfo> stateChangeListener)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public QueryId getQueryId()
    {
        return state.getBasicQueryInfo().getQueryId();
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public Session getSession()
    {
        return state.getSession().toSession(sessionPropertyManager);
    }

    @Override
    public DateTime getCreateTime()
    {
        return state.getBasicQueryInfo().getQueryStats().getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return Optional.of(state.getBasicQueryInfo().getQueryStats().getEndTime());
    }

    @Override
    public void fail(Throwable cause)
    {
        toKill = true;
    }

    @Override
    public void pruneInfo()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Return if the query is getting killed
     *
     * @return whether the query is to be killed
     */
    public boolean isGettingKilled()
    {
        return toKill;
    }
}
