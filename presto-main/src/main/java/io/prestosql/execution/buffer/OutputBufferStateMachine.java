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
package io.prestosql.execution.buffer;

import io.prestosql.execution.StateMachine;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.TaskId;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static io.prestosql.execution.buffer.BufferState.ABORTED;
import static io.prestosql.execution.buffer.BufferState.FAILED;
import static io.prestosql.execution.buffer.BufferState.FINISHED;
import static io.prestosql.execution.buffer.BufferState.FLUSHING;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_PAGES;
import static io.prestosql.execution.buffer.BufferState.OPEN;
import static io.prestosql.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static java.util.Objects.requireNonNull;

public class OutputBufferStateMachine
{
    private final StateMachine<BufferState> state;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    public OutputBufferStateMachine(TaskId taskId, Executor executor)
    {
        state = new StateMachine<>(taskId + "-buffer", executor, OPEN, TERMINAL_BUFFER_STATES);
    }

    public OutputBufferStateMachine(String name, Executor executor)
    {
        state = new StateMachine<>(name, executor, OPEN, TERMINAL_BUFFER_STATES);
    }

    public boolean setIf(BufferState newState, Predicate<BufferState> predicate)
    {
        return state.setIf(newState, predicate);
    }

    public boolean compareAndSet(BufferState expectedState, BufferState newState)
    {
        return state.compareAndSet(expectedState, newState);
    }

    public StateMachine<BufferState> getStateMachine()
    {
        return state;
    }

    public BufferState getState()
    {
        return state.get();
    }

    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    public boolean noMoreBuffers()
    {
        if (state.compareAndSet(OPEN, NO_MORE_BUFFERS)) {
            return true;
        }
        return state.compareAndSet(NO_MORE_PAGES, FLUSHING);
    }

    public boolean noMorePages()
    {
        if (state.compareAndSet(OPEN, NO_MORE_PAGES)) {
            return true;
        }
        return state.compareAndSet(NO_MORE_BUFFERS, FLUSHING);
    }

    public boolean finish()
    {
        return state.setIf(FINISHED, oldState -> !oldState.isTerminal());
    }

    public boolean abort()
    {
        return state.setIf(ABORTED, oldState -> !oldState.isTerminal());
    }

    public boolean fail(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");

        failure.compareAndSet(null, throwable);
        return state.setIf(FAILED, oldState -> !oldState.isTerminal());
    }

    public Optional<Throwable> getFailureCause()
    {
        return Optional.ofNullable(failure.get());
    }
}
