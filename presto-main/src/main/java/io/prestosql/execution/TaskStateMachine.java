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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.prestosql.execution.StateMachine.StateChangeListener;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.execution.TaskState.FLUSHING;
import static io.prestosql.execution.TaskState.RUNNING;
import static io.prestosql.execution.TaskState.SUSPENDED;
import static io.prestosql.execution.TaskState.TERMINAL_TASK_STATES;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TaskStateMachine
{
    private static final Logger log = Logger.get(TaskStateMachine.class);

    private final DateTime createdTime = DateTime.now();

    private final TaskId taskId;
    private final StateMachine<TaskState> taskState;
    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();
    private final Executor executor;

    @GuardedBy("this")
    private final Map<TaskId, Throwable> sourceTaskFailures = new HashMap<>();
    @GuardedBy("this")
    private final List<TaskFailureListener> sourceTaskFailureListeners = new ArrayList<>();

    public TaskStateMachine(TaskId taskId, Executor executor)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        taskState = new StateMachine<>("task " + taskId, executor, TaskState.RUNNING, TERMINAL_TASK_STATES);
        this.executor = executor;
        taskState.addStateChangeListener(new StateChangeListener<TaskState>()
        {
            @Override
            public void stateChanged(TaskState newState)
            {
                log.debug("Task %s is %s", taskId, newState);
            }
        });
    }

    public DateTime getCreatedTime()
    {
        return createdTime;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public TaskState getState()
    {
        return taskState.get();
    }

    public ListenableFuture<TaskState> getStateChange(TaskState currentState)
    {
        requireNonNull(currentState, "currentState is null");
        checkArgument(!currentState.isDone(), "Current state is already done");

        ListenableFuture<TaskState> future = taskState.getStateChange(currentState);
        TaskState state = taskState.get();
        if (state.isDone()) {
            return immediateFuture(state);
        }
        return future;
    }

    public LinkedBlockingQueue<Throwable> getFailureCauses()
    {
        return failureCauses;
    }

    public void transitionToFlushing()
    {
        taskState.setIf(FLUSHING, currentState -> currentState == RUNNING);
    }

    public void finished()
    {
        transitionToDoneState(TaskState.FINISHED);
    }

    public void cancel(TaskState targetState)
    {
        transitionToDoneState(targetState);
    }

    public void failed(Throwable cause)
    {
        failureCauses.add(cause);
        transitionToDoneState(TaskState.FAILED);
    }

    private void transitionToDoneState(TaskState doneState)
    {
        requireNonNull(doneState, "doneState is null");
        checkArgument(doneState.isDone(), "doneState %s is not a done state", doneState);

        taskState.setIf(doneState, currentState -> !currentState.isDone());
    }

    public void suspend()
    {
        taskState.set(SUSPENDED);
    }

    public void resume()
    {
        taskState.set(RUNNING);
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateChangeListener<TaskState> stateChangeListener)
    {
        taskState.addStateChangeListener(stateChangeListener);
    }

    /**
     * Add listener to the tail, this listener will be notified at last when state changed.
     * @param stateChangeListener listener of state change.
     */
    public void addStateChangeListenerToTail(StateChangeListener<TaskState> stateChangeListener)
    {
        taskState.addStateChangeListenerToTail(stateChangeListener);
    }

    public void sourceTaskFailed(TaskId taskId, Throwable failure)
    {
        List<TaskFailureListener> listeners;
        synchronized (this) {
            sourceTaskFailures.putIfAbsent(taskId, failure);
            listeners = ImmutableList.copyOf(sourceTaskFailureListeners);
        }
        executor.execute(() -> {
            for (TaskFailureListener listener : listeners) {
                listener.onTaskFailed(taskId, failure);
            }
        });
    }

    public void addSourceTaskFailureListener(TaskFailureListener listener)
    {
        Map<TaskId, Throwable> failures;
        synchronized (this) {
            sourceTaskFailureListeners.add(listener);
            failures = ImmutableMap.copyOf(sourceTaskFailures);
        }
        executor.execute(() -> {
            failures.forEach(listener::onTaskFailed);
        });
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .add("taskState", taskState)
                .add("failureCauses", failureCauses)
                .toString();
    }
}
