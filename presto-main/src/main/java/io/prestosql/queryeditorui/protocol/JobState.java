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
package io.prestosql.queryeditorui.protocol;

import com.google.common.base.Predicate;

public enum JobState
{
    /**
     * Query has been accepted and is awaiting execution.
     */
    QUEUED(false),
    /**
     * Query is waiting for the required resources (beta).
     */
    WAITING_FOR_RESOURCES(false),
    /**
     * Query is being dispatched to a coordinator.
     */
    DISPATCHING(false),
    /**
     * Query is being planned.
     */
    PLANNING(false),
    /**
     * Query execution is being started.
     */
    STARTING(false),
    /**
     * Query has at least one task in the output stage.
     */
    RUNNING(false),
    /**
     * Failed tasks will be re-scheduled. Waiting for old stages/tasks to finish.
     */
    RESCHEDULING(false),
    /**
     * Resume execution of rescheduled tasks, after old stages/tasks finish.
     */
    RESUMING(false),
    /**
     * Query is finishing (e.g. commit for autocommit queries)
     */
    FINISHING(false),
    /**
     * Query has finished executing and all output has been consumed.
     */
    FINISHED_EXECUTION(false),
    /**
     * Job has finished forwarding all output to S3/Hive
     */
    FINISHED(true),
    /**
     * Query was canceled by a user.
     */
    CANCELED(true),
    /**
     * Query execution failed.
     */
    FAILED(true);

    private final boolean doneState;

    JobState(boolean doneState)
    {
        this.doneState = doneState;
    }

    /**
     * Is this a terminal state.
     */
    public boolean isDone()
    {
        return doneState;
    }

    public static Predicate<JobState> inDoneState()
    {
        return state -> state.isDone();
    }

    public static JobState fromStatementState(String statementState)
    {
        String state = statementState.equalsIgnoreCase("FINISHED") ? "FINISHED_EXECUTION" : statementState;
        return JobState.valueOf(state);
    }
}
