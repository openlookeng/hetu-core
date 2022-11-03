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
package io.prestosql.execution.scheduler.policy;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.execution.SqlStageExecution;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StagesScheduleResult
{
    private final Set<SqlStageExecution> stagesToSchedule;
    private final Optional<ListenableFuture<?>> rescheduleFuture;

    public StagesScheduleResult(Set<SqlStageExecution> stagesToSchedule)
    {
        this(stagesToSchedule, Optional.empty());
    }

    public StagesScheduleResult(Set<SqlStageExecution> stagesToSchedule, Optional<ListenableFuture<?>> rescheduleFuture)
    {
        this.stagesToSchedule = requireNonNull(stagesToSchedule, "stagesToSchedule is null");
        this.rescheduleFuture = requireNonNull(rescheduleFuture, "rescheduleFuture is null");
    }

    public Set<SqlStageExecution> getStagesToSchedule()
    {
        return stagesToSchedule;
    }

    public Optional<ListenableFuture<?>> getRescheduleFuture()
    {
        return rescheduleFuture;
    }
}
