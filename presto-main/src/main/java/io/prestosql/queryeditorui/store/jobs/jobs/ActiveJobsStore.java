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
package io.prestosql.queryeditorui.store.jobs.jobs;

import io.prestosql.queryeditorui.protocol.Job;

import java.util.Optional;
import java.util.Set;

/**
 * A store for currently running jobs.
 */
public interface ActiveJobsStore
{
    /**
     * Get all running jobs for the specified user.
     *
     * @param user The user to retrieve jobs for.
     * @return All currently running jobs for this user.
     */
    Set<Job> getJobsForUser(Optional<String> user);

    /**
     * Mark a job as having started.
     *
     * @param job The job that has started.
     */
    void jobStarted(Job job);

    /**
     * Mark a job as having finished.
     *
     * @param job The job that has finished.
     */
    void jobFinished(Job job);
}
