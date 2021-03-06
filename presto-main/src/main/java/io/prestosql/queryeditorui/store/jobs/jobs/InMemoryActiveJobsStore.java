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

import com.google.common.collect.ImmutableSet;
import io.prestosql.queryeditorui.protocol.Job;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryActiveJobsStore
        implements ActiveJobsStore
{
    private ConcurrentMap<String, Set<Job>> activeJobs = new ConcurrentHashMap<>();

    @Override
    public Set<Job> getJobsForUser(Optional<String> user)
    {
        if (user.isPresent()) {
            Set<Job> tmp = activeJobs.get(user.get());
            if (tmp == null) {
                return Collections.emptySet();
            }
            return ImmutableSet.copyOf(tmp);
        }
        Set<Job> jobsForUser = Collections.newSetFromMap(new ConcurrentHashMap<Job, Boolean>());
        for (Set<Job> tmp : activeJobs.values()) {
            jobsForUser.addAll(tmp);
        }
        return ImmutableSet.copyOf(jobsForUser);
    }

    @Override
    public void jobStarted(Job job)
    {
        Set<Job> jobsForUser = activeJobs.get(job.getUser());

        if (jobsForUser == null) {
            jobsForUser = Collections.newSetFromMap(new ConcurrentHashMap<Job, Boolean>());
            activeJobs.putIfAbsent(job.getUser(), jobsForUser);
        }

        activeJobs.get(job.getUser()).add(job);
    }

    @Override
    public void jobFinished(Job job)
    {
        Set<Job> jobsForUser = activeJobs.get(job.getUser());

        if (jobsForUser == null) {
            return;
        }

        jobsForUser.remove(job);
    }
}
