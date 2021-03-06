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
package io.prestosql.queryeditorui.store.history;

import io.prestosql.queryeditorui.protocol.Job;
import io.prestosql.queryeditorui.protocol.Table;

import java.util.List;
import java.util.Optional;

public interface JobHistoryStore
{
    List<Job> getRecentlyRunForUser(Optional<String> user, long maxResults);

    List<Job> getRecentlyRunForUser(Optional<String> user, long maxResults, Iterable<Table> tables);

    List<Job> getRecentlyRunForUser(Optional<String> user, long maxResults, Table table1, Table... otherTables);

    void addRun(Job job);
}
