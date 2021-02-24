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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.prestosql.queryeditorui.EvictingDeque;
import io.prestosql.queryeditorui.protocol.Job;
import io.prestosql.queryeditorui.protocol.Table;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

public class LocalJobHistoryStore
        implements JobHistoryStore
{
    public static class FinishedJobEvictingDeque
            extends EvictingDeque<Job>
    {
        public FinishedJobEvictingDeque(int capacity)
        {
            super(capacity);
        }

        @Override
        protected boolean evictItem(LinkedBlockingDeque<Job> deque)
        {
            final Job job = deque.poll();

            if (job != null) {
                if (job.getState().isDone()) {
                    return true;
                }
                else {
                    boolean addResult = add(job);
                    boolean secondEviction = evictItem(deque);

                    return addResult && secondEviction;
                }
            }
            else {
                return false;
            }
        }
    }

    private final Cache<Table, EvictingDeque<Job>> tableHistoryCache;
    private final EvictingDeque<Job> historyCache;
    private final int maximumHistoryPerTable;

    public LocalJobHistoryStore()
    {
        this(100, 100, 1000);
    }

    public LocalJobHistoryStore(final long maximumTableHistories,
                                final int maximumHistoryPerTable,
                                final long maximumHistoryGeneral)
    {
        this.tableHistoryCache = CacheBuilder.newBuilder()
                .maximumSize(maximumTableHistories)
                .build();

        this.historyCache = new FinishedJobEvictingDeque((int) maximumHistoryGeneral);

        this.maximumHistoryPerTable = maximumHistoryPerTable;
    }

    @Override
    public List<Job> getRecentlyRunForUser(Optional<String> user, long maxResults, Table table1, Table... otherTables)
    {
        return getRecentlyRunForUser(user, maxResults, Lists.asList(table1, otherTables));
    }

    @Override
    public List<Job> getRecentlyRunForUser(Optional<String> user, long maxResults)
    {
        final ImmutableList.Builder<Job> builder = ImmutableList.builder();
        long added = 0;

        for (Iterator<Job> job = historyCache.descendingIterator(); job.hasNext(); ) {
            Job nextJob = job.next();
            if (user.isPresent() && !nextJob.getUser().equals(user.get())) {
                continue;
            }
            if (added + 1 > maxResults) {
                break;
            }

            builder.add(nextJob);
            added += 1;
        }

        return builder.build();
    }

    @Override
    public List<Job> getRecentlyRunForUser(Optional<String> user, long maxResults, Iterable<Table> tables)
    {
        final ImmutableList.Builder<Job> builder = ImmutableList.builder();
        long added = 0;

        for (Map.Entry<Table, EvictingDeque<Job>> entry : tableHistoryCache.getAllPresent(tables).entrySet()) {
            EvictingDeque<Job> deque = entry.getValue();
            if (deque != null) {
                List<Job> filteredJob = deque.stream().collect(Collectors.toList());
                if (user.isPresent()) {
                    filteredJob = deque.stream().filter(job -> job.getUser().equals(user.get())).collect(Collectors.toList());
                }
                final int dequeSize = filteredJob.size();
                if (added + dequeSize > maxResults) {
                    break;
                }
                else {
                    builder.addAll(filteredJob);
                    added += dequeSize;
                }
            }
        }

        return builder.build();
    }

    @Override
    public void addRun(Job job)
    {
        historyCache.add(job);

        for (Table usedTable : job.getTablesUsed()) {
            EvictingDeque<Job> tableCache = tableHistoryCache.getIfPresent(usedTable);

            if (tableCache == null) {
                tableCache = new FinishedJobEvictingDeque(maximumHistoryPerTable);
                tableHistoryCache.put(usedTable, tableCache);
            }

            tableCache.add(job);
        }
    }
}
