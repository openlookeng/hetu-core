/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.resourcemanager;

import com.google.inject.Inject;
import io.prestosql.Session;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.memory.ClusterMemoryPoolManager;
import io.prestosql.spi.resourcegroups.ResourceGroupId;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class QueryResourceManagerService
{
    private final ResourceUpdateListener resourceUpdateListener = new ResourceUpdateListener() {
        public void resourceUpdate(QueryId queryId, BasicResourceStats stats, Map<String, BasicResourceStats> nodeResourceMap)
        {
            /* Todo(FutureFeature) Implement resource estimation & reservation for future
             *   also, decision for given query level resource change */
        }
    };
    private final ExecutorService executor;

    @Inject
    QueryResourceManagerService(ClusterMemoryPoolManager memoryPoolManager, @ForResourceMonitor ExecutorService executor)
    {
        /* Todo(FutureFeature) add a resourceMonitor and resourcePlanner based on observed
         *   resource usage pattern over a period of time. */

        this.executor = requireNonNull(executor, "Executor cannot be null");
    }

    private void start()
    {
        executor.submit(this::resourceMonitor);
        executor.submit(this::resourcePlanner);
    }

    private void resourcePlanner()
    {
        /* Do Nothing */

        /* Todo(FutureFeature) add action handler for queries out of bound on resources, like:
         *   - Suspend - Resume
         *   - Apply Grace (add resource so that query can finish and make more resource available)
         *   - Future usage based resource planning
         *   - Resource allocation reduction in case more queries arrive in burst */
    }

    private void resourceMonitor()
    {
        /* Do Nothing */

        /* Todo(FutureFeature) add action handler for queries out of bound on resources, like:
        *   - Apply Penalty
        *   - Trigger Memory Spill
        *   - Suspend - Resume
        *   - Apply Grace (add resource so that query can finish and make more resource available) */
    }

    public QueryResourceManager createQueryResourceManager(QueryId queryId, Session session, ResourceGroupId resourceGroup, ResourceGroupManager resourceGrpMgr)
    {
        return new QueryResourceManager(queryId, session, resourceGroup, resourceGrpMgr, resourceUpdateListener);
    }

    public interface ResourceUpdateListener
    {
        void resourceUpdate(QueryId queryId, BasicResourceStats stats, Map<String, BasicResourceStats> nodeResourceMap);
    }
}
