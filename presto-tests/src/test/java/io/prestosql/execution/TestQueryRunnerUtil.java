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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.QueryId;
import io.prestosql.tests.DistributedQueryRunner;

import java.util.Map;
import java.util.Set;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.prestosql.execution.QueryState.RUNNING;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class TestQueryRunnerUtil
{
    private TestQueryRunnerUtil() {}

    public static QueryId createQuery(DistributedQueryRunner queryRunner, Session session, String sql)
    {
        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        getFutureValue(dispatchManager.createQuery(session.getQueryId(), "slug", new TestingSessionContext(session), sql, false));
        return session.getQueryId();
    }

    public static void cancelQuery(DistributedQueryRunner queryRunner, QueryId queryId)
    {
        queryRunner.getCoordinator().getDispatchManager().cancelQuery(queryId);
    }

    public static void waitForQueryState(DistributedQueryRunner queryRunner, QueryId queryId, QueryState expectedQueryState)
            throws InterruptedException
    {
        waitForQueryState(queryRunner, queryId, ImmutableSet.of(expectedQueryState));
    }

    public static void waitForQueryState(DistributedQueryRunner queryRunner, QueryId queryId, Set<QueryState> expectedQueryStates)
            throws InterruptedException
    {
        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        do {
            // Heartbeat all the running queries, so they don't die while we're waiting
            for (BasicQueryInfo queryInfo : dispatchManager.getQueries()) {
                if (queryInfo.getState() == RUNNING) {
                    dispatchManager.getQueryInfo(queryInfo.getQueryId());
                }
            }
            MILLISECONDS.sleep(500);
        }
        while (!expectedQueryStates.contains(dispatchManager.getQueryInfo(queryId).getState()));
    }

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .setExtraProperties(extraProperties)
                .setNodeCount(2)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
