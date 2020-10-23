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
package io.prestosql.queryeditorui.execution;

import com.google.common.base.Stopwatch;
import io.prestosql.client.QueryStatusInfo;
import io.prestosql.client.StatementClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class QueryClient
{
    private final QueryRunner queryRunner;
    private final Duration timeout;
    private final String query;
    private final AtomicReference<QueryStatusInfo> finalResults = new AtomicReference<>();

    public QueryClient(QueryRunner queryRunner, String query)
    {
        this(queryRunner, Duration.ofSeconds(60 * 30), query);
    }

    public QueryClient(QueryRunner queryRunner, org.joda.time.Duration timeout, String query)
    {
        this(queryRunner, Duration.ofMillis(timeout.getMillis()), query);
    }

    public QueryClient(QueryRunner queryRunner, Duration timeout, String query)
    {
        this.queryRunner = queryRunner;
        this.timeout = timeout;
        this.query = query;
    }

    public <T> T executeWith(Function<StatementClient, T> function)
            throws QueryTimeOutException
    {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        T t = null;

        try (StatementClient client = queryRunner.startInternalQuery(query)) {
            while (client.isRunning() && !Thread.currentThread().isInterrupted()) {
                if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > timeout.toMillis()) {
                    throw new QueryTimeOutException(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                }

                t = function.apply(client);
                client.advance();
            }

            finalResults.set(client.finalStatusInfo());
        }
        catch (RuntimeException | QueryTimeOutException e) {
            stopwatch.stop();
            throw e;
        }

        return t;
    }

    public QueryStatusInfo finalResults()
    {
        return finalResults.get();
    }

    public static class QueryTimeOutException
            extends Throwable
    {
        private final long elapsedMs;

        public QueryTimeOutException(long elapsedMs)
        {
            this.elapsedMs = elapsedMs;
        }

        public long getElapsedMs()
        {
            return elapsedMs;
        }
    }
}
