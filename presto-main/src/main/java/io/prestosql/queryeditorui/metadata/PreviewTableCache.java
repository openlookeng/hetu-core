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
package io.prestosql.queryeditorui.metadata;

import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.log.Logger;
import io.prestosql.client.QueryData;
import io.prestosql.client.StatementClient;
import io.prestosql.queryeditorui.QueryEditorUIModule;
import io.prestosql.queryeditorui.execution.BackgroundCacheLoader;
import io.prestosql.queryeditorui.execution.QueryClient;
import io.prestosql.queryeditorui.execution.QueryRunner;
import io.prestosql.queryeditorui.protocol.Table;
import org.joda.time.Duration;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PreviewTableCache
{
    private static final Logger log = Logger.get(PreviewTableCache.class);
    private static final Joiner FQN_JOINER = Joiner.on('.').skipNulls();
    private static final int PREVIEW_LIMIT = 100;
    private final LoadingCache<Table, List<List<Object>>> previewTableCache;
    private final QueryRunner.QueryRunnerFactory queryRunnerFactory;

    public PreviewTableCache(final QueryRunner.QueryRunnerFactory queryRunnerFactory,
            final ExecutorService executor, int cacheExpiryMin)
    {
        this.queryRunnerFactory = requireNonNull(queryRunnerFactory, "queryRunnerFactory session was null!");

        ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(executor);

        BackgroundCacheLoader<Table, List<List<Object>>> tableLoader =
                new BackgroundCacheLoader<Table, List<List<Object>>>(listeningExecutor)
                {
                    @Override
                    public List<List<Object>> load(Table key)
                            throws Exception
                    {
                        return queryRows(buildQueryWithLimit(key, PREVIEW_LIMIT));
                    }
                };

        this.previewTableCache = CacheBuilder.newBuilder()
                                            .expireAfterWrite(cacheExpiryMin, TimeUnit.MINUTES)
                                            .maximumSize(PREVIEW_LIMIT)
                                            .build(tableLoader);
    }

    private static String buildQueryWithLimit(Table table, int limit)
    {
        return format("SELECT * FROM %s LIMIT %d",
                getFqnTableName(table.getConnectorId(), table.getSchema(), table.getTable()),
                limit);
    }

    private List<List<Object>> queryRows(String query)
    {
        final ImmutableList.Builder<List<Object>> cache = ImmutableList.builder();
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorUIModule.UI_QUERY_SOURCE, "lk");
        QueryClient queryClient = new QueryClient(queryRunner, Duration.standardSeconds(60), query);

        try {
            queryClient.executeWith(new Function<StatementClient, Void>() {
                @Nullable
                @Override
                public Void apply(StatementClient client)
                {
                    QueryData results = client.currentData();
                    if (results.getData() != null) {
                        cache.addAll(results.getData());
                    }

                    return null;
                }
            });
        }
        catch (QueryClient.QueryTimeOutException e) {
            log.error("Caught timeout loading columns", e);
        }

        return cache.build();
    }

    public List<List<Object>> getPreview(final String schema,
            final String table)
            throws ExecutionException
    {
        return previewTableCache.get(Table.valueOf(getFqnTableName(schema, table)));
    }

    public static String getFqnTableName(String connectorId, String schemaName, String tableName)
    {
        return FQN_JOINER.join(connectorId, schemaName, tableName);
    }

    public static String getFqnTableName(String schemaName, String tableName)
    {
        return FQN_JOINER.join(schemaName, tableName);
    }
}
