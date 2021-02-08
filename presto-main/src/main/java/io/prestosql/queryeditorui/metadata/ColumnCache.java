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
import io.prestosql.client.Column;
import io.prestosql.client.QueryData;
import io.prestosql.client.StatementClient;
import io.prestosql.queryeditorui.QueryEditorUIModule;
import io.prestosql.queryeditorui.execution.BackgroundCacheLoader;
import io.prestosql.queryeditorui.execution.QueryClient;
import io.prestosql.queryeditorui.execution.QueryRunner;
import io.prestosql.queryeditorui.execution.QueryRunner.QueryRunnerFactory;
import io.prestosql.server.protocol.Query;
import io.prestosql.spi.type.TypeSignature;
import org.joda.time.Duration;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ColumnCache
{
    private static final Logger log = Logger.get(ColumnCache.class);
    private static final Joiner FQN_JOINER = Joiner.on('.').skipNulls();
    private final LoadingCache<String, List<Column>> tableColumnCache;
    private final QueryRunnerFactory queryRunnerFactory;
    private final ExecutorService executor;

    public ColumnCache(final QueryRunnerFactory queryRunnerFactory,
            final ExecutorService executor, int cacheExpiryMin)
    {
        this.queryRunnerFactory = requireNonNull(queryRunnerFactory, "queryRunnerFactory session was null!");
        this.executor = requireNonNull(executor, "executor was null!");

        ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(executor);

        BackgroundCacheLoader<String, List<Column>> columnLoader = new BackgroundCacheLoader<String,
                        List<Column>>(listeningExecutor)
        {
            @Override
            public List<Column> load(String fqTableName)
            {
                return queryColumns(format("SHOW COLUMNS FROM %s", fqTableName));
            }
        };

        this.tableColumnCache = CacheBuilder.newBuilder()
                .expireAfterWrite(cacheExpiryMin, TimeUnit.MINUTES)
                .build(columnLoader);
    }

    private List<Column> queryColumns(String query)
    {
        final ImmutableList.Builder<Column> cache = ImmutableList.builder();
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
                        for (List<Object> row : results.getData()) {
                            TypeSignature typeSignature = TypeSignature.parseTypeSignature((String) row.get(1));
                            Column column = new Column((String) row.get(0), (String) row.get(1), Query.toClientTypeSignature(typeSignature));
                            cache.add(column);
                        }
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

    public void refreshCache()
    {
        tableColumnCache.invalidateAll();
    }

    public void populateCache(final String fqnTableName)
    {
        requireNonNull(fqnTableName, "fqnTableName is null");
        executor.execute(() -> tableColumnCache.refresh(fqnTableName));
    }

    public List<Column> getColumns(String schemaName, String tableName) throws ExecutionException
    {
        return tableColumnCache.get(getFqnTableName(schemaName, tableName));
    }

    public static String getFqnTableName(String schemaName, String tableName)
    {
        return FQN_JOINER.join(schemaName, tableName);
    }
}
