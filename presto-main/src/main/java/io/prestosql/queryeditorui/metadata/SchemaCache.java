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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.client.JsonResponse;
import io.prestosql.client.QueryData;
import io.prestosql.client.ServerInfo;
import io.prestosql.client.StatementClient;
import io.prestosql.connector.DataCenterConnectorManager;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.queryeditorui.execution.BackgroundCacheLoader;
import io.prestosql.queryeditorui.execution.QueryClient;
import io.prestosql.queryeditorui.execution.QueryRunner;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.joda.time.Duration;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;

public class SchemaCache
        implements Closeable
{
    private static final Logger log = Logger.get(SchemaCache.class);
    private static final Set<String> EXCLUDED_SCHEMAS = Sets.newHashSet("sys");

    private final ExecutorService executor;
    private final QueryRunner.QueryRunnerFactory queryRunnerFactory;
    private final LoadingCache<String, Map<String, List<String>>> schemaTableCache;
    private final LoadingCache<String, List<String>> catalogSchemaCache;

    public SchemaCache(final QueryRunner.QueryRunnerFactory queryRunnerFactory,
            final ExecutorService executor, int cacheExpiryMin)
    {
        this.queryRunnerFactory = requireNonNull(queryRunnerFactory, "queryRunnerFactory session was null!");
        this.executor = requireNonNull(executor, "executor was null!");

        ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(executor);
        BackgroundCacheLoader<String, Map<String, List<String>>> loader =
                new BackgroundCacheLoader<String, Map<String, List<String>>>(listeningExecutor)
                {
                    @Override
                    public Map<String, List<String>> load(String catalogName)
                    {
                        return queryMetadata(format(
                                "SELECT table_catalog, table_schema, table_name " +
                                        "FROM %s.information_schema.tables " +
                                        "WHERE table_catalog = '%s'",
                                catalogName, catalogName));
                    }
                };

        schemaTableCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(cacheExpiryMin, TimeUnit.MINUTES)
                .build(loader);
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        cacheBuilder.refreshAfterWrite(cacheExpiryMin, TimeUnit.MINUTES);
        catalogSchemaCache = cacheBuilder.build(new CacheLoader<String, List<String>>()
        {
            @Override
            public List<String> load(String key)
                    throws Exception
            {
                return querySchemas(format("SELECT schema_name FROM %s.information_schema.schemata", key));
            }
        });
    }

    private Map<String, List<String>> queryMetadata(String query)
    {
        final Map<String, List<String>> cache = new HashMap<>();
        QueryRunner queryRunner = queryRunnerFactory.create("ui-server", "lk");
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
                            String schema = (String) row.get(1);
                            String table = (String) row.get(2);

                            if (EXCLUDED_SCHEMAS.contains(schema)) {
                                continue;
                            }

                            List<String> tables = cache.get(schema);

                            if (tables == null) {
                                tables = new ArrayList<>();
                                cache.put(schema, tables);
                            }

                            tables.add(table);
                        }
                    }

                    return null;
                }
            });
        }
        catch (QueryClient.QueryTimeOutException e) {
            log.error("Caught timeout loading columns", e);
        }

        return ImmutableMap.copyOf(cache);
    }

    private List<String> querySchemas(String query)
    {
        final ImmutableList.Builder<String> resultsBuilder = ImmutableList.builder();
        QueryRunner queryRunner = queryRunnerFactory.create("ui-server", "lk");
        QueryClient queryClient = new QueryClient(queryRunner, Duration.standardSeconds(120), query);

        try {
            queryClient.executeWith(new Function<StatementClient, Void>() {
                @Nullable
                @Override
                public Void apply(StatementClient client)
                {
                    QueryData results = client.currentData();
                    if (results.getData() != null) {
                        for (List<Object> row : results.getData()) {
                            String schema = (String) row.get(0);
                            if (EXCLUDED_SCHEMAS.contains(schema)) {
                                continue;
                            }
                            resultsBuilder.add(schema);
                        }
                    }

                    return null;
                }
            });
        }
        catch (QueryClient.QueryTimeOutException e) {
            log.error("Caught timeout loading columns", e);
        }

        return resultsBuilder.build();
    }

    public void populateCache(CatalogManager catalogManager, DataCenterConnectorManager dataCenterConnectorManager)
    {
        log.info("Pre-Populating schema cache");
        JsonCodec<ServerInfo> serverInfoCodec = jsonCodec(ServerInfo.class);
        OkHttpClient httpClient = queryRunnerFactory.getHttpClient();
        executor.execute(() -> {
            Request request = new Request.Builder()
                    .url(queryRunnerFactory.getSessionFactory().getServer().toString() + "/v1/info")
                    .build();
            //Wait for startup
            while (true) {
                try {
                    JsonResponse<ServerInfo> response = JsonResponse.execute(serverInfoCodec, httpClient, request);
                    if ((response.getStatusCode() != HTTP_OK) || !response.hasValue() || response.getValue().isStarting()) {
                        Thread.sleep(1000);
                        continue;
                    }
                    break;
                }
                catch (InterruptedException e) {
                    break;
                }
            }
            dataCenterConnectorManager.loadAllDCCatalogs();
            List<Catalog> catalogs = catalogManager.getCatalogs();
            for (Catalog catalog : catalogs) {
                executor.execute(() -> {
                    try {
                        getSchemasForCatalog(catalog.getCatalogName());
                        getSchemaMap(catalog.getCatalogName(), true);
                    }
                    catch (ExecutionException e) {
                    }
                });
            }
        });
    }

    public void refreshCache(final String catalog)
    {
        requireNonNull(catalog, "schemaName is null");
        executor.execute(() -> {
            //Refresh synchronously
            schemaTableCache.invalidate(catalog);
            schemaTableCache.refresh(catalog);
        });
        catalogSchemaCache.refresh(catalog);
    }

    public Set<String> getCatalogs()
    {
        return schemaTableCache.asMap().keySet();
    }

    public Map<String, List<String>> getSchemaMap(final String catalog, boolean force)
    {
        try {
            return schemaTableCache.get(catalog);
        }
        catch (ExecutionException e) {
            e.printStackTrace();
            return new HashMap<>();
        }
    }

    public List<String> getSchemasForCatalog(String catalog)
            throws ExecutionException
    {
        return catalogSchemaCache.get(catalog);
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
    }

    public static ThreadFactory daemonThreadsNamed(String nameFormat)
    {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build();
    }
}
