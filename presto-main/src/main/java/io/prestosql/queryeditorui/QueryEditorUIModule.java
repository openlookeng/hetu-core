/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.queryeditorui;

import com.google.inject.Binder;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.client.SocketChannelSocketFactory;
import io.prestosql.queryeditorui.execution.ClientSessionFactory;
import io.prestosql.queryeditorui.execution.ExecutionClient;
import io.prestosql.queryeditorui.execution.QueryInfoClient;
import io.prestosql.queryeditorui.execution.QueryRunner.QueryRunnerFactory;
import io.prestosql.queryeditorui.metadata.ColumnService;
import io.prestosql.queryeditorui.metadata.PreviewTableService;
import io.prestosql.queryeditorui.metadata.SchemaService;
import io.prestosql.queryeditorui.output.PersistentJobOutputFactory;
import io.prestosql.queryeditorui.output.builders.OutputBuilderFactory;
import io.prestosql.queryeditorui.output.persistors.CSVPersistorFactory;
import io.prestosql.queryeditorui.output.persistors.PersistorFactory;
import io.prestosql.queryeditorui.protocol.ExecutionStatus.ExecutionError;
import io.prestosql.queryeditorui.protocol.ExecutionStatus.ExecutionSuccess;
import io.prestosql.queryeditorui.resources.ConnectorResource;
import io.prestosql.queryeditorui.resources.FilesResource;
import io.prestosql.queryeditorui.resources.LoginResource;
import io.prestosql.queryeditorui.resources.MetadataResource;
import io.prestosql.queryeditorui.resources.QueryResource;
import io.prestosql.queryeditorui.resources.ResultsPreviewResource;
import io.prestosql.queryeditorui.resources.UIExecuteResource;
import io.prestosql.queryeditorui.resources.UserResource;
import io.prestosql.queryeditorui.security.UiAuthenticator;
import io.prestosql.queryeditorui.store.files.ExpiringFileStore;
import io.prestosql.queryeditorui.store.history.JobHistoryStore;
import io.prestosql.queryeditorui.store.history.LocalJobHistoryStore;
import io.prestosql.queryeditorui.store.jobs.jobs.ActiveJobsStore;
import io.prestosql.queryeditorui.store.jobs.jobs.InMemoryActiveJobsStore;
import io.prestosql.queryeditorui.store.queries.InMemoryQueryStore;
import io.prestosql.queryeditorui.store.queries.QueryStore;
import io.prestosql.server.InternalAuthenticationManager;
import io.prestosql.server.InternalCommunicationConfig;
import io.prestosql.server.security.WebUIAuthenticator;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;

import javax.inject.Named;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.prestosql.client.OkHttpUtil.setupCookieJar;
import static io.prestosql.client.OkHttpUtil.setupSsl;
import static io.prestosql.client.OkHttpUtil.setupTimeouts;
import static java.util.concurrent.TimeUnit.SECONDS;

public class QueryEditorUIModule
        extends AbstractConfigurationAwareModule
{
    public static final String UI_QUERY_SOURCE = "ui-server";
    private static final ConfigDefaults<HttpClientConfig> HTTP_CLIENT_CONFIG_DEFAULTS = d -> new HttpClientConfig()
            .setConnectTimeout(new Duration(10, TimeUnit.SECONDS));

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(QueryEditorConfig.class);

        //resources
        jsonCodecBinder(binder).bindJsonCodec(ExecutionSuccess.class);
        jsonCodecBinder(binder).bindJsonCodec(ExecutionError.class);
        jaxrsBinder(binder).bind(UIExecuteResource.class);
        jaxrsBinder(binder).bind(FilesResource.class);
        jaxrsBinder(binder).bind(QueryResource.class);
        jaxrsBinder(binder).bind(ResultsPreviewResource.class);
        jaxrsBinder(binder).bind(MetadataResource.class);
        jaxrsBinder(binder).bind(ConnectorResource.class);
        jaxrsBinder(binder).bind(LoginResource.class);
        jaxrsBinder(binder).bind(UserResource.class);

        binder.bind(SchemaService.class).in(Scopes.SINGLETON);
        binder.bind(ColumnService.class).in(Scopes.SINGLETON);
        binder.bind(PreviewTableService.class).in(Scopes.SINGLETON);
        binder.bind(ExecutionClient.class).in(Scopes.SINGLETON);
        binder.bind(PersistentJobOutputFactory.class).in(Scopes.SINGLETON);
        binder.bind(JobHistoryStore.class).to(LocalJobHistoryStore.class).in(Scopes.SINGLETON);

        httpClientBinder(binder).bindHttpClient("query-info", ForQueryInfoClient.class)
                .withConfigDefaults(HTTP_CLIENT_CONFIG_DEFAULTS);
        binder.bind(WebUIAuthenticator.class).to(UiAuthenticator.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public ExpiringFileStore provideExpiringFileStore(QueryEditorConfig config)
    {
        return new ExpiringFileStore(config.getMaxResultCount());
    }

    @Provides
    @Singleton
    public CSVPersistorFactory provideCSVPersistorFactory(ExpiringFileStore fileStore)
    {
        return new CSVPersistorFactory(fileStore);
    }

    @Provides
    @Singleton
    public PersistorFactory providePersistorFactory(CSVPersistorFactory csvPersistorFactory)
    {
        return new PersistorFactory(csvPersistorFactory);
    }

    @Provides
    @Singleton
    public ActiveJobsStore provideActiveJobsStore()
    {
        return new InMemoryActiveJobsStore();
    }

    @Provides
    @Singleton
    public OutputBuilderFactory provideOutputBuilderFactory(QueryEditorConfig config)
    {
        long maxFileSizeInBytes = Math.round(Math.floor(config.getMaxResultSize().getValue(DataSize.Unit.BYTE)));
        return new OutputBuilderFactory(maxFileSizeInBytes, false);
    }

    @Singleton
    @Provides
    public OkHttpClient provideOkHttpClient(HttpServerConfig serverConfig, InternalCommunicationConfig internalCommunicationConfig,
            InternalAuthenticationManager internalAuthenticationManager)
    {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        builder.socketFactory(new SocketChannelSocketFactory());

        setupTimeouts(builder, 30, SECONDS);
        setupCookieJar(builder);
        if (serverConfig.isHttpsEnabled() && internalCommunicationConfig.isHttpsRequired()) {
            setupSsl(builder, Optional.of(serverConfig.getKeystorePath()), Optional.of(serverConfig.getKeystorePassword()),
                    Optional.of(serverConfig.getKeystorePath()), Optional.of(serverConfig.getKeystorePassword()));

            //Setup the authorization for UI queries
            Interceptor authInterceptor = chain -> chain.proceed(
                    internalAuthenticationManager.getJwtGenerator()
                            .map(Supplier::get)
                            .map(jwt ->
                                    chain.request().newBuilder()
                                            .header(InternalAuthenticationManager.PRESTO_INTERNAL_BEARER, jwt)
                                            .build())
                            .orElse(chain.request()));
            builder.addInterceptor(authInterceptor);
            internalAuthenticationManager.getJwtGenerator().map(Supplier::get)
                    .orElse(null);
        }
        return builder.build();
    }

    @Named("coordinator-uri")
    @Provides
    public URI providePrestoCoordinatorURI(HttpServerConfig serverConfig, InternalCommunicationConfig internalCommunicationConfig,
            QueryEditorConfig queryEditorConfig)
    {
        if (queryEditorConfig.isRunningEmbeded()) {
            if (serverConfig.isHttpsEnabled() && internalCommunicationConfig.isHttpsRequired()) {
                return URI.create("https://localhost:" + serverConfig.getHttpsPort());
            }
            return URI.create("http://localhost:" + serverConfig.getHttpPort());
        }
        else {
            return URI.create(queryEditorConfig.getCoordinatorUri());
        }
    }

    @Singleton
    @Named("default-catalog")
    @Provides
    public String provideDefaultCatalog()
    {
        return "hive";
    }

    @Provides
    @Singleton
    public ClientSessionFactory provideClientSessionFactory(@Named("coordinator-uri") Provider<URI> uriProvider)
    {
        return new ClientSessionFactory(uriProvider,
                "lk",
                "ui",
                "system",
                "information_schema",
                Duration.succinctDuration(15, TimeUnit.MINUTES));
    }

    @Provides
    public QueryRunnerFactory provideQueryRunner(ClientSessionFactory sessionFactory,
            OkHttpClient httpClient)
    {
        return new QueryRunnerFactory(sessionFactory, httpClient);
    }

    @Provides
    public QueryInfoClient provideQueryInfoClient(OkHttpClient httpClient)
    {
        return new QueryInfoClient(httpClient);
    }

    @Provides
    public QueryStore provideQueryStore(QueryEditorConfig queryEditorConfig) throws IOException
    {
        return new InMemoryQueryStore(new File(queryEditorConfig.getFeaturedQueriesPath()), new File(queryEditorConfig.getUserQueriesPath()));
    }
}
