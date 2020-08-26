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

package io.hetu.core.plugin.datacenter.client;

import io.hetu.core.plugin.datacenter.DataCenterConfig;
import io.prestosql.client.DataCenterClientSession;
import io.prestosql.client.StatementClient;
import io.prestosql.spi.type.TypeManager;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;

import javax.inject.Singleton;

import java.io.File;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.net.HostAndPort.fromString;
import static io.prestosql.client.KerberosUtil.defaultCredentialCachePath;
import static io.prestosql.client.OkHttpUtil.basicAuth;
import static io.prestosql.client.OkHttpUtil.setupCookieJar;
import static io.prestosql.client.OkHttpUtil.setupHttpProxy;
import static io.prestosql.client.OkHttpUtil.setupKerberos;
import static io.prestosql.client.OkHttpUtil.setupSocksProxy;
import static io.prestosql.client.OkHttpUtil.setupSsl;
import static io.prestosql.client.OkHttpUtil.tokenAuth;

/**
 * Statement client factory.
 *
 * @since 2020-02-11
 */
@Singleton
public class DataCenterStatementClientFactory
{
    private static final long KEEP_ALIVE_DURATION = 5L;

    private DataCenterStatementClientFactory()
    {
    }

    /**
     * Create a client session.
     *
     * @param config data center config.
     * @param typeManager the type manager.
     * @return a instance of client session.
     */
    public static DataCenterClientSession createClientSession(DataCenterConfig config, TypeManager typeManager)
    {
        return createClientSession(config, typeManager, Collections.emptyMap());
    }

    /**
     * Create a client session.
     * @param config data center config.
     * @param typeManager the type manager.
     * @param properties session properties
     * @return a instance of client session.
     */
    public static DataCenterClientSession createClientSession(DataCenterConfig config, TypeManager typeManager, Map<String, String> properties)
    {
        DataCenterClientSession.Builder builder = DataCenterClientSession.builder(config.getConnectionUrl(),
                config.getConnectionUser())
                .withSource(config.getApplicationNamePrefix())
                .withTimezone(ZoneId.systemDefault())
                .withLocale(Locale.getDefault())
                .withClientTimeout(config.getClientTimeout())
                .withMaxAnticipatedDelay(config.getMaxAnticipatedDelay())
                .withCompression(config.isCompressionEnabled())
                .withProperties(properties)
                .withTypeManager(typeManager);
        return builder.build();
    }

    /**
     * Create a client session.
     *
     * @param client previous statement client.
     * @param config data center config.
     * @param typeManager the type manager.
     * @return a instance of client session.
     */
    public static DataCenterClientSession createClientSession(StatementClient client, DataCenterConfig config,
            TypeManager typeManager)
    {
        DataCenterClientSession.Builder builder = DataCenterClientSession.builder(config.getConnectionUrl(),
                config.getConnectionUser())
                .withSource(config.getApplicationNamePrefix())
                .withLocale(Locale.getDefault())
                .withClientTimeout(config.getClientTimeout())
                .withMaxAnticipatedDelay(config.getMaxAnticipatedDelay())
                .withCompression(config.isCompressionEnabled())
                .withCatalog(client.getSetCatalog().orElse(null))
                .withSchema(client.getSetSchema().orElse(null))
                .withPath(client.getSetPath().orElse(null))
                .withPreparedStatements(client.getAddedPreparedStatements())
                .withRoles(client.getSetRoles())
                .withTransactionId(client.getStartedTransactionId())
                .withTimezone(client.getTimeZone())
                .withProperties(client.getSetSessionProperties())
                .withTypeManager(typeManager);

        return builder.build();
    }

    /**
     * Set up a http client.
     *
     * @param builder okHttpClient builder.
     * @param config data center config.
     */
    private static void setupClient(OkHttpClient.Builder builder, DataCenterConfig config)
    {
        boolean isUseSecureConnection = config.isSsl();

        setupCookieJar(builder);
        setupSocksProxy(builder,
                Optional.ofNullable(config.getSocksProxy() != null ? fromString(config.getSocksProxy()) : null));
        setupHttpProxy(builder,
                Optional.ofNullable(config.getHttpProxy() != null ? fromString(config.getHttpProxy()) : null));

        String password = config.getConnectionPassword();
        if (password != null && !password.isEmpty()) {
            if (!isUseSecureConnection) {
                throw new RuntimeException("Authentication using username/password requires SSL to be enabled");
            }
            builder.addInterceptor(basicAuth(config.getConnectionUser(), password));
        }

        if (isUseSecureConnection) {
            setupSsl(builder, Optional.ofNullable(config.getSslKeyStorePath()),
                    Optional.ofNullable(config.getSslKeyStorePassword()),
                    Optional.ofNullable(config.getSslTrustStorePath()),
                    Optional.ofNullable(config.getSslTrustStorePassword()));
        }

        builder.connectTimeout(config.getHttpRequestConnectTimeout().toMillis(), TimeUnit.MILLISECONDS);
        builder.readTimeout(config.getHttpRequestReadTimeout().toMillis(), TimeUnit.MILLISECONDS);

        String kerberosRemoteServiceName = config.getKerberosRemoteServiceName();
        if (kerberosRemoteServiceName != null && !kerberosRemoteServiceName.isEmpty()) {
            if (!isUseSecureConnection) {
                throw new RuntimeException("Authentication using Kerberos requires SSL to be enabled");
            }
            String kerberosCredentialCachePath = config.getKerberosCredentialCachePath() != null
                    ? config.getKerberosCredentialCachePath()
                    : defaultCredentialCachePath().orElse(null);

            setupKerberos(builder, config.getKerberosServicePrincipalPattern(), kerberosRemoteServiceName,
                    config.isKerberosUseCanonicalHostname(), Optional.ofNullable(config.getKerberosPrincipal()),
                    Optional.ofNullable(
                            config.getKerberosConfigPath() != null ? new File(config.getKerberosConfigPath()) : null),
                    Optional.ofNullable(
                            config.getKerberosKeytabPath() != null ? new File(config.getKerberosKeytabPath()) : null),
                    Optional.ofNullable(
                            kerberosCredentialCachePath != null ? new File(kerberosCredentialCachePath) : null));
        }

        String accessToken = config.getAccessToken();
        if (accessToken != null && !accessToken.isEmpty()) {
            if (!isUseSecureConnection) {
                throw new RuntimeException("Authentication using an access token requires SSL to be enabled");
            }
            builder.addInterceptor(tokenAuth(accessToken));
        }

        builder.connectionPool(
                new ConnectionPool(config.getMaxIdleConnections(), KEEP_ALIVE_DURATION, TimeUnit.MINUTES));
    }

    /**
     * New a http client.
     *
     * @param config data center config.
     * @return http client instance.
     */
    public static OkHttpClient newHttpClient(DataCenterConfig config)
    {
        // Setup the client
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        setupClient(builder, config);
        return builder.build();
    }
}
