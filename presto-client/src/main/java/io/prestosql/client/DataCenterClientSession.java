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
package io.prestosql.client;

import io.airlift.units.Duration;
import io.prestosql.spi.type.TypeManager;

import java.net.URI;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DataCenterClientSession
        extends ClientSession
{
    private final Duration maxAnticipatedDelay;
    private final boolean compressionEnabled;
    private TypeManager typeManager;

    private DataCenterClientSession(URI server, String user, String source, Optional<String> traceToken, Set<String> clientTags, String clientInfo, String catalog, String schema, String path, ZoneId timeZone, Locale locale, Map<String, String> resourceEstimates, Map<String, String> properties, Map<String, String> preparedStatements, Map<String, ClientSelectedRole> roles, Map<String, String> extraCredentials, String transactionId, Duration clientRequestTimeout, Duration maxAnticipatedDelay, boolean compressionEnabled, TypeManager typeManager)
    {
        super(server, user, source, traceToken, clientTags, clientInfo, catalog, schema, path, timeZone, locale, resourceEstimates, properties, preparedStatements, roles, extraCredentials, transactionId, clientRequestTimeout);
        this.maxAnticipatedDelay = maxAnticipatedDelay;
        this.compressionEnabled = compressionEnabled;
        this.typeManager = typeManager;
    }

    public static Builder builder(DataCenterClientSession clientSession)
    {
        return new Builder(clientSession);
    }

    public static Builder builder(URI server, String user)
    {
        return new Builder(server, user);
    }

    public Duration getMaxAnticipatedDelay()
    {
        return maxAnticipatedDelay;
    }

    public boolean isCompressionEnabled()
    {
        return compressionEnabled;
    }

    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    /**
     * The type Builder.
     */
    public static final class Builder
    {
        private URI server;
        private String user;
        private String source;
        private Optional<String> traceToken;
        private Set<String> clientTags;
        private String clientInfo;
        private String catalog;
        private String schema;
        private String path;
        private ZoneId timeZone;
        private Locale locale;
        private Map<String, String> resourceEstimates;
        private Map<String, String> properties;
        private Map<String, String> preparedStatements;
        private Map<String, ClientSelectedRole> roles;
        private Map<String, String> credentials;
        private String transactionId;
        private Duration clientRequestTimeout;
        private Duration maxAnticipatedDelay;
        private boolean compressionEnabled;
        private TypeManager typeManager;

        private Builder(URI server, String user)
        {
            this.server = server;
            this.user = user;
            traceToken = Optional.empty();
            clientTags = Collections.emptySet();
            resourceEstimates = Collections.emptyMap();
            properties = Collections.emptyMap();
            preparedStatements = Collections.emptyMap();
            roles = Collections.emptyMap();
            credentials = Collections.emptyMap();
        }

        private Builder(DataCenterClientSession clientSession)
        {
            requireNonNull(clientSession, "clientSession is null");
            server = clientSession.getServer();
            user = clientSession.getUser();
            source = clientSession.getSource();
            traceToken = clientSession.getTraceToken();
            clientTags = clientSession.getClientTags();
            clientInfo = clientSession.getClientInfo();
            catalog = clientSession.getCatalog();
            schema = clientSession.getSchema();
            path = clientSession.getPath();
            timeZone = clientSession.getTimeZone();
            locale = clientSession.getLocale();
            resourceEstimates = clientSession.getResourceEstimates();
            properties = clientSession.getProperties();
            preparedStatements = clientSession.getPreparedStatements();
            roles = clientSession.getRoles();
            credentials = clientSession.getExtraCredentials();
            transactionId = clientSession.getTransactionId();
            clientRequestTimeout = clientSession.getClientRequestTimeout();
            maxAnticipatedDelay = clientSession.getMaxAnticipatedDelay();
            compressionEnabled = clientSession.isCompressionEnabled();
            typeManager = clientSession.getTypeManager();
        }

        public Builder withCatalog(String catalog)
        {
            this.catalog = catalog;
            return this;
        }

        public Builder withSchema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public Builder withPath(String path)
        {
            this.path = path;
            return this;
        }

        public Builder withProperties(Map<String, String> properties)
        {
            this.properties = requireNonNull(properties, "properties is null");
            return this;
        }

        public Builder withRoles(Map<String, ClientSelectedRole> roles)
        {
            this.roles = roles;
            return this;
        }

        public Builder withCredentials(Map<String, String> credentials)
        {
            this.credentials = requireNonNull(credentials, "extraCredentials is null");
            return this;
        }

        public Builder withPreparedStatements(Map<String, String> preparedStatements)
        {
            this.preparedStatements = requireNonNull(preparedStatements, "preparedStatements is null");
            return this;
        }

        public Builder withTransactionId(String transactionId)
        {
            this.transactionId = transactionId;
            return this;
        }

        public Builder withoutTransactionId()
        {
            this.transactionId = null;
            return this;
        }

        public Builder withMaxAnticipatedDelay(Duration delay)
        {
            this.maxAnticipatedDelay = delay;
            return this;
        }

        public Builder withCompression(boolean enabled)
        {
            this.compressionEnabled = enabled;
            return this;
        }

        public Builder withSource(String source)
        {
            this.source = source;
            return this;
        }

        public Builder withTimezone(ZoneId zoneId)
        {
            this.timeZone = zoneId;
            return this;
        }

        public Builder withLocale(Locale locale)
        {
            this.locale = locale;
            return this;
        }

        public Builder withClientTimeout(Duration timeout)
        {
            this.clientRequestTimeout = timeout;
            return this;
        }

        public Builder withServer(URI server)
        {
            this.server = server;
            return this;
        }

        public Builder withUser(String user)
        {
            this.user = user;
            return this;
        }

        public Builder withTypeManager(TypeManager typeManager)
        {
            this.typeManager = typeManager;
            return this;
        }

        public DataCenterClientSession build()
        {
            return new DataCenterClientSession(
                    server,
                    user,
                    source,
                    traceToken,
                    clientTags,
                    clientInfo,
                    catalog,
                    schema,
                    path,
                    timeZone != null ? timeZone : ZoneId.systemDefault(),
                    locale != null ? locale : Locale.getDefault(),
                    resourceEstimates,
                    properties,
                    preparedStatements,
                    roles,
                    credentials,
                    transactionId,
                    clientRequestTimeout,
                    maxAnticipatedDelay,
                    compressionEnabled,
                    typeManager);
        }
    }
}
