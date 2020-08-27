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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.prestosql.client.ClientSession;

import javax.inject.Provider;

import java.net.URI;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ClientSessionFactory
{
    private final String defaultSchema;
    private final String catalog;
    private final String source;
    private final String user;
    private final Provider<URI> server;
    private final ZoneId timeZoneId;
    private final Locale locale;
    private final Duration clientSessionTimeout;

    public ClientSessionFactory(Provider<URI> server, String user, String source, String catalog, String defaultSchema, Duration clientSessionTimeout)
    {
        this.server = server;
        this.user = user;
        this.source = source;
        this.catalog = catalog;
        this.defaultSchema = defaultSchema;
        this.timeZoneId = TimeZone.getTimeZone("UTC").toZoneId();
        this.locale = Locale.getDefault();
        this.clientSessionTimeout = firstNonNull(clientSessionTimeout, succinctDuration(1, MINUTES));
    }

    public URI getServer()
    {
        return server.get();
    }

    public ClientSession create(String user, String catalog, String schema, Map<String, String> properties)
    {
        return create(user, catalog, schema, properties, source);
    }

    public ClientSession create(String user, String catalog, String schema, Map<String, String> properties, String source)
    {
        return new ClientSession(server.get(),
                user,
                source,
                Optional.empty(),
                ImmutableSet.of(),
                null,
                catalog == null ? this.catalog : catalog,
                schema == null ? this.defaultSchema : schema,
                null,
                timeZoneId,
                locale,
                ImmutableMap.<String, String>of(),
                properties,
                ImmutableMap.<String, String>of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                null,
                clientSessionTimeout);
    }

    public ClientSession create(String source, String user)
    {
        return create(user, this.catalog, defaultSchema, ImmutableMap.<String, String>of(), source);
    }

    public ClientSession create()
    {
        return create(user, catalog, defaultSchema, ImmutableMap.<String, String>of());
    }
}
