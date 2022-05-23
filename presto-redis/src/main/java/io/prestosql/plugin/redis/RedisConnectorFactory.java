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
package io.prestosql.plugin.redis;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.plugin.redis.description.RedisTableDescription;
import io.prestosql.plugin.redis.handle.RedisHandleResolver;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class RedisConnectorFactory
        implements ConnectorFactory
{
    private final Optional<Supplier<Map<SchemaTableName, RedisTableDescription>>> tableDescriptionSupplier;

    public RedisConnectorFactory(final Optional<Supplier<Map<SchemaTableName, RedisTableDescription>>> tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
    }

    @Override
    public String getName()
    {
        return "redis";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new RedisHandleResolver();
    }

    @Override
    public Connector create(final String catalogName, final Map<String, String> requiredConfig, final ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(requiredConfig, "requiredConfig is null");
        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new RedisModule(context.getTypeManager(), tableDescriptionSupplier, context.getNodeManager()));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();
            return injector.getInstance(RedisConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
