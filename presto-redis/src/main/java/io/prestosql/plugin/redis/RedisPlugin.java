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

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.redis.description.RedisTableDescription;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.ConnectorConfig;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

@ConnectorConfig(connectorLabel = "Redis: Alllow the use of Redis KV as tables in openLooKeng",
        propertiesEnabled = true,
        catalogConfigFilesEnabled = true,
        globalConfigFilesEnabled = true,
        docLink = "https://openlookeng.io/docs/docs/connector/redis.html",
        configLink = "https://openlookeng.io/docs/docs/connector/reids.html#configuration")
public class RedisPlugin
        implements Plugin
{
    private Optional<Supplier<Map<SchemaTableName, RedisTableDescription>>> tableDescriptionSupplier = Optional.empty();

    public synchronized void setTableDescriptionSupplier(final Supplier<Map<SchemaTableName, RedisTableDescription>> tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = Optional.ofNullable(tableDescriptionSupplier);
    }

    @Override
    public synchronized Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new RedisConnectorFactory(tableDescriptionSupplier));
    }
}
