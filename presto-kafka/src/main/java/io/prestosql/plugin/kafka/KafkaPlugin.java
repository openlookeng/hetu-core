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
package io.prestosql.plugin.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.ConnectorConfig;
import io.prestosql.spi.queryeditorui.ConnectorUtil;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Presto plugin to use Apache Kafka as a data source.
 */
@ConnectorConfig(connectorLabel = "Kafka: Allow the use of Apache Kafka topics as tables in openLooKeng",
        propertiesEnabled = true,
        catalogConfigFilesEnabled = true,
        globalConfigFilesEnabled = true,
        docLink = "https://openlookeng.io/docs/docs/connector/kafka.html",
        configLink = "https://openlookeng.io/docs/docs/connector/kafka.html#configuration")
public class KafkaPlugin
        implements Plugin
{
    private Optional<Supplier<Map<SchemaTableName, KafkaTopicDescription>>> tableDescriptionSupplier = Optional.empty();

    @VisibleForTesting
    public synchronized void setTableDescriptionSupplier(Supplier<Map<SchemaTableName, KafkaTopicDescription>> tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = Optional.of(requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null"));
    }

    @Override
    public synchronized Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new KafkaConnectorFactory(tableDescriptionSupplier));
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = KafkaPlugin.class.getAnnotation(ConnectorConfig.class);
        return ConnectorUtil.assembleConnectorProperties(connectorConfig,
                Arrays.asList(KafkaConnectorConfig.class.getDeclaredMethods()));
    }
}
