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
package io.prestosql.elasticsearch;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.type.TypeManager;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ElasticsearchConnectorFactory
        implements ConnectorFactory
{
    ElasticsearchConnectorFactory() {}

    @Override
    public String getName()
    {
        return "elasticsearch";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ElasticsearchHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new ElasticsearchConnectorModule(),
                binder -> {
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                    binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                });

        Injector injector = app.strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(ElasticsearchConnector.class);
    }
}
