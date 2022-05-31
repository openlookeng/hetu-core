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
package io.hetu.core.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.function.ConnectorConfig;
import io.prestosql.spi.queryeditorui.ConnectorUtil;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;
import io.prestosql.spi.type.Type;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

import static io.hetu.core.plugin.mongodb.ObjectIdType.OBJECT_ID;

/**
 * hetu plugin to use mongodb as a data source.
 */
@ConnectorConfig(connectorLabel = "MongoDB: Allow access to MongoDB data from openLooKeng",
        propertiesEnabled = true,
        docLink = "https://openlookeng.io/docs/docs/connector/mongodb.html",
        configLink = "https://openlookeng.io/docs/docs/connector/mongodb.html#configuration")
public class MongoPlugin
        implements Plugin
{
    @Override
    public Iterable<Type> getTypes()
    {
        return ImmutableList.of(OBJECT_ID);
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.of(ObjectIdFunctions.class);
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new MongoConnectorFactory("mongodb"));
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = MongoPlugin.class.getAnnotation(ConnectorConfig.class);
        return ConnectorUtil.assembleConnectorProperties(connectorConfig,
                Arrays.asList(MongoClientConfig.class.getDeclaredMethods()));
    }
}
