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
package io.prestosql.plugin.thrift;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.function.ConnectorConfig;
import io.prestosql.spi.queryeditorui.ConnectorUtil;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;

import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

@ConnectorConfig(connectorLabel = "Thrift : Connect to Thrift servers to integrate with external storage systems",
        propertiesEnabled = true,
        catalogConfigFilesEnabled = true,
        globalConfigFilesEnabled = true,
        docLink = "https://openlookeng.io/docs/docs/connector/thrift.html",
        configLink = "https://openlookeng.io/docs/docs/connector/thrift.html#configuration")
public class ThriftPlugin
        implements Plugin
{
    private final String name;
    private final Module module;

    public ThriftPlugin()
    {
        this(getPluginInfo());
    }

    private ThriftPlugin(ThriftPluginInfo info)
    {
        this(info.getName(), info.getModule());
    }

    public ThriftPlugin(String name, Module module)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = requireNonNull(module, "module is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new ThriftConnectorFactory(name, module));
    }

    private static ThriftPluginInfo getPluginInfo()
    {
        ClassLoader classLoader = ThriftPlugin.class.getClassLoader();
        ServiceLoader<ThriftPluginInfo> loader = ServiceLoader.load(ThriftPluginInfo.class, classLoader);
        List<ThriftPluginInfo> list = ImmutableList.copyOf(loader);
        return list.isEmpty() ? new ThriftPluginInfo() : getOnlyElement(list);
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = ThriftPlugin.class.getAnnotation(ConnectorConfig.class);
        Optional<ConnectorWithProperties> connectorWithProperties = ConnectorUtil.assembleConnectorProperties(connectorConfig,
                ImmutableList.of());
        if (connectorWithProperties.isPresent()) {
            ConnectorWithProperties.Properties properties = new ConnectorWithProperties.Properties();
            properties.setName("presto.thrift.client.addresses");
            properties.setDescription("Location of Thrift servers");
            properties.setValue("host1:port,host2:port");
            properties.setRequired(Optional.of(true));
            properties.setReadOnly(Optional.of(false));
            properties.setType(Optional.of("string"));
            connectorWithProperties.get().addProperties(properties);
        }
        return connectorWithProperties;
    }
}
