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
package io.hetu.core.plugin.carbondata;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.HivePlugin;
import io.prestosql.plugin.hive.metastore.thrift.StaticMetastoreConfig;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.function.ConnectorConfig;
import io.prestosql.spi.queryeditorui.ConnectorUtil;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;

import java.util.Arrays;
import java.util.Optional;

@ConnectorConfig(connectorLabel = "Carbondata: Query data stored in a Carbondata warehouse",
        propertiesEnabled = true,
        catalogConfigFilesEnabled = true,
        globalConfigFilesEnabled = true,
        docLink = "https://openlookeng.io/zh-cn/docs/docs/connector/carbondata.html",
        configLink = "https://openlookeng.io/zh-cn/docs/docs/connector/carbondata.html#configuration")
public class CarbondataPlugin
        extends HivePlugin
{
    public CarbondataPlugin()
    {
        super("carbondata");
    }

    private static ClassLoader getClassLoader()
    {
        return FileFactory.class.getClassLoader();
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new CarbondataConnectorFactory("carbondata", getClassLoader()));
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = CarbondataPlugin.class.getAnnotation(ConnectorConfig.class);
        return ConnectorUtil.assembleConnectorProperties(connectorConfig,
                Arrays.asList(StaticMetastoreConfig.class.getDeclaredMethods()));
    }
}
