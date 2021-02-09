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
package io.prestosql.plugin.tpch;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.function.ConnectorConfig;
import io.prestosql.spi.queryeditorui.ConnectorUtil;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;

import java.util.Optional;

@ConnectorConfig(connectorLabel = "TPC-H : TPC Benchmark™ H, data is generated on the fly",
        docLink = "https://openlookeng.io/docs/docs/connector/tpch.html",
        configLink = "https://openlookeng.io/docs/docs/connector/tpch.html#configuration")
public class TpchPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new TpchConnectorFactory());
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = TpchPlugin.class.getAnnotation(ConnectorConfig.class);
        return ConnectorUtil.assembleConnectorProperties(connectorConfig, ImmutableList.of());
    }
}
