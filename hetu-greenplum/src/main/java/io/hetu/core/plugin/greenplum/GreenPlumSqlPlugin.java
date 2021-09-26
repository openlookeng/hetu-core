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
package io.hetu.core.plugin.greenplum;

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.JdbcPlugin;
import io.prestosql.spi.function.ConnectorConfig;
import io.prestosql.spi.queryeditorui.ConnectorUtil;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import static io.hetu.core.plugin.greenplum.GreenPlumConstants.GREENPLUM_CONNECTOR_NAME;

@ConnectorConfig(connectorLabel = "Greenplum: Query data in Greenplum datasource",
        propertiesEnabled = true,
        catalogConfigFilesEnabled = true,
        globalConfigFilesEnabled = true,
        docLink = "https://openlookeng.io/docs/docs/connector/greenplum.html",
        configLink = "https://openlookeng.io/docs/docs/connector/greenplum.html#configuration")
public class GreenPlumSqlPlugin
        extends JdbcPlugin
{
    public GreenPlumSqlPlugin()
    {
        super(GREENPLUM_CONNECTOR_NAME, new GreenPlumClientModule());
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = GreenPlumSqlPlugin.class.getAnnotation(ConnectorConfig.class);
        ArrayList<Method> methods = new ArrayList<>();
        methods.addAll(Arrays.asList(BaseJdbcConfig.class.getDeclaredMethods()));
        methods.addAll(Arrays.asList(GreenPlumSqlConfig.class.getDeclaredMethods()));
        return ConnectorUtil.assembleConnectorProperties(connectorConfig, methods);
    }
}
