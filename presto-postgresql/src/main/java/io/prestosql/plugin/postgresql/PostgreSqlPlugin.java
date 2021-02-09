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
package io.prestosql.plugin.postgresql;

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.JdbcPlugin;
import io.prestosql.spi.function.ConnectorConfig;
import io.prestosql.spi.queryeditorui.ConnectorUtil;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;

import java.util.Arrays;
import java.util.Optional;

@ConnectorConfig(connectorLabel = "PostgreSQL : Query and create tables on an external PostgreSQL database",
        propertiesEnabled = true,
        docLink = "https://openlookeng.io/docs/docs/connector/postgresql.html",
        configLink = "https://openlookeng.io/docs/docs/connector/postgresql.html#configuration")
public class PostgreSqlPlugin
        extends JdbcPlugin
{
    public PostgreSqlPlugin()
    {
        super("postgresql", new PostgreSqlClientModule());
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = PostgreSqlPlugin.class.getAnnotation(ConnectorConfig.class);
        Optional<ConnectorWithProperties> connectorWithProperties = ConnectorUtil.assembleConnectorProperties(connectorConfig,
                Arrays.asList(BaseJdbcConfig.class.getDeclaredMethods()));
        ConnectorUtil.addConnUrlProperty(connectorWithProperties, "jdbc:postgresql://host:port/database");
        return connectorWithProperties;
    }
}
