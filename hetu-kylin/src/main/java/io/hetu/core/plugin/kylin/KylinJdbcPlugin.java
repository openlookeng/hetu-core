/*
 * Copyright (C) 2018-2020. Autohome Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.kylin;

import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.JdbcPlugin;
import io.prestosql.spi.function.ConnectorConfig;
import io.prestosql.spi.queryeditorui.ConnectorUtil;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

@ConnectorConfig(connectorLabel = "Kylin : Query  tables on an external Kylin database",
        propertiesEnabled = true,
        docLink = "https://openlookeng.io/docs/docs/connector/kylin.html",
        configLink = "https://openlookeng.io/docs/docs/connector/kylin.html#configuration")
public class KylinJdbcPlugin
        extends JdbcPlugin
{
    private static final Logger log = Logger.get(KylinJdbcPlugin.class);

    public KylinJdbcPlugin()
    {
        super("kylin", new KylinClientModule());
        log.info("kylin plugin is registered");
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = KylinJdbcPlugin.class.getAnnotation(ConnectorConfig.class);
        ArrayList<Method> methods = new ArrayList<>();
        methods.addAll(Arrays.asList(BaseJdbcConfig.class.getDeclaredMethods()));
        methods.addAll(Arrays.asList(KylinConfig.class.getDeclaredMethods()));
        Optional<ConnectorWithProperties> connectorWithProperties = ConnectorUtil.assembleConnectorProperties(connectorConfig, methods);
        ConnectorUtil.addConnUrlProperty(connectorWithProperties, "jdbc:kylin://host:port");
        return connectorWithProperties;
    }
}
