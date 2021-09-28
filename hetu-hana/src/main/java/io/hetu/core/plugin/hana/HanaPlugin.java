/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.hana;

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.JdbcMetadataConfig;
import io.prestosql.plugin.jdbc.JdbcPlugin;
import io.prestosql.spi.function.ConnectorConfig;
import io.prestosql.spi.queryeditorui.ConnectorUtil;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

/**
 * HanaPlugin class
 *
 * @since 2019-07-10
 */
@ConnectorConfig(connectorLabel = "Hana: Query and create tables on an external Hana database",
        propertiesEnabled = true,
        docLink = "https://openlookeng.io/docs/docs/connector/hana.html",
        configLink = "https://openlookeng.io/docs/docs/connector/hana.html#configuration")
public class HanaPlugin
        extends JdbcPlugin
{
    /**
     * Hana Plugin Constructor
     */
    public HanaPlugin()
    {
        // name of the connector and the module implementation
        super("hana", new HanaClientModule());
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = HanaPlugin.class.getAnnotation(ConnectorConfig.class);
        ArrayList<Method> methods = new ArrayList<>();
        methods.addAll(Arrays.asList(BaseJdbcConfig.class.getDeclaredMethods()));
        methods.addAll(Arrays.asList(JdbcMetadataConfig.class.getDeclaredMethods()));
        Optional<ConnectorWithProperties> connectorWithProperties = ConnectorUtil.assembleConnectorProperties(connectorConfig, methods);
        ConnectorUtil.addConnUrlProperty(connectorWithProperties, "jdbc:sap://host:port");
        return connectorWithProperties;
    }
}
