/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.queryeditorui.store.connectors;

import io.prestosql.queryeditorui.protocol.Connector;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static java.util.Collections.unmodifiableList;

public class ConnectorCache
{
    private ConnectorCache() {}

    private static final List<Connector> CONNECTORS_CACHE = new ArrayList<>();

    public static void addCatalogConfig(Plugin plugin, String connectorName)
    {
        Optional<ConnectorWithProperties> connectorWithProperties = plugin.getConnectorWithProperties();
        if (connectorWithProperties.isPresent() && connectorName != null) {
            connectorWithProperties.get().setConnectorName(connectorName);
            Connector connector = new Connector(
                    UUID.randomUUID(),
                    connectorWithProperties.get().getDocLink(),
                    connectorWithProperties.get().getConfigLink(),
                    connectorWithProperties.get());
            CONNECTORS_CACHE.add(connector);
        }
    }

    public static List<Connector> getConnectors()
    {
        return unmodifiableList(CONNECTORS_CACHE);
    }
}
