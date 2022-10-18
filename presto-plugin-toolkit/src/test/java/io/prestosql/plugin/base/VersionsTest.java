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
package io.prestosql.plugin.base;

import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import org.testng.annotations.Test;

import java.util.Map;

public class VersionsTest
{
    @Test
    public void testCheckSpiVersion()
    {
        // Setup
        final ConnectorContext context = new ConnectorContext()
        {
            @Override
            public NodeManager getNodeManager()
            {
                return null;
            }
        };
        final ConnectorFactory connectorFactory = new ConnectorFactory()
        {
            @Override
            public String getName()
            {
                return null;
            }

            @Override
            public ConnectorHandleResolver getHandleResolver()
            {
                return null;
            }

            @Override
            public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
            {
                return null;
            }
        };

        // Run the test
        Versions.checkSpiVersion(context, connectorFactory);

        // Verify the results
    }
}
