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

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.queryeditorui.protocol.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ConnectorCache
{
    private static final Logger LOG = LoggerFactory.getLogger(ConnectorCache.class);

    static final JsonCodec<List<Connector>> CONNECTORS_CODEC =
            new JsonCodecFactory(new ObjectMapperProvider()).listJsonCodec(Connector.class);
    private List<Connector> connectorsList = new ArrayList<>();

    public ConnectorCache(File connectorsJsonPath) throws IOException
    {
        loadConnectorsAndProperties(connectorsJsonPath);
    }

    public List<Connector> getConnectors(String user)
    {
        return connectorsList.stream()
                .filter(q -> q.getUser().equals(user))
                .collect(Collectors.toList());
    }

    private synchronized void loadConnectorsAndProperties(File connectorsJsonPath) throws IOException
    {
        if (!connectorsJsonPath.exists()) {
            return;
        }
        try (FileInputStream fin = new FileInputStream(connectorsJsonPath)) {
            StringBuilder jsonString = new StringBuilder();
            byte[] buffer = new byte[4096];
            int n;
            while ((n = fin.read(buffer)) > 0) {
                jsonString.append(new String(buffer, 0, n));
            }
            connectorsList.addAll(CONNECTORS_CODEC.fromJson(jsonString.toString()));
        }
        catch (IOException e) {
            LOG.error("Connectors properties file not found", e);
            throw e;
        }
    }
}
