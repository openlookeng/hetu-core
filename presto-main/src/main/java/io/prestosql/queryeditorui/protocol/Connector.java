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

package io.prestosql.queryeditorui.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class Connector
{
    @JsonProperty
    private ConnectorWithProperties connectorWithProperties;

    @JsonProperty
    private UUID uuid;

    @JsonProperty
    private String docLink;

    @JsonProperty
    private String configLink;

    @JsonProperty
    public UUID getUuid()
    {
        return uuid;
    }

    @JsonProperty
    public ConnectorWithProperties getConnectorWithProperties()
    {
        return connectorWithProperties;
    }

    @JsonProperty
    public String getDocLink()
    {
        return docLink;
    }

    @JsonProperty
    public String getConfigLink()
    {
        return configLink;
    }

    public Connector()
    {
    }

    public Connector(@JsonProperty("uuid") UUID uuid,
            @JsonProperty("docLink") String docLink,
            @JsonProperty("configLink") String configLink,
            @JsonProperty("connectorWithProperties") ConnectorWithProperties connectorWithProperties)
    {
        this.connectorWithProperties = requireNonNull(connectorWithProperties, "Properties is null");
        this.uuid = requireNonNull(uuid, "uuid is null");
        this.docLink = requireNonNull(docLink, "docLink is null");
        this.configLink = requireNonNull(configLink, "configLink is null");
    }
}
