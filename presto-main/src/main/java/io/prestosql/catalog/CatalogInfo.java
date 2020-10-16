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
package io.prestosql.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

@Immutable
public class CatalogInfo
{
    Map<String, String> properties;
    private String catalogName; // data source instance name
    private String connectorName; // data source type,for example:mysql,oracle
    private String securityKey;  // the key to decrypt the encrypted property
    private String version;
    private long createdTime;

    @JsonCreator
    public CatalogInfo(
            String catalogName,
            String connectorName,
            String securityKey,
            long createdTime,
            String version,
            Map<String, String> properties)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.securityKey = securityKey;
        this.createdTime = createdTime;
        this.version = version == null || version.isEmpty() ? UUID.randomUUID().toString() : version;
        this.properties = properties == null ? ImmutableMap.of() : properties;
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    public void setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
    }

    @JsonProperty
    public String getConnectorName()
    {
        return connectorName;
    }

    public long getCreatedTime()
    {
        return createdTime;
    }

    public String getVersion()
    {
        return version;
    }

    public void setVersion(String version)
    {
        this.version = version;
    }

    @JsonProperty
    public String getSecurityKey()
    {
        return securityKey;
    }

    public void setSecurityKey(String securityKey)
    {
        this.securityKey = securityKey;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    public void setProperties(Map<String, String> properties)
    {
        this.properties = properties;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CatalogInfo that = (CatalogInfo) o;
        return createdTime == that.createdTime &&
                version == that.version &&
                properties.equals(that.properties) &&
                catalogName.equals(that.catalogName) &&
                connectorName.equals(that.connectorName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(properties, catalogName, connectorName, createdTime, version);
    }
}
