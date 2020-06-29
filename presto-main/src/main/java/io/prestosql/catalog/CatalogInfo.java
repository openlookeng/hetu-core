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
    private String submitter;
    private String version;
    private long createdTime;

    @JsonCreator
    public CatalogInfo(
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("connectorName") String connectorName,
            @JsonProperty("submitter") String submitter,
            @JsonProperty("createdTime") long createdTime,
            @JsonProperty("version") String version,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.submitter = submitter;
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

    @JsonProperty
    public String getSubmitter()
    {
        return submitter;
    }

    public void setSubmitter(String submitter)
    {
        this.submitter = submitter;
    }

    @JsonProperty
    public long getCreatedTime()
    {
        return createdTime;
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    public void setVersion(String version)
    {
        this.version = version;
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
                connectorName.equals(that.connectorName) &&
                submitter.equals(that.submitter);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(properties, catalogName, connectorName, submitter, createdTime, version);
    }
}
