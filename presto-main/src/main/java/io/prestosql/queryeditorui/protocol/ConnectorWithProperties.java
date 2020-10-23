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

package io.prestosql.queryeditorui.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ConnectorWithProperties
{
    @JsonProperty
    private String connectorName;

    @JsonProperty
    private Optional<String> connectorLabel;

    @JsonProperty
    private boolean propertiesEnabled;
    @JsonProperty
    private boolean catalogConfigFilesEnabled;
    @JsonProperty
    private boolean globalConfigFilesEnabled;

    @JsonProperty
    private List<Properties> connectorProperties;

    public ConnectorWithProperties()
    {
    }

    public ConnectorWithProperties(
            @JsonProperty("connectorName") String connectorName,
            @JsonProperty("connectorLabel") Optional<String> connectorLabel,
            @JsonProperty("propertiesEnabled") boolean propertiesEnabled,
            @JsonProperty("catalogConfigFilesEnabled") boolean catalogConfigFilesEnabled,
            @JsonProperty("globalConfigFilesEnabled") boolean globalConfigFilesEnabled,
            @JsonProperty("properties") List<Properties> connectorProperties)
    {
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.connectorLabel = requireNonNull(connectorLabel, "connectorLabel is null");
        this.propertiesEnabled = propertiesEnabled;
        this.catalogConfigFilesEnabled = catalogConfigFilesEnabled;
        this.globalConfigFilesEnabled = globalConfigFilesEnabled;
        this.connectorProperties = requireNonNull(connectorProperties, "Properties is null");
    }

    @JsonProperty
    public String getConnectorName()
    {
        return connectorName;
    }

    @JsonProperty
    public Optional<String> getConnectorLabel()
    {
        if (connectorLabel != null && connectorLabel.isPresent()) {
            return connectorLabel;
        }
        return Optional.of(connectorName);
    }

    @JsonProperty
    public boolean isPropertiesEnabled()
    {
        return propertiesEnabled;
    }

    @JsonProperty
    public boolean isCatalogConfigFilesEnabled()
    {
        return catalogConfigFilesEnabled;
    }

    @JsonProperty
    public boolean isGlobalConfigFilesEnabled()
    {
        return globalConfigFilesEnabled;
    }

    @JsonProperty
    public List<Properties> getConnectorProperties()
    {
        return connectorProperties;
    }

    public static class Properties
    {
        private final String name;
        private final String value;
        private final String description;
        private final Optional<String> type;
        private final Optional<Boolean> required;
        private final Optional<Boolean> readOnly;

        @JsonCreator
        public Properties(
                @JsonProperty("name") String name,
                @JsonProperty("value") String value,
                @JsonProperty("description") String description,
                @JsonProperty("type") Optional<String> type,
                @JsonProperty("required") Optional<Boolean> required,
                @JsonProperty("readOnly") Optional<Boolean> readOnly)
        {
            this.name = name;
            this.value = value;
            this.description = description;
            this.type = type;
            this.required = required;
            this.readOnly = readOnly;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public String getValue()
        {
            return value;
        }

        @JsonProperty
        public String getDescription()
        {
            return description;
        }

        @JsonProperty
        public Optional<String> getType()
        {
            return type;
        }

        @JsonProperty
        public Optional<Boolean> getRequired()
        {
            return required;
        }

        @JsonProperty
        public Optional<Boolean> getReadOnly()
        {
            return readOnly;
        }
    }
}
