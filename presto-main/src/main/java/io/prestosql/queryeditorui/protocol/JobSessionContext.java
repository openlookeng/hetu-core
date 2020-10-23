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
import io.prestosql.client.ClientSession;

import java.util.Map;
import java.util.Optional;

public class JobSessionContext
{
    private final String catalog;
    private final String schema;
    private final Map<String, String> properties;
    private final Optional<String> path;

    @JsonCreator
    public JobSessionContext(
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("properties") Map<String, String> properties,
            @JsonProperty("path") Optional<String> path)
    {
        this.catalog = catalog;
        this.schema = schema;
        this.properties = properties;
        this.path = path;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @JsonProperty
    public Optional<String> getPath()
    {
        return path;
    }

    public static JobSessionContext buildFromClient(ClientSession session)
    {
        return new JobSessionContext(session.getCatalog(),
                session.getSchema(),
                session.getProperties(),
                session.getPath() == null ? Optional.empty() : Optional.of(session.getPath()));
    }
}
