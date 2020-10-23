/*
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

public class ExecutionRequest
{
    @JsonProperty
    private final String query;

    @JsonProperty
    private final JobSessionContext sessionContext;

    @JsonProperty
    private final String defaultConnector;

    @JsonProperty
    private final String defaultSchema;

    @JsonCreator
    public ExecutionRequest(
            @JsonProperty("query") final String query,
            @JsonProperty("defaultConnector") final String defaultConnector,
            @JsonProperty("defaultSchema") final String defaultSchema,
            @JsonProperty("sessionContext") final JobSessionContext sessionContext)
    {
        this.query = query;
        this.sessionContext = sessionContext;
        this.defaultConnector = (sessionContext == null || sessionContext.getCatalog() == null) ?
                (defaultConnector == null ? "hive" : defaultConnector) : sessionContext.getCatalog();
        this.defaultSchema = (sessionContext == null || sessionContext.getSchema() == null) ?
                (defaultConnector == null ? "default" : defaultConnector) : sessionContext.getSchema();
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public String getDefaultConnector()
    {
        return defaultConnector;
    }

    @JsonProperty
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @JsonProperty
    public JobSessionContext getSessionContext()
    {
        return sessionContext;
    }
}
