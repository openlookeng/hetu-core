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
package io.prestosql.queryeditorui.output;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.net.URI;

@JsonTypeName("csv")
public class CSVPersistentOutput
        implements PersistentJobOutput
{
    private URI location;
    private final String type;
    private final String description;

    @JsonCreator
    public CSVPersistentOutput(
            @JsonProperty("location") URI location,
            @JsonProperty("type") String type,
            @JsonProperty("description") String description)
    {
        this.location = location;
        this.type = type;
        this.description = description;
    }

    @Override
    public String processQuery(String query)
    {
        return query;
    }

    @JsonProperty
    @Override
    public URI getLocation()
    {
        return location;
    }

    @Override
    public void setLocation(URI location)
    {
        this.location = location;
    }

    @JsonProperty
    @Override
    public String getType()
    {
        return type;
    }

    @Override
    public String getDescription()
    {
        return description;
    }
}
