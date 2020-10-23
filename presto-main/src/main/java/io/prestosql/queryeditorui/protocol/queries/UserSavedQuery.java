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
package io.prestosql.queryeditorui.protocol.queries;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import java.util.UUID;

public class UserSavedQuery
        implements SavedQuery
{
    @JsonProperty
    private QueryWithPlaceholders queryWithPlaceholders;

    @JsonProperty
    private String user;

    @JsonProperty
    private String name;

    @JsonProperty
    private String description;

    private DateTime createdAt;

    @JsonProperty
    private UUID uuid;

    @JsonProperty
    private boolean featured;

    public UserSavedQuery()
    {
    }

    public UserSavedQuery(@JsonProperty("queryWithPlaceholders") QueryWithPlaceholders queryWithPlaceholders,
                          @JsonProperty("user") String user,
                          @JsonProperty("name") String name,
                          @JsonProperty("description") String description,
                          @JsonProperty("createdAt") DateTime createdAt,
                          @JsonProperty("uuid") UUID uuid,
                          @JsonProperty("featured") boolean featured)
    {
        this.queryWithPlaceholders = queryWithPlaceholders;
        this.user = user;
        this.name = name;
        this.description = description;
        this.createdAt = createdAt;
        this.uuid = uuid;
        this.featured = featured;
    }

    @JsonProperty
    @Override
    public QueryWithPlaceholders getQueryWithPlaceholders()
    {
        return queryWithPlaceholders;
    }

    @JsonProperty
    @Override
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    @Override
    public String getName()
    {
        return name;
    }

    @JsonProperty
    @Override
    public String getDescription()
    {
        return description;
    }

    @JsonProperty
    @Override
    public UUID getUuid()
    {
        return uuid;
    }

    public void setFeatured(boolean featured)
    {
        this.featured = featured;
    }

    @JsonProperty
    public boolean isFeatured()
    {
        return featured;
    }

    @JsonProperty
    public String getCreatedAt()
    {
        if (createdAt != null) {
            return createdAt.toDateTimeISO().toString();
        }
        else {
            return null;
        }
    }
}
