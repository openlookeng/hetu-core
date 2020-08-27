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

import static java.util.Objects.requireNonNull;

public class CreateSavedQueryBuilder
{
    @JsonProperty
    private String description;
    @JsonProperty
    private String query;
    @JsonProperty
    private String user;
    @JsonProperty
    private String name;
    private final DateTime createdAt = new DateTime();
    private final boolean featured;

    private CreateSavedQueryBuilder(String user,
                                    String query,
                                    String name,
                                    String description,
                                    boolean featured)
    {
        this.user = user;
        this.query = query;
        this.name = name;
        this.description = description;
        this.featured = featured;
    }

    public static CreateSavedQueryBuilder featured()
    {
        return new CreateSavedQueryBuilder(null, null, null, null, true);
    }

    public static CreateSavedQueryBuilder notFeatured()
    {
        return new CreateSavedQueryBuilder(null, null, null, null, false);
    }

    public CreateSavedQueryBuilder user(String user)
    {
        this.user = requireNonNull(user, "User can not be null");
        return this;
    }

    public CreateSavedQueryBuilder query(String query)
    {
        this.query = requireNonNull(query, "Query can not be null");
        return this;
    }

    public CreateSavedQueryBuilder name(String name)
    {
        this.name = requireNonNull(name, "Name can not be null");
        return this;
    }

    public CreateSavedQueryBuilder description(String description)
    {
        this.description = requireNonNull(description, "Description can not be null");
        return this;
    }

    public SavedQuery build()
    {
        requireNonNull(user, "User can not be null");
        requireNonNull(query, "Query can not be null");
        requireNonNull(name, "Name can not be null");
        requireNonNull(description, "Description can not be null");

        final SavedQuery.QueryWithPlaceholders queryWithPlaceholders =
                requireNonNull(SavedQuery.QueryWithPlaceholders.fromQuery(query),
                        "Generated query can not be null");

        return new UserSavedQuery(queryWithPlaceholders,
                user,
                name,
                description,
                createdAt,
                UUID.randomUUID(),
                featured);
    }
}
