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
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface SavedQuery
{
    Pattern PLACEHOLDER_PATTERN = Pattern.compile("(\\[\\[placeholder:([\\w-]+)\\]\\])",
            Pattern.CASE_INSENSITIVE);

    String getUser();

    String getName();

    String getDescription();

    UUID getUuid();

    QueryWithPlaceholders getQueryWithPlaceholders();

    class Position
    {
        @JsonProperty
        private int row;
        @JsonProperty
        private int column;

        public Position(@JsonProperty("row") int row,
                        @JsonProperty("column") int column)
        {
            this.row = row;
            this.column = column;
        }

        @JsonProperty
        public int getRow()
        {
            return row;
        }

        @JsonProperty
        public int getColumn()
        {
            return column;
        }
    }

    class QueryPlaceholder
    {
        @JsonProperty
        private int length;
        @JsonProperty
        private Position position;
        @JsonProperty
        private String name;

        public QueryPlaceholder(@JsonProperty("length") int length,
                                @JsonProperty("position") Position position,
                                @JsonProperty("name") String name)
        {
            this.length = length;
            this.position = position;
            this.name = name;
        }

        @JsonProperty
        public int getLength()
        {
            return length;
        }

        @JsonProperty
        public Position getPosition()
        {
            return position;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }
    }

    class QueryWithPlaceholders
    {
        @JsonProperty
        private String query;
        @JsonProperty
        private List<QueryPlaceholder> placeholders;

        public QueryWithPlaceholders()
        {
        }

        public QueryWithPlaceholders(@JsonProperty("query") String query,
                                     @JsonProperty("placeholders") List<QueryPlaceholder> placeholders)
        {
            this.query = query;
            this.placeholders = placeholders;
        }

        @JsonProperty
        public String getQuery()
        {
            return query;
        }

        @JsonProperty
        public List<QueryPlaceholder> getPlaceholders()
        {
            return placeholders;
        }

        public static QueryWithPlaceholders fromQuery(String query)
        {
            ImmutableList.Builder<QueryPlaceholder> builder = ImmutableList.builder();
            Matcher matcher = PLACEHOLDER_PATTERN.matcher(query);
            String[] queryLines = query.split("\\r?\\n");

            while (matcher.find()) {
                int start = matcher.start();
                int end = matcher.end();
                int line = 0;

                for (int i = 0; i < queryLines.length; i++) {
                    if (queryLines[i].contains(matcher.group(1))) {
                        line = i;
                        break;
                    }
                }

                builder.add(new QueryPlaceholder((end - start),
                        new Position(line, start),
                        matcher.group(2)));
            }

            return new QueryWithPlaceholders(query, builder.build());
        }
    }
}
