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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.prestosql.execution.Column;
import io.prestosql.execution.Input;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class Table
{
    private static final Splitter TABLE_PART_SPLITTER = Splitter.on(".").omitEmptyStrings().trimResults();
    private static final Joiner TABLE_PART_JOINER = Joiner.on(".").skipNulls();

    private final String connectorId;
    private final String schema;
    private final String table;
    private final ImmutableList<String> columns;

    @JsonCreator
    protected Table(@JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("columns") List<String> columns)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    public Table(String connectorId,
            String schema,
            String table)
    {
        this(connectorId, schema, table, Collections.<String>emptyList());
    }

    public static Table valueOf(String s)
    {
        List<String> parts = TABLE_PART_SPLITTER.splitToList(s);

        if (parts.size() == 3) {
            return new Table(parts.get(0), parts.get(1), parts.get(2));
        }
        else if (parts.size() == 2) {
            return new Table("hive", parts.get(0), parts.get(1));
        }
        else if (parts.size() == 1) {
            return new Table("hive", "default", parts.get(0));
        }
        else if (parts.size() > 3) {
            StringBuilder catalogBuilder = new StringBuilder();
            for (int i = 0; i < parts.size() - 2; i++) {
                catalogBuilder.append(parts.get(i));
                if (i + 1 < parts.size() - 2) {
                    catalogBuilder.append(".");
                }
            }
            return new Table(catalogBuilder.toString(), parts.get(parts.size() - 2), parts.get(parts.size() - 1));
        }
        else {
            throw new IllegalArgumentException("Table identifier parts not found.");
        }
    }

    public static Table fromInput(Input input)
    {
        List<String> columns = new ArrayList<>(input.getColumns().size());

        for (Column c : input.getColumns()) {
            columns.add(c.getName());
        }

        return new Table(input.getCatalogName().getCatalogName(), input.getSchema(), input.getTable(), columns);
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty("fqn")
    public String getFqn()
    {
        return TABLE_PART_JOINER.join(getConnectorId(), getSchema(), getTable());
    }

    @JsonProperty
    public ImmutableList<String> getColumns()
    {
        return columns;
    }

    @Override
    public int hashCode()
    {
        int result = getConnectorId().hashCode();
        result = 31 * result + getSchema().hashCode();
        result = 31 * result + getTable().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (obj instanceof Input) {
            Input other = (Input) obj;

            return getConnectorId().equals(other.getCatalogName().getCatalogName()) &&
                    getSchema().equals(other.getSchema()) &&
                    getTable().equals(other.getTable());
        }
        else if (obj instanceof Table) {
            Table other = (Table) obj;

            return getConnectorId().equals(other.getConnectorId()) &&
                    getSchema().equals(other.getSchema()) &&
                    getTable().equals(other.getTable());
        }

        return false;
    }
}
