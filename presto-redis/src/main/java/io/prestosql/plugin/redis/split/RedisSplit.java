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
package io.prestosql.plugin.redis.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RedisSplit
        implements ConnectorSplit
{
    private final String schemaName;
    private final String tableName;
    private final String keyDataFormat;
    private final String keyName;
    private final String valueDataFormat;
    private final List<HostAddress> nodes;
    private final long start;
    private final long end;

    @JsonCreator
    public RedisSplit(
            @JsonProperty("schemaName") final String schemaName,
            @JsonProperty("tableName") final String tableName,
            @JsonProperty("keyDataFormat") final String keyDataFormat,
            @JsonProperty("valueDataFormat") final String valueDataFormat,
            @JsonProperty("keyName") final String keyName,
            @JsonProperty("start") final long start,
            @JsonProperty("end") final long end,
            @JsonProperty("nodes") final List<HostAddress> nodes)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.keyDataFormat = requireNonNull(keyDataFormat, "keyDataFormat is null");
        this.valueDataFormat = requireNonNull(valueDataFormat, "valueDataFormat is null");
        this.keyName = keyName;
        this.nodes = ImmutableList.copyOf(requireNonNull(nodes, "nodes is null"));
        this.start = start;
        this.end = end;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getKeyDataFormat()
    {
        return keyDataFormat;
    }

    @JsonProperty
    public String getValueDataFormat()
    {
        return valueDataFormat;
    }

    @JsonProperty
    public String getKeyName()
    {
        return keyName;
    }

    @JsonProperty
    public List<HostAddress> getNodes()
    {
        return nodes;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getEnd()
    {
        return end;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return nodes;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("keyDataFormat", keyDataFormat)
                .add("valueDataFormat", valueDataFormat)
                .add("keyName", keyName)
                .add("start", start)
                .add("end", end)
                .add("nodes", nodes)
                .toString();
    }
}
