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
package io.prestosql.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class JdbcSplit
        implements ConnectorSplit
{
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final String splitField;
    private final String rangeStart;
    private final String rangEnd;
    private final long timeStamp;
    private final int scanNodes;
    private final Optional<String> additionalPredicate;

    @JsonCreator
    public JdbcSplit(
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("splitField") String splitField,
            @JsonProperty("beginIndex") String rangeStart,
            @JsonProperty("endIndex") String rangEnd,
            @JsonProperty("timeStamp") long timeStamp,
            @JsonProperty("scanNodes") int scanNodes,
            @JsonProperty("additionalPredicate") Optional<String> additionalPredicate)
    {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "table name is null");
        this.rangeStart = rangeStart;
        this.rangEnd = rangEnd;
        this.splitField = splitField;
        this.timeStamp = timeStamp;
        this.scanNodes = scanNodes;
        this.additionalPredicate = requireNonNull(additionalPredicate, "additionalPredicate is null");
    }

    @JsonProperty
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @Nullable
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
    public String getRangeStart()
    {
        return rangeStart;
    }

    @JsonProperty
    public String getRangEnd()
    {
        return rangEnd;
    }

    @JsonProperty
    public long getTimeStamp()
    {
        return timeStamp;
    }

    @JsonProperty
    @Override
    public int getSplitCount()
    {
        return scanNodes;
    }

    @JsonProperty
    public Optional<String> getAdditionalPredicate()
    {
        return additionalPredicate;
    }

    @JsonProperty
    public String getSplitField()
    {
        return splitField;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
