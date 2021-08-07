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
package io.prestosql.plugin.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MemorySplit
        implements ConnectorSplit
{
    private final long table;
    private final int logicalPartNum; // the order of the logicalPart (equivalent to a split) in each node, starting from 0 (1 split, 0 is the available index)
    private final HostAddress address;
    private final long expectedRows;
    private final OptionalLong limit;

    @JsonCreator
    public MemorySplit(
            @JsonProperty("table") long table,
            @JsonProperty("logicalPartNum") int logicalPartNum,
            @JsonProperty("address") HostAddress address,
            @JsonProperty("expectedRows") long expectedRows,
            @JsonProperty("limit") OptionalLong limit)
    {
        checkState(logicalPartNum > 0, "logicalPartNum be > 0");

        this.table = requireNonNull(table, "table is null");
        this.logicalPartNum = logicalPartNum;
        this.address = requireNonNull(address, "address is null");
        this.expectedRows = expectedRows;
        this.limit = limit;
    }

    @JsonProperty
    public long getTable()
    {
        return table;
    }

    @JsonProperty
    public int getLogicalPartNum()
    {
        return logicalPartNum;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(address);
    }

    @JsonProperty
    public long getExpectedRows()
    {
        return expectedRows;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MemorySplit other = (MemorySplit) obj;
        return Objects.equals(this.table, other.table) &&
                Objects.equals(this.logicalPartNum, other.logicalPartNum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, logicalPartNum);
    }

    @Override
    public String toString()
    {
        return "MemorySplit{" +
                "table=" + table +
                ", logicalPart number=" + logicalPartNum +
                '}';
    }
}
