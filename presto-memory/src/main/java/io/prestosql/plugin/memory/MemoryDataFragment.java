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
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.HostAddress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public class MemoryDataFragment
{
    private static final JsonCodec<MemoryDataFragment> MEMORY_DATA_FRAGMENT_CODEC = jsonCodec(MemoryDataFragment.class);

    private final HostAddress hostAddress;
    private final long rows;
    private final int logicalPartCount;
    private final Map<String, List<Integer>> logicalPartPartitionMap;

    @JsonCreator
    public MemoryDataFragment(
            @JsonProperty("hostAddress") HostAddress hostAddress,
            @JsonProperty("rows") long rows,
            @JsonProperty("logicalPartCount") int logicalPartCount,
            @JsonProperty("logicalPartPartitionMap") Map<String, List<Integer>> logicalPartPartitionMap)
    {
        this.hostAddress = requireNonNull(hostAddress, "hostAddress is null");
        checkArgument(rows >= 0, "Rows number can not be negative");
        this.rows = rows;
        this.logicalPartCount = logicalPartCount;
        this.logicalPartPartitionMap = logicalPartPartitionMap;
    }

    @JsonProperty
    public HostAddress getHostAddress()
    {
        return hostAddress;
    }

    @JsonProperty
    public long getRows()
    {
        return rows;
    }

    @JsonProperty
    public int getLogicalPartCount()
    {
        return logicalPartCount;
    }

    @JsonProperty
    public Map<String, List<Integer>> getLogicalPartPartitionMap()
    {
        return logicalPartPartitionMap;
    }

    public Slice toSlice()
    {
        return Slices.wrappedBuffer(MEMORY_DATA_FRAGMENT_CODEC.toJsonBytes(this));
    }

    public static MemoryDataFragment fromSlice(Slice fragment)
    {
        return MEMORY_DATA_FRAGMENT_CODEC.fromJson(fragment.getBytes());
    }

    public static Map<String, List<Integer>> getMergedPartitionMap(Map<String, List<Integer>> a, Map<String, List<Integer>> b)
    {
        Map<String, List<Integer>> result = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry : a.entrySet()) {
            List<Integer> merged = new ArrayList<>(entry.getValue());
            List<Integer> bValue = b.get(entry.getKey());
            if (bValue != null) {
                merged.addAll(bValue);
            }
            result.put(entry.getKey(), merged);
        }
        for (Map.Entry<String, List<Integer>> entry : b.entrySet()) {
            if (!result.containsKey(entry.getKey())) {
                result.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }
        }
        return result;
    }

    public static MemoryDataFragment merge(MemoryDataFragment a, MemoryDataFragment b)
    {
        checkArgument(a.getHostAddress().equals(b.getHostAddress()), "Can not merge fragments from different hosts");
        if (a.getLogicalPartPartitionMap().size() == 0 && b.getLogicalPartPartitionMap().size() == 0) {
            return new MemoryDataFragment(a.getHostAddress(), a.getRows() + b.getRows(), Math.max(a.getLogicalPartCount(), b.getLogicalPartCount()), Collections.emptyMap());
        }
        else {
            return new MemoryDataFragment(a.getHostAddress(), a.getRows() + b.getRows(), Math.max(a.getLogicalPartCount(), b.getLogicalPartCount()), getMergedPartitionMap(a.getLogicalPartPartitionMap(), b.getLogicalPartPartitionMap()));
        }
    }
}
