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
package io.prestosql.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spi.memory.MemoryPoolInfo;

import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MemoryInfo
{
    private final int availableProcessors;
    private final double processCpuLoad;
    private final double systemCpuLoad;
    private final DataSize totalNodeMemory;
    private final Map<MemoryPoolId, MemoryPoolInfo> pools;

    @JsonCreator
    public MemoryInfo(
            @JsonProperty("availableProcessors") int availableProcessors,
            @JsonProperty("processCpuLoad") double processCpuLoad,
            @JsonProperty("systemCpuLoad") double systemCpuLoad,
            @JsonProperty("totalNodeMemory") DataSize totalNodeMemory,
            @JsonProperty("pools") Map<MemoryPoolId, MemoryPoolInfo> pools)
    {
        this.totalNodeMemory = requireNonNull(totalNodeMemory, "totalNodeMemory is null");
        this.processCpuLoad = requireNonNull(processCpuLoad, "processCpuLoad is null");
        this.systemCpuLoad = requireNonNull(systemCpuLoad, "systemCpuLoad is null");
        this.pools = ImmutableMap.copyOf(requireNonNull(pools, "pools is null"));
        this.availableProcessors = availableProcessors;
    }

    @JsonProperty
    public int getAvailableProcessors()
    {
        return availableProcessors;
    }

    @JsonProperty
    public DataSize getTotalNodeMemory()
    {
        return totalNodeMemory;
    }

    @JsonProperty
    public double getProcessCpuLoad()
    {
        return processCpuLoad;
    }

    @JsonProperty
    public double getSystemCpuLoad()
    {
        return systemCpuLoad;
    }

    @JsonProperty
    public Map<MemoryPoolId, MemoryPoolInfo> getPools()
    {
        return pools;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("availableProcessors", availableProcessors)
                .add("processCpuLoad", processCpuLoad)
                .add("systemCpuLoad", systemCpuLoad)
                .add("totalNodeMemory", totalNodeMemory)
                .add("pools", pools)
                .toString();
    }
}
