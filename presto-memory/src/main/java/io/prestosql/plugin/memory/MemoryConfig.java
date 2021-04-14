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

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.prestosql.spi.function.Mandatory;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class MemoryConfig
{
    private int splitsPerNode = Runtime.getRuntime().availableProcessors();
    private DataSize maxDataPerNode = new DataSize(128, DataSize.Unit.MEGABYTE);
    private DataSize maxLogicalPartSize = new DataSize(256, MEGABYTE);
    private int processingThreads = Runtime.getRuntime().availableProcessors();

    @NotNull
    public int getSplitsPerNode()
    {
        return splitsPerNode;
    }

    @Config("memory.splits-per-node")
    public MemoryConfig setSplitsPerNode(int splitsPerNode)
    {
        this.splitsPerNode = splitsPerNode;
        return this;
    }

    @NotNull
    public DataSize getMaxDataPerNode()
    {
        return maxDataPerNode;
    }

    @Mandatory(name = "memory.max-data-per-node",
            description = "Define memory limit for pages stored in this connector per each node",
            defaultValue = "128MB",
            required = true)
    @Config("memory.max-data-per-node")
    public MemoryConfig setMaxDataPerNode(DataSize maxDataPerNode)
    {
        this.maxDataPerNode = maxDataPerNode;
        return this;
    }

    @MinDataSize("32MB")
    @MaxDataSize("1GB")
    public DataSize getMaxLogicalPartSize()
    {
        return maxLogicalPartSize;
    }

    @Config("memory.max-logical-part-size")
    public MemoryConfig setMaxLogicalPartSize(DataSize maxLogicalPartSize)
    {
        this.maxLogicalPartSize = maxLogicalPartSize;
        return this;
    }
}
