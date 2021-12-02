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
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import io.hetu.core.common.util.SecurePathWhiteList;
import io.prestosql.spi.function.Mandatory;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class MemoryConfig
{
    private DataSize maxDataPerNode = new DataSize(256, DataSize.Unit.MEGABYTE);
    private DataSize maxLogicalPartSize = new DataSize(256, MEGABYTE);
    private DataSize maxPageSize = new DataSize(512, DataSize.Unit.KILOBYTE);
    private Duration processingDelay = new Duration(5, TimeUnit.SECONDS);
    private Path spillRoot;
    private int threadPoolSize = Math.max((Runtime.getRuntime().availableProcessors() / 2), 1);
    private boolean tableStatisticsEnabled; //default value is false unless specified in memory config file

    @NotNull
    public Path getSpillRoot()
    {
        // if spillRoot is null, let the config manager throw the error, it's more user friendly
        if (spillRoot != null) {
            try {
                checkArgument(!spillRoot.toString().contains("../"),
                        "memory.spill-path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
                checkArgument(SecurePathWhiteList.isSecurePath(spillRoot.toString()),
                        "memory.spill-path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            }
            catch (IOException e) {
                throw new IllegalArgumentException("Failed to get secure path list.", e);
            }
        }
        return spillRoot;
    }

    @Mandatory(name = "memory.spill-path",
            description = "Specify the directory where memory data will get spilled to",
            required = true)
    @Config("memory.spill-path")
    public MemoryConfig setSpillRoot(String spillRoot)
    {
        this.spillRoot = Paths.get(spillRoot);
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
        if (maxDataPerNode.toBytes() > Runtime.getRuntime().maxMemory()) {
            throw new IllegalArgumentException("Invalid config for max data per node. Can not exceed heap headroom: " + Runtime.getRuntime().maxMemory());
        }
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

    @Config("memory.table-statistics-enabled")
    @ConfigDescription("Enable use of table statistics.") // currently, the table statistics are collected by running analyze statement on tables
    public MemoryConfig setTableStatisticsEnabled(boolean tableStatisticsEnabled)
    {
        this.tableStatisticsEnabled = tableStatisticsEnabled;
        return this;
    }

    public boolean isTableStatisticsEnabled()
    {
        return tableStatisticsEnabled;
    }

    @MinDataSize("1kB")
    @MaxDataSize("10MB")
    public DataSize getMaxPageSize()
    {
        return maxPageSize;
    }

    @Config("memory.max-page-size")
    @ConfigDescription("Max size of pages stored in Memory Connector (default: 1MB)")
    public MemoryConfig setMaxPageSize(DataSize maxPageSize)
    {
        this.maxPageSize = maxPageSize;
        return this;
    }

    @MinDuration("1s")
    @MaxDuration("10s")
    public Duration getProcessingDelay()
    {
        return processingDelay;
    }

    @Config("memory.logical-part-processing-delay")
    @ConfigDescription("The delay between when table is created/updated and logical part processing starts (default: 5s)")
    public MemoryConfig setProcessingDelay(Duration processingDelay)
    {
        this.processingDelay = processingDelay;
        return this;
    }

    @Min(1)
    public int getThreadPoolSize()
    {
        return threadPoolSize;
    }

    @Config("memory.thread-pool-size")
    @ConfigDescription("Maximum threads to allocate for background processing, e.g. sorting, cleanup, etc (default: half of threads available to the JVM)")
    public MemoryConfig setThreadPoolSize(int threadPoolSize)
    {
        this.threadPoolSize = threadPoolSize;
        return this;
    }
}
