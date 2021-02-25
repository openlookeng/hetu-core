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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.prestosql.spi.function.Mandatory;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class JdbcMetadataConfig
{
    private boolean allowDropTable;
    // added by Hetu for metadata caching
    private Duration metadataCacheTtl = new Duration(1, TimeUnit.SECONDS); // metadata cache eviction time
    private long metadataCacheMaximumSize = 10000; // metadata cache max size
    private boolean metadataCacheEnabled = true; // enable metadata caching for Jdbc based connectors

    public boolean isAllowDropTable()
    {
        return allowDropTable;
    }

    @Mandatory(name = "allow-drop-table",
            description = "Allow connector to drop tables",
            defaultValue = "true")
    @Config("allow-drop-table")
    @ConfigDescription("Allow connector to drop tables")
    public JdbcMetadataConfig setAllowDropTable(boolean allowDropTable)
    {
        this.allowDropTable = allowDropTable;
        return this;
    }

    // added by Hetu for metadata caching
    public boolean isMetadataCacheEnabled()
    {
        return metadataCacheEnabled;
    }

    @Config("metadata-cache-enabled")
    @ConfigDescription("Enable metadata caching for Jdbc based connectors")
    public JdbcMetadataConfig setMetadataCacheEnabled(boolean metadataCacheEnabled)
    {
        this.metadataCacheEnabled = metadataCacheEnabled;
        return this;
    }

    @NotNull
    public @MinDuration("0ms") Duration getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    @Config("metadata-cache-ttl")
    @ConfigDescription("Set the metadata cache eviction time for Jdbc based connectors")
    public JdbcMetadataConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }

    @Min(1)
    public long getMetadataCacheMaximumSize()
    {
        return metadataCacheMaximumSize;
    }

    @Config("metadata-cache-maximum-size")
    @ConfigDescription("Set the metadata cache max size for Jdbc based connectors")
    public JdbcMetadataConfig setMetadataCacheMaximumSize(long metadataCacheMaximumSize)
    {
        this.metadataCacheMaximumSize = metadataCacheMaximumSize;
        return this;
    }
}
