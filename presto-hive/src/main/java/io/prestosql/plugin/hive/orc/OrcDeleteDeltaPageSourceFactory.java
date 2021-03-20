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
package io.prestosql.plugin.hive.orc;

import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static java.util.Objects.requireNonNull;

public class OrcDeleteDeltaPageSourceFactory
{
    private final String sessionUser;
    private final Configuration configuration;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final DataSize maxMergeDistance;
    private final DataSize maxBufferSize;
    private final DataSize streamBufferSize;
    private final DataSize maxReadBlockSize;
    private final DataSize tinyStripeThreshold;
    private final boolean lazyReadSmallRanges;
    private final boolean orcBloomFiltersEnabled;

    public OrcDeleteDeltaPageSourceFactory(
            String sessionUser,
            Configuration configuration,
            HdfsEnvironment hdfsEnvironment,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            DataSize streamBufferSize,
            DataSize maxReadBlockSize,
            DataSize tinyStripeThreshold,
            boolean lazyReadSmallRanges,
            boolean orcBloomFiltersEnabled,
            FileFormatDataSourceStats stats)
    {
        this.sessionUser = requireNonNull(sessionUser, "sessionUser is null");
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
        this.streamBufferSize = requireNonNull(streamBufferSize, "streamBufferSize is null");
        this.maxReadBlockSize = requireNonNull(maxReadBlockSize, "maxReadBlockSize is null");
        this.tinyStripeThreshold = requireNonNull(tinyStripeThreshold, "tinyStripeThreshold is null");
        this.lazyReadSmallRanges = requireNonNull(lazyReadSmallRanges, "lazyReadSmallRanges is null");
        this.orcBloomFiltersEnabled = requireNonNull(orcBloomFiltersEnabled, "orcBloomFiltersEnabled is null");
    }

    public OrcDeleteDeltaPageSource createPageSource(Path path, long fileSize, long lastModifiedTime)
    {
        return new OrcDeleteDeltaPageSource(
                path,
                fileSize,
                sessionUser,
                configuration,
                hdfsEnvironment,
                maxMergeDistance,
                maxBufferSize,
                streamBufferSize,
                maxReadBlockSize,
                tinyStripeThreshold,
                lazyReadSmallRanges,
                orcBloomFiltersEnabled,
                stats,
                lastModifiedTime);
    }
}
