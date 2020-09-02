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
package io.prestosql.vacuum;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class AutoVacuumConfig
{
    private boolean autoVacuumEnabled;
    private Duration vacuumScanInterval = new Duration(10, TimeUnit.MINUTES);
    private int vacuumScanThreads = 3;

    @Config("auto-vacuum.enabled")
    @ConfigDescription("Whether to enable auto vacuum.")
    public AutoVacuumConfig setAutoVacuumEnabled(boolean autoVacuumEnabled)
    {
        this.autoVacuumEnabled = autoVacuumEnabled;
        return this;
    }

    public boolean isAutoVacuumEnabled()
    {
        return this.autoVacuumEnabled;
    }

    @Config("auto-vacuum.scan.interval")
    @ConfigDescription("Interval of auto compaction scan, default value is 10m.")
    public AutoVacuumConfig setVacuumScanInterval(Duration vacuumScanInterval)
    {
        this.vacuumScanInterval = vacuumScanInterval;
        return this;
    }

    @NotNull
    public @MinDuration("15s") @MaxDuration("24h") Duration getVacuumScanInterval()
    {
        return this.vacuumScanInterval;
    }

    @Config("auto-vacuum.scan.threads")
    @ConfigDescription("number of threads to scan, default number is 3.")
    public AutoVacuumConfig setVacuumScanThreads(int vacuumScanThreads)
    {
        this.vacuumScanThreads = vacuumScanThreads;
        return this;
    }

    @Min(1)
    @Max(16)
    public int getVacuumScanThreads()
    {
        return this.vacuumScanThreads;
    }
}
