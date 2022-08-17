/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.resourcemanager;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class BasicResourceStats
{
    public Duration cpuTime;
    public DataSize memCurrent;
    public DataSize revocableMem;
    public DataSize ioCurrent;

    public BasicResourceStats()
    {
        this(DataSize.succinctBytes(0),
                Duration.succinctDuration(0, TimeUnit.NANOSECONDS),
                DataSize.succinctBytes(0), DataSize.succinctBytes(0));
    }

    public BasicResourceStats(DataSize memCurrent, Duration cpuTime, DataSize ioCurrent, DataSize revocableMem)
    {
        this.memCurrent = memCurrent;
        this.cpuTime = cpuTime;
        this.ioCurrent = ioCurrent;
        this.revocableMem = revocableMem;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BasicResourceStats stats = (BasicResourceStats) o;

        return this.cpuTime.equals(stats.cpuTime)
                && this.ioCurrent.equals(stats.ioCurrent)
                && this.memCurrent.equals(stats.memCurrent)
                && this.revocableMem.equals(stats.revocableMem);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cpuTime, ioCurrent, memCurrent, revocableMem);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("CpuTime: ").append(this.cpuTime).append(", ")
                .append("Memory: ").append(this.memCurrent).append(", ")
                .append("RevokabkeMemory: ").append(this.revocableMem).append(", ")
                .append("Network: ").append(this.ioCurrent).append("}");
        return builder.toString();
    }
}
