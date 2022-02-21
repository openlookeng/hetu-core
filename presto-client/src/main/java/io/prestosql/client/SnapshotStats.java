/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
public class SnapshotStats
{
    // Total CPU time taken for capturing all snapshots of query
    private final long totalCpuTimeMillis;
    // Total Wall time taken for capturing all snapshots of query
    private final long totalWallTimeMillis;
    // Total cpu time taken for capturing last successful snapshot of query
    private final long lastSnapshotCpuTimeMillis;
    // Total Wall time taken for capturing last successful snapshot of query
    private final long lastSnapshotWallTimeMillis;
    // Size of all snapshots
    private final long allSnapshotsSizeBytes;
    // Size of last successful snapshot
    private final long lastSnapshotSizeBytes;

    @JsonCreator
    public SnapshotStats(
            @JsonProperty("totalCpuTimeMillis") long totalCpuTimeMillis,
            @JsonProperty("totalWallTimeMillis") long totalWallTimeMillis,
            @JsonProperty("lastSnapshotCpuTimeMillis") long lastSnapshotCpuTimeMillis,
            @JsonProperty("lastSnapshotWallTimeMillis") long lastSnapshotWallTimeMillis,
            @JsonProperty("allSnapshotsSizeBytes") long allSnapshotsSizeBytes,
            @JsonProperty("lastSnapshotSizeBytes") long lastSnapshotSizeBytes)
    {
        this.totalCpuTimeMillis = totalCpuTimeMillis;
        this.totalWallTimeMillis = totalWallTimeMillis;
        this.lastSnapshotCpuTimeMillis = lastSnapshotCpuTimeMillis;
        this.lastSnapshotWallTimeMillis = lastSnapshotWallTimeMillis;
        this.allSnapshotsSizeBytes = allSnapshotsSizeBytes;
        this.lastSnapshotSizeBytes = lastSnapshotSizeBytes;
    }

    @JsonProperty
    public long getTotalCpuTimeMillis()
    {
        return totalCpuTimeMillis;
    }

    @JsonProperty
    public long getTotalWallTimeMillis()
    {
        return totalWallTimeMillis;
    }

    @JsonProperty
    public long getLastSnapshotCpuTimeMillis()
    {
        return lastSnapshotCpuTimeMillis;
    }

    @JsonProperty
    public long getLastSnapshotWallTimeMillis()
    {
        return lastSnapshotWallTimeMillis;
    }

    @JsonProperty
    public long getAllSnapshotsSizeBytes()
    {
        return allSnapshotsSizeBytes;
    }

    @JsonProperty
    public long getLastSnapshotSizeBytes()
    {
        return lastSnapshotSizeBytes;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("totalCpuTimeMillis", totalCpuTimeMillis)
                .add("totalWallTimeMillis", totalWallTimeMillis)
                .add("lastSnapshotCpuTimeMillis", lastSnapshotCpuTimeMillis)
                .add("lastSnapshotWallTimeMillis", lastSnapshotWallTimeMillis)
                .add("allSnapshotsSizeBytes", allSnapshotsSizeBytes)
                .add("lastSnapshotSizeBytes", lastSnapshotSizeBytes)
                .toString();
    }

    public static SnapshotStats.Builder builder()
    {
        return new SnapshotStats.Builder();
    }

    public static class Builder
    {
        private long totalCpuTimeMillis;
        private long totalWallTimeMillis;
        private long lastSnapshotCpuTimeMillis;
        private long lastSnapshotWallTimeMillis;
        private long allSnapshotsSizeBytes;
        private long lastSnapshotSizeBytes;

        private Builder() {}

        public void setTotalCpuTimeMillis(long totalCpuTimeMillis)
        {
            this.totalCpuTimeMillis = totalCpuTimeMillis;
        }

        public void setTotalWallTimeMillis(long totalWallTimeMillis)
        {
            this.totalWallTimeMillis = totalWallTimeMillis;
        }

        public void setLastSnapshotCpuTimeMillis(long lastSnapshotCpuTimeMillis)
        {
            this.lastSnapshotCpuTimeMillis = lastSnapshotCpuTimeMillis;
        }

        public void setLastSnapshotWallTimeMillis(long lastSnapshotWallTimeMillis)
        {
            this.lastSnapshotWallTimeMillis = lastSnapshotWallTimeMillis;
        }

        public void setAllSnapshotsSizeBytes(long allSnapshotsSizeBytes)
        {
            this.allSnapshotsSizeBytes = allSnapshotsSizeBytes;
        }

        public void setLastSnapshotSizeBytes(long lastSnapshotSizeBytes)
        {
            this.lastSnapshotSizeBytes = lastSnapshotSizeBytes;
        }

        public SnapshotStats build()
        {
            return new SnapshotStats(
                    totalCpuTimeMillis,
                    totalWallTimeMillis,
                    lastSnapshotCpuTimeMillis,
                    lastSnapshotWallTimeMillis,
                    allSnapshotsSizeBytes,
                    lastSnapshotSizeBytes);
        }
    }
}
