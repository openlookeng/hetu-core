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
package io.prestosql.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
public class SnapshotStats
{
    // id of last successful snapshot
    private final long lastCaptureSnapshotId;
    // Total CPU time (ms) taken for capturing all snapshots of query
    private final long totalCaptureCpuTime;
    // Total Wall time (ms) taken for capturing all snapshots of query
    private final long totalCaptureWallTime;
    // Cpu time (ms) taken for capturing last successful snapshot of query
    private final long lastCaptureCpuTime;
    // Wall time (ms) taken for capturing last successful snapshot of query
    private final long lastCaptureWallTime;
    // Size of all snapshots (bytes)
    private final long allCaptureSize;
    // Size (bytes) of last successful snapshot
    private final long lastCaptureSize;
    // List of snapshots from which restore happened
    private final List<Long> restoredSnapshotList;
    // List of successfully captured snapshots
    private final List<Long> capturedSnapshotList;
    // Number of successful restores in current query
    private final long successRestoreCount;
    // Total Wall time (ms) for all restores happened during query
    private final long totalRestoreWallTime;
    // Total restored size during restore
    private final long totalRestoreSize;
    // Total Cpu time (ms) for loading state during restore
    private final long totalRestoreCpuTime;
    // Snapshot Id from which restore in progress
    private final long restoringSnapshotId;
    // Snapshot Ids from which capture in progress (Multiple captures possible at a time)
    private final List<Long> capturingSnapshotIds;

    @JsonCreator
    public SnapshotStats(
            @JsonProperty("lastCaptureSnapshotId") long lastCaptureSnapshotId,
            @JsonProperty("totalCaptureCpuTime") long totalCaptureCpuTime,
            @JsonProperty("totalCaptureWallTime") long totalCaptureWallTime,
            @JsonProperty("lastCaptureCpuTime") long lastCaptureCpuTime,
            @JsonProperty("lastCaptureWallTime") long lastCaptureWallTime,
            @JsonProperty("allCaptureSize") long allCaptureSize,
            @JsonProperty("lastCaptureSize") long lastCaptureSize,
            @JsonProperty("restoredSnapshotList") List<Long> restoredSnapshotList,
            @JsonProperty("capturedSnapshotList") List<Long> capturedSnapshotList,
            @JsonProperty("successRestoreCount") long successRestoreCount,
            @JsonProperty("totalRestoreWallTime") long totalRestoreWallTime,
            @JsonProperty("totalRestoreSize") long totalRestoreSize,
            @JsonProperty("totalRestoreCpuTime") long totalRestoreCpuTime,
            @JsonProperty("restoringSnapshotId") long restoringSnapshotId,
            @JsonProperty("capturingSnapshotIds") List<Long> capturingSnapshotIds)
    {
        this.lastCaptureSnapshotId = lastCaptureSnapshotId;
        this.totalCaptureCpuTime = totalCaptureCpuTime;
        this.totalCaptureWallTime = totalCaptureWallTime;
        this.lastCaptureCpuTime = lastCaptureCpuTime;
        this.lastCaptureWallTime = lastCaptureWallTime;
        this.allCaptureSize = allCaptureSize;
        this.lastCaptureSize = lastCaptureSize;
        this.restoredSnapshotList = restoredSnapshotList;
        this.capturedSnapshotList = capturedSnapshotList;
        this.successRestoreCount = successRestoreCount;
        this.totalRestoreWallTime = totalRestoreWallTime;
        this.totalRestoreSize = totalRestoreSize;
        this.totalRestoreCpuTime = totalRestoreCpuTime;
        this.restoringSnapshotId = restoringSnapshotId;
        this.capturingSnapshotIds = capturingSnapshotIds;
    }

    @JsonProperty
    public long getLastCaptureSnapshotId()
    {
        return lastCaptureSnapshotId;
    }

    @JsonProperty
    public long getTotalCaptureCpuTime()
    {
        return totalCaptureCpuTime;
    }

    @JsonProperty
    public long getTotalCaptureWallTime()
    {
        return totalCaptureWallTime;
    }

    @JsonProperty
    public long getLastCaptureCpuTime()
    {
        return lastCaptureCpuTime;
    }

    @JsonProperty
    public long getLastCaptureWallTime()
    {
        return lastCaptureWallTime;
    }

    @JsonProperty
    public long getAllCaptureSize()
    {
        return allCaptureSize;
    }

    @JsonProperty
    public long getLastCaptureSize()
    {
        return lastCaptureSize;
    }

    @JsonProperty
    public List<Long> getRestoredSnapshotList()
    {
        return restoredSnapshotList;
    }

    @JsonProperty
    public List<Long> getCapturedSnapshotList()
    {
        return capturedSnapshotList;
    }

    @JsonProperty
    public List<Long> getCapturingSnapshotIds()
    {
        return capturingSnapshotIds;
    }

    @JsonProperty
    public long getRestoringSnapshotId()
    {
        return restoringSnapshotId;
    }

    @JsonProperty
    public long getSuccessRestoreCount()
    {
        return successRestoreCount;
    }

    @JsonProperty
    public long getTotalRestoreWallTime()
    {
        return totalRestoreWallTime;
    }

    @JsonProperty
    public long getTotalRestoreSize()
    {
        return totalRestoreSize;
    }

    @JsonProperty
    public long getTotalRestoreCpuTime()
    {
        return totalRestoreCpuTime;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("lastCaptureSnapshotId", lastCaptureSnapshotId)
                .add("totalCaptureCpuTime", totalCaptureCpuTime)
                .add("totalCaptureWallTime", totalCaptureWallTime)
                .add("lastCaptureCpuTime", lastCaptureCpuTime)
                .add("lastCaptureWallTime", lastCaptureWallTime)
                .add("allCaptureSize", allCaptureSize)
                .add("lastCaptureSize", lastCaptureSize)
                .add("restoredSnapshotList", restoredSnapshotList)
                .add("capturedSnapshotList", capturedSnapshotList)
                .add("successRestoreCount", successRestoreCount)
                .add("totalRestoreWallTime", totalRestoreWallTime)
                .add("totalRestoreSize", totalRestoreSize)
                .add("totalRestoreCpuTime", totalRestoreCpuTime)
                .add("restoringSnapshotId", restoringSnapshotId)
                .add("capturingSnapshotIds", capturingSnapshotIds)
                .toString();
    }

    public static SnapshotStats.Builder builder()
    {
        return new SnapshotStats.Builder();
    }

    public static class Builder
    {
        private long lastCaptureSnapshotId;
        private long totalCaptureCpuTime;
        private long totalCaptureWallTime;
        private long lastCaptureCpuTime;
        private long lastCaptureWallTime;
        private long allCaptureSize;
        private long lastCaptureSize;
        private List<Long> restoredSnapshotList;
        private List<Long> capturedSnapshotList;
        private long successRestoreCount;
        private long totalRestoreWallTime;
        private long totalRestoreSize;
        private long totalRestoreCpuTime;
        private long restoringSnapshotId;
        private List<Long> capturingSnapshotIds;

        private Builder() {}

        public Builder setLastCaptureSnapshotId(long lastCaptureSnapshotId)
        {
            this.lastCaptureSnapshotId = lastCaptureSnapshotId;
            return this;
        }

        public Builder setTotalCpuTimeMillis(long totalCaptureCpuTime)
        {
            this.totalCaptureCpuTime = totalCaptureCpuTime;
            return this;
        }

        public Builder setTotalWallTimeMillis(long totalCaptureWallTime)
        {
            this.totalCaptureWallTime = totalCaptureWallTime;
            return this;
        }

        public Builder setLastSnapshotCpuTimeMillis(long lastCaptureCpuTime)
        {
            this.lastCaptureCpuTime = lastCaptureCpuTime;
            return this;
        }

        public Builder setLastSnapshotWallTimeMillis(long lastCaptureWallTime)
        {
            this.lastCaptureWallTime = lastCaptureWallTime;
            return this;
        }

        public Builder setAllSnapshotsSizeBytes(long allCaptureSize)
        {
            this.allCaptureSize = allCaptureSize;
            return this;
        }

        public Builder setLastSnapshotSizeBytes(long lastCaptureSize)
        {
            this.lastCaptureSize = lastCaptureSize;
            return this;
        }

        public Builder setRestoringSnapshotId(long restoringSnapshotId)
        {
            this.restoringSnapshotId = restoringSnapshotId;
            return this;
        }

        public Builder setRestoredSnapshotList(List<Long> restoredSnapshotList)
        {
            this.restoredSnapshotList = restoredSnapshotList;
            return this;
        }

        public Builder setCapturedSnapshotList(List<Long> capturedSnapshotList)
        {
            this.capturedSnapshotList = capturedSnapshotList;
            return this;
        }

        public Builder setSuccessRestoreCount(long successRestoreCount)
        {
            this.successRestoreCount = successRestoreCount;
            return this;
        }

        public Builder setTotalRestoreWallTime(long totalRestoreWallTime)
        {
            this.totalRestoreWallTime = totalRestoreWallTime;
            return this;
        }

        public Builder setTotalRestoreSize(long totalRestoreSize)
        {
            this.totalRestoreSize = totalRestoreSize;
            return this;
        }

        public Builder setTotalRestoreCpuTime(long totalRestoreCpuTime)
        {
            this.totalRestoreCpuTime = totalRestoreCpuTime;
            return this;
        }

        public Builder setCapturingSnapshotIds(List<Long> capturingSnapshotIds)
        {
            this.capturingSnapshotIds = capturingSnapshotIds;
            return this;
        }

        public SnapshotStats build()
        {
            return new SnapshotStats(
                    lastCaptureSnapshotId,
                    totalCaptureCpuTime,
                    totalCaptureWallTime,
                    lastCaptureCpuTime,
                    lastCaptureWallTime,
                    allCaptureSize,
                    lastCaptureSize,
                    restoredSnapshotList,
                    capturedSnapshotList,
                    successRestoreCount,
                    totalRestoreWallTime,
                    totalRestoreSize,
                    totalRestoreCpuTime,
                    restoringSnapshotId,
                    capturingSnapshotIds);
        }
    }
}
