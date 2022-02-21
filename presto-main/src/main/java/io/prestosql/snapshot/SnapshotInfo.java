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
package io.prestosql.snapshot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SnapshotInfo
        implements Serializable
{
    // Snapshot status
    private SnapshotResult snapshotResult;
    // captured/restored size in bytes (consolidated to task level)
    private AtomicLong sizeBytes;
    // CPU time taken while capturing/restoring the state
    private AtomicLong cpuTime;
    // To track snapshot capture/restore begin and end time (Wall time) at query level
    private long beginTime;
    private long endTime;
    // Used for capture result, to mark snapshot capture was succesful or not
    private boolean completeSnapshot;

    @JsonCreator
    public SnapshotInfo(
            @JsonProperty("sizeBytes") long sizeBytes,
            @JsonProperty("cpuTime") long cpuTime,
            @JsonProperty("beginTime") long beginTime,
            @JsonProperty("endTime") long endTime,
            @JsonProperty("snapshotResult") SnapshotResult snapshotResult)
    {
        this.sizeBytes = new AtomicLong(sizeBytes);
        this.cpuTime = new AtomicLong(cpuTime);
        this.beginTime = beginTime;
        this.endTime = endTime;
        this.snapshotResult = snapshotResult;
        this.completeSnapshot = false;
    }

    @JsonProperty
    public SnapshotResult getSnapshotResult()
    {
        return snapshotResult;
    }

    @JsonProperty
    public long getSizeBytes()
    {
        return sizeBytes.get();
    }

    @JsonProperty
    public long getCpuTime()
    {
        return cpuTime.get();
    }

    @JsonProperty
    public long getBeginTime()
    {
        return beginTime;
    }

    @JsonProperty
    public long getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public void setSnapshotResult(SnapshotResult snapshotResult)
    {
        this.snapshotResult = snapshotResult;
    }

    @JsonProperty
    public void setBeginTime(long beginTime)
    {
        this.beginTime = beginTime;
    }

    @JsonProperty
    public void setEndTime(long endTime)
    {
        this.endTime = endTime;
    }

    @JsonProperty
    public void updateSizeBytes(long sizeBytes)
    {
        this.sizeBytes.addAndGet(sizeBytes);
    }

    @JsonProperty
    public void updateCpuTime(long cpuTime)
    {
        this.cpuTime.addAndGet(cpuTime);
    }

    @JsonProperty
    public boolean isCompleteSnapshot()
    {
        return completeSnapshot;
    }

    @JsonProperty
    public void setCompleteSnapshot(boolean restoreCompleted)
    {
        this.completeSnapshot = restoreCompleted;
    }

    public static SnapshotInfo withStatus(SnapshotResult result)
    {
        SnapshotInfo info = new SnapshotInfo(0, 0, 0, 0, result);
        info.setSnapshotResult(result);
        return info;
    }

    public void updateStats(SnapshotInfo curSnapshotInfo)
    {
        // Update only Size and CpuTime, which to be accumulated from task level
        sizeBytes.addAndGet(curSnapshotInfo.getSizeBytes());
        cpuTime.addAndGet(curSnapshotInfo.getCpuTime());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("snapshotResult", snapshotResult)
                .add("sizeBytes", sizeBytes)
                .add("cpuTime", cpuTime)
                .add("beginTime", beginTime)
                .add("endTime", endTime)
                .add("completeSnapshot", completeSnapshot)
                .toString();
    }
}
