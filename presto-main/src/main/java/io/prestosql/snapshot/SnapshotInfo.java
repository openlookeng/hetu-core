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
package io.prestosql.snapshot;

import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SnapshotInfo
{
    // Snapshot status
    private SnapshotResult snapshotResult;
    // captured size in bytes (consolidated to task level)
    private AtomicLong sizeBytes;
    // CPU time taken while capturing the state (consolidated to task level)
    private AtomicLong captureCpuTime;

    public SnapshotInfo()
    {
        this.sizeBytes.set(0);
        this.captureCpuTime.set(0);
        this.snapshotResult = SnapshotResult.IN_PROGRESS;
    }

    public SnapshotResult getSnapshotResult()
    {
        return snapshotResult;
    }

    public AtomicLong getSizeBytes()
    {
        return sizeBytes;
    }

    public AtomicLong getCaptureCpuTime()
    {
        return captureCpuTime;
    }

    public void setSnapshotResult(SnapshotResult snapshotResult)
    {
        this.snapshotResult = snapshotResult;
    }

    public void updateSizeBytes(long sizeBytes)
    {
        this.sizeBytes.addAndGet(sizeBytes);
    }

    public void updateCaptureCpuTime(long captureCpuTime)
    {
        this.captureCpuTime.addAndGet(captureCpuTime);
    }

    public static SnapshotInfo withStatus(SnapshotResult result)
    {
        SnapshotInfo info = new SnapshotInfo();
        info.setSnapshotResult(result);
        return info;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("snapshotResult", snapshotResult)
                .add("sizeBytes", sizeBytes)
                .add("captureCpuTime", captureCpuTime)
                .toString();
    }
}
