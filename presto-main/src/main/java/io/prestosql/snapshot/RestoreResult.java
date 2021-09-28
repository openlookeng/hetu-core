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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * RestoreResult contains information of restoring process from snapshot, and report to coordinator
 */
public class RestoreResult
{
    private long snapshotId;
    private SnapshotResult snapshotResult;

    public RestoreResult()
    {
        this(0, SnapshotResult.IN_PROGRESS);
    }

    @JsonCreator
    public RestoreResult(@JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("snapshotResult") SnapshotResult snapshotResult)
    {
        this.snapshotId = snapshotId;
        this.snapshotResult = snapshotResult;
    }

    @JsonProperty
    public long getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public SnapshotResult getSnapshotResult()
    {
        return snapshotResult;
    }

    boolean setSnapshotResult(long snapshotId, SnapshotResult snapshotResult)
    {
        boolean changed = false;
        if (this.snapshotId != snapshotId) {
            this.snapshotId = snapshotId;
            changed = true;
        }
        if (this.snapshotResult != snapshotResult) {
            this.snapshotResult = snapshotResult;
            changed = true;
        }
        return changed;
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
        RestoreResult that = (RestoreResult) o;
        return snapshotId == that.snapshotId &&
                snapshotResult == that.snapshotResult;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(snapshotId);
    }
}
