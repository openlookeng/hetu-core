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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * This class contains all configs of snapshot
 */
public class RecoveryConfig
{
    // Temporary constant. May make configurable later.
    // Don't use all nodes. Reserve some to be used to schedule tasks from failed nodes
    private static final float MAX_NODE_ALLOCATION = 80 / 100F;

    public static final String RECOVERY_MAX_RETRIES = "hetu.recovery.maxRetries";
    public static final String RECOVERY_RETRY_TIMEOUT = "hetu.recovery.retryTimeout";
    public static final String SNAPSHOT_PROFILE = "hetu.experimental.snapshot.profile";
    public static final String SNAPSHOT_INTERVAL_TYPE = "hetu.internal.snapshot.intervalType";
    public static final String SNAPSHOT_TIME_INTERVAL = "hetu.internal.snapshot.timeInterval";
    public static final String SNAPSHOT_SPLIT_COUNT_INTERVAL = "hetu.internal.snapshot.splitCountInterval";
    public static final String SNAPSHOT_USE_KRYO_SERIALIZATION = "hetu.snapshot.useKryoSerialization";
    public static final String SPILLER_SPILL_PROFILE = "experimental.spiller-spill-profile";
    public static final String SPILLER_SPILL_TO_HDFS = "experimental.spiller-spill-to-hdfs";

    private String snapshotProfile;
    private String spillProfile;
    private boolean spillToHdfs;

    private long recoveryMaxRetries = 10;
    private Duration recoveryRetryTimeout = new Duration(10, TimeUnit.MINUTES);
    private boolean isSnapshotCaptureEnabled;
    private IntervalType snapshotIntervalType = IntervalType.TIME;
    private Duration snapshotTimeInterval = new Duration(5, TimeUnit.MINUTES);
    private long snapshotSplitCountInterval = 1_000;
    private boolean snapshotUseKryoSerialization;

    public enum IntervalType
    {
        TIME,
        SPLIT_COUNT
    }

    public static int calculateTaskCount(int nodeCount)
    {
        if (nodeCount < 2) {
            return nodeCount;
        }
        // Where possible, reserve some nodes in case of node failures
        return (int) (nodeCount * MAX_NODE_ALLOCATION);
    }

    @Min(1)
    public long getRecoveryMaxRetries()
    {
        return recoveryMaxRetries;
    }

    @Config(RECOVERY_MAX_RETRIES)
    @ConfigDescription("Recovery max number of retries")
    public RecoveryConfig setRecoveryMaxRetries(long recoveryMaxRetries)
    {
        this.recoveryMaxRetries = recoveryMaxRetries;
        return this;
    }

    @NotNull
    public Duration getRecoveryRetryTimeout()
    {
        return recoveryRetryTimeout;
    }

    @Config(RECOVERY_RETRY_TIMEOUT)
    @ConfigDescription("Recovery retry timeout")
    public RecoveryConfig setRecoveryRetryTimeout(Duration recoveryRetryTimeout)
    {
        this.recoveryRetryTimeout = recoveryRetryTimeout;
        return this;
    }

    public String getSnapshotProfile()
    {
        return snapshotProfile;
    }

    @Config(SNAPSHOT_PROFILE)
    @ConfigDescription("snapshot profile")
    public RecoveryConfig setSnapshotProfile(String snapshotProfile)
    {
        this.snapshotProfile = snapshotProfile;
        return this;
    }

    @NotNull
    public IntervalType getSnapshotIntervalType()
    {
        return snapshotIntervalType;
    }

    @Config(SNAPSHOT_INTERVAL_TYPE)
    @ConfigDescription("snapshot interval type")
    public RecoveryConfig setSnapshotIntervalType(IntervalType snapshotIntervalType)
    {
        this.snapshotIntervalType = snapshotIntervalType;
        return this;
    }

    @NotNull
    public Duration getSnapshotTimeInterval()
    {
        return snapshotTimeInterval;
    }

    @Config(SNAPSHOT_TIME_INTERVAL)
    @ConfigDescription("snapshot time interval")
    public RecoveryConfig setSnapshotTimeInterval(Duration snapshotTimeInterval)
    {
        this.snapshotTimeInterval = snapshotTimeInterval;
        return this;
    }

    @Min(1)
    public long getSnapshotSplitCountInterval()
    {
        return snapshotSplitCountInterval;
    }

    @Config(SNAPSHOT_SPLIT_COUNT_INTERVAL)
    @ConfigDescription("snapshot split count interval")
    public RecoveryConfig setSnapshotSplitCountInterval(long snapshotSplitCountInterval)
    {
        this.snapshotSplitCountInterval = snapshotSplitCountInterval;
        return this;
    }

    public boolean isSnapshotUseKryoSerialization()
    {
        return snapshotUseKryoSerialization;
    }

    @Config(SNAPSHOT_USE_KRYO_SERIALIZATION)
    @ConfigDescription("snapshot use kryo serialization")
    public RecoveryConfig setSnapshotUseKryoSerialization(boolean snapshotUseKryoSerialization)
    {
        this.snapshotUseKryoSerialization = snapshotUseKryoSerialization;
        return this;
    }

    public String getSpillProfile()
    {
        return spillProfile;
    }

    @Config(SPILLER_SPILL_PROFILE)
    @ConfigDescription("spill profile")
    public RecoveryConfig setSpillProfile(String spillProfile)
    {
        this.spillProfile = spillProfile;
        return this;
    }

    public boolean isSpillToHdfs()
    {
        return spillToHdfs;
    }

    @Config(SPILLER_SPILL_TO_HDFS)
    @ConfigDescription("spill to hdfs")
    public RecoveryConfig setSpillToHdfs(boolean spillToHdfs)
    {
        this.spillToHdfs = spillToHdfs;
        return this;
    }
}
