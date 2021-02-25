/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
public class SnapshotConfig
{
    // Temporary constant. May make configurable later.
    // Don't use all nodes. Reserve some to be used to schedule tasks from failed nodes
    public static final float MAX_NODE_ALLOCATION = 80 / 100F;

    public static final String SNAPSHOT_PROFILE = "hetu.snapshot.profile";
    public static final String SNAPSHOT_INTERVAL_TYPE = "hetu.snapshot.intervalType";
    public static final String SNAPSHOT_TIME_INTERVAL = "hetu.snapshot.timeInterval";
    public static final String SNAPSHOT_SPLIT_COUNT_INTERVAL = "hetu.snapshot.splitCountInterval";
    public static final String SNAPSHOT_MAX_RETRIES = "hetu.snapshot.maxRetries";
    public static final String SNAPSHOT_RETRY_TIMEOUT = "hetu.snapshot.retryTimeout";

    private String snapshotProfile;

    private IntervalType snapshotIntervalType = IntervalType.TIME;
    private Duration snapshotTimeInterval = new Duration(2, TimeUnit.MINUTES);
    private long snapshotSplitCountInterval = 1_000;
    private long snapshotMaxRetries = 10;
    private Duration snapshotRetryTimeout = new Duration(3, TimeUnit.MINUTES);

    public enum IntervalType
    {
        TIME,
        SPLIT_COUNT
    }

    public String getSnapshotProfile()
    {
        return snapshotProfile;
    }

    @Config(SNAPSHOT_PROFILE)
    @ConfigDescription("snapshot profile")
    public SnapshotConfig setSnapshotProfile(String snapshotProfile)
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
    public SnapshotConfig setSnapshotIntervalType(IntervalType snapshotIntervalType)
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
    public SnapshotConfig setSnapshotTimeInterval(Duration snapshotTimeInterval)
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
    public SnapshotConfig setSnapshotSplitCountInterval(long snapshotSplitCountInterval)
    {
        this.snapshotSplitCountInterval = snapshotSplitCountInterval;
        return this;
    }

    @Min(1)
    public long getSnapshotMaxRetries()
    {
        return snapshotMaxRetries;
    }

    @Config(SNAPSHOT_MAX_RETRIES)
    @ConfigDescription("snapshot max number of retries")
    public SnapshotConfig setSnapshotMaxRetries(long snapshotMaxRetries)
    {
        this.snapshotMaxRetries = snapshotMaxRetries;
        return this;
    }

    @NotNull
    public Duration getSnapshotRetryTimeout()
    {
        return snapshotRetryTimeout;
    }

    @Config(SNAPSHOT_RETRY_TIMEOUT)
    @ConfigDescription("snapshot retry timeout")
    public SnapshotConfig setSnapshotRetryTimeout(Duration snapshotRetryTimeout)
    {
        this.snapshotRetryTimeout = snapshotRetryTimeout;
        return this;
    }
}
