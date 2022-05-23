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
package io.prestosql.failuredetector;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class GossipProtocolConfig
{
    public static final String GOSSIP_CONFIG_PREFIX = "failure-detector.";
    public static final String GOSSIP_GROUP_SIZE = GOSSIP_CONFIG_PREFIX + "gossip-group-size";
    public static final String CN_GOSSIP_PROBE_INTERVAL = GOSSIP_CONFIG_PREFIX + "coordinator-gossip-probe-interval";
    public static final String WK_GOSSIP_PROBE_INTERVAL = GOSSIP_CONFIG_PREFIX + "worker-gossip-probe-interval";
    public static final String CN_GOSSIP_COLLATE_INTERVAL = GOSSIP_CONFIG_PREFIX + "coordinator-gossip-collate-interval";

    private static final int CN_GOSSIP_PROBE_INTERVAL_SEC = 5;
    private static final int WK_GOSSIP_PROBE_INTERVAL_SEC = 5;
    private static final int CN_GOSSIP_COLLATE_INTERVAL_SEC = 2;

    private Duration cnWkUpdateGossipMonitorServiceInterval = new Duration(CN_GOSSIP_PROBE_INTERVAL_SEC, TimeUnit.SECONDS);
    private Duration wkWkUpdateGossipMonitorServiceInterval = new Duration(WK_GOSSIP_PROBE_INTERVAL_SEC, TimeUnit.SECONDS);
    private Duration cnGossipCollateInterval = new Duration(CN_GOSSIP_COLLATE_INTERVAL_SEC, TimeUnit.SECONDS);
    private int gossipGroupSize = Integer.MAX_VALUE; // any number more than the cluster size means all to all node gossip

    @Config(GOSSIP_GROUP_SIZE)
    public GossipProtocolConfig setGossipGroupSize(int s)
    {
        this.gossipGroupSize = s;
        return this;
    }

    public int getGossipGroupSize()
    {
        return this.gossipGroupSize;
    }

    @Config(CN_GOSSIP_PROBE_INTERVAL)
    public GossipProtocolConfig setCnWkUpdateGossipMonitorServiceInterval(Duration sec)
    {
        this.cnWkUpdateGossipMonitorServiceInterval = sec;
        return this;
    }

    public Duration getCnWkUpdateGossipMonitorServiceInterval()
    {
        return this.cnWkUpdateGossipMonitorServiceInterval;
    }

    @Config(WK_GOSSIP_PROBE_INTERVAL)
    public GossipProtocolConfig setWkWkUpdateGossipMonitorServiceInterval(Duration sec)
    {
        this.wkWkUpdateGossipMonitorServiceInterval = sec;
        return this;
    }

    public Duration getWkWkUpdateGossipMonitorServiceInterval()
    {
        return this.wkWkUpdateGossipMonitorServiceInterval;
    }

    @Config(CN_GOSSIP_COLLATE_INTERVAL)
    public GossipProtocolConfig setCnGossipCollateInterval(Duration sec)
    {
        this.cnGossipCollateInterval = sec;
        return this;
    }

    public Duration getCnGossipCollateInterval()
    {
        return this.cnGossipCollateInterval;
    }
}
