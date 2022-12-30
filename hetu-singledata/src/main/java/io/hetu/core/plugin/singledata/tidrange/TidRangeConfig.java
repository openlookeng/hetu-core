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

package io.hetu.core.plugin.singledata.tidrange;

import io.airlift.configuration.Config;

import javax.validation.constraints.Min;

public class TidRangeConfig
{
    private int maxConnectionCountPerNode = 100;
    private int maxTableSplitCountPerNode = 50;
    private long connectionTimeout;
    private long maxLifetime = 1800000L;

    @Min(1)
    public int getMaxConnectionCountPerNode()
    {
        return maxConnectionCountPerNode;
    }

    @Config("tidrange.max-connection-count-per-node")
    public TidRangeConfig setMaxConnectionCountPerNode(int maxConnectionCountPerNode)
    {
        this.maxConnectionCountPerNode = maxConnectionCountPerNode;
        return this;
    }

    @Min(1)
    public int getMaxTableSplitCountPerNode()
    {
        return maxTableSplitCountPerNode;
    }

    @Config("tidrange.max-table-split-count-per-node")
    public TidRangeConfig setMaxTableSplitCountPerNode(int maxTableSplitCountPerNode)
    {
        this.maxTableSplitCountPerNode = maxTableSplitCountPerNode;
        return this;
    }

    public long getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("tidrange.connection-timeout")
    public TidRangeConfig setConnectionTimeout(long connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @Min(30000L)
    public long getMaxLifetime()
    {
        return maxLifetime;
    }

    @Config("tidrange.max-lifetime")
    public TidRangeConfig setMaxLifetime(long maxLifetime)
    {
        this.maxLifetime = maxLifetime;
        return this;
    }
}
