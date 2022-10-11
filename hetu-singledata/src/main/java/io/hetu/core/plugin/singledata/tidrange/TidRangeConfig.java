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
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TidRangeConfig
{
    private long maxSplitCount = 100;
    private DataSize pageSize = new DataSize(8, KILOBYTE);
    private DataSize defaultSplitSize = new DataSize(32, MEGABYTE);

    @Min(1)
    public long getMaxSplitCount()
    {
        return maxSplitCount;
    }

    @Config("tidrange.max-split-count")
    public TidRangeConfig setMaxSplitCount(long maxSplitCount)
    {
        this.maxSplitCount = maxSplitCount;
        return this;
    }

    @NotNull
    public DataSize getPageSize()
    {
        return pageSize;
    }

    @Config("tidrange.page-size")
    public TidRangeConfig setPageSize(DataSize pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }

    @MinDataSize("1MB")
    @MaxDataSize("1GB")
    public DataSize getDefaultSplitSize()
    {
        return defaultSplitSize;
    }

    @Config("tidrange.default-split-size")
    public TidRangeConfig setDefaultSplitSize(DataSize defaultSplitSize)
    {
        this.defaultSplitSize = defaultSplitSize;
        return this;
    }
}
