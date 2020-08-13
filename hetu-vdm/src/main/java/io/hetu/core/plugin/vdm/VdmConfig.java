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
package io.hetu.core.plugin.vdm;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * vdm config
 *
 * @since 2020-02-27
 */
public class VdmConfig
{
    private static final long DEFAULT_MAXIMUM_SIZE = 10000L;
    private boolean isMetadataCacheEnabled = true; // enable metadata caching for vdm
    private Duration metadataCacheTtl = new Duration(1, TimeUnit.SECONDS); // metadata cache eviction time
    private long metadataCacheMaximumSize = DEFAULT_MAXIMUM_SIZE; // metadata cache max size

    public boolean isMetadataCacheEnabled()
    {
        return isMetadataCacheEnabled;
    }

    /**
     * set metadata cache
     *
     * @param isCacheEnabled metadata cache entable
     * @return vdm config
     */
    @Config("vdm.metadata-cache-enabled")
    @ConfigDescription("Enable metadata caching")
    public VdmConfig setMetadataCacheEnabled(boolean isCacheEnabled)
    {
        this.isMetadataCacheEnabled = isCacheEnabled;
        return this;
    }

    @NotNull
    public @MinDuration("0ms") Duration getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    /**
     * set metadata cache ttl
     *
     * @param metadataCacheTtl metadata cache ttl
     * @return vdm config
     */
    @Config("vdm.metadata-cache-ttl")
    @ConfigDescription("Set the metadata cache eviction time for vdm connector")
    public VdmConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }

    @Min(1)
    public long getMetadataCacheMaximumSize()
    {
        return metadataCacheMaximumSize;
    }

    /**
     * metadata cache maximum sise
     *
     * @param metadataCacheMaximumSize maxinum size of cache
     * @return vdm config
     */
    @Config("vdm.metadata-cache-maximum-size")
    @ConfigDescription("Set the metadata cache max size for vdm connector")
    public VdmConfig setMetadataCacheMaximumSize(long metadataCacheMaximumSize)
    {
        this.metadataCacheMaximumSize = metadataCacheMaximumSize;
        return this;
    }
}
