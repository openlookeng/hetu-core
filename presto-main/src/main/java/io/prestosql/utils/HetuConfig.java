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
package io.prestosql.utils;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.hetu.core.common.util.SecurePathWhiteList;
import io.prestosql.spi.HetuConstant;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.GIGABYTE;

/**
 * HetuConfig contains Hetu configurations
 *
 * @since 2019-11-29
 */
public class HetuConfig
{
    private Boolean enableFilter = Boolean.FALSE;
    private DataSize indexCacheMaxMemory = new DataSize(10, GIGABYTE);
    private long indexCacheLoadingThreads = 10L;
    private Duration indexCacheLoadingDelay = new Duration(10, TimeUnit.SECONDS);
    private Duration indexCacheTTL = new Duration(24, TimeUnit.HOURS);
    private Boolean indexCacheSoftReference = Boolean.TRUE;
    private String indexStoreUri = "/opt/hetu/indices/";
    private String indexStoreFileSystemProfile = "local-config-default";
    private boolean indexAutoload = true;
    private String indexToPreload = "";
    private Boolean enableEmbeddedStateStore = Boolean.FALSE;
    private Boolean enableMultipleCoordinator = Boolean.FALSE;
    private Duration stateUpdateInterval = new Duration(100, TimeUnit.MILLISECONDS);
    private Duration stateFetchInterval = new Duration(100, TimeUnit.MILLISECONDS);
    private Duration querySubmitTimeout = new Duration(10, TimeUnit.SECONDS);
    private Duration stateExpireTime = new Duration(60, TimeUnit.SECONDS);
    private int dataCenterSplits = 5;
    private Duration dataCenterConsumerTimeout = new Duration(10, TimeUnit.MINUTES);
    private boolean executionPlanCacheEnabled;
    private long executionPlanCacheMaxItems = 10000L;
    private long executionPlanCacheTimeout = 86400000L;
    private boolean splitCacheMapEnabled = Boolean.FALSE;
    private Duration splitCacheStateUpdateInterval = new Duration(2, TimeUnit.SECONDS);
    private boolean isTraceStackVisible;

    public HetuConfig()
    {
    }

    @NotNull
    public boolean isFilterEnabled()
    {
        return enableFilter;
    }

    @Config(HetuConstant.FILTER_ENABLED)
    @ConfigDescription("Is split filter enabled")
    public HetuConfig setFilterEnabled(boolean enabled)
    {
        this.enableFilter = enabled;
        return this;
    }

    @NotNull
    public String getIndexStoreUri()
    {
        // if indexStoreUri is null, let the config manager throw the error, it's more user friendly
        if (indexStoreUri != null) {
            try {
                checkArgument(!indexStoreUri.toString().contains("../"),
                        HetuConstant.INDEXSTORE_URI + " must be absolute and under one of the following whitelisted directories:  " + SecurePathWhiteList.getSecurePathWhiteList().toString());
                checkArgument(SecurePathWhiteList.isSecurePath(indexStoreUri),
                        HetuConstant.INDEXSTORE_URI + " must be under one of the following whitelisted directories: " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            }
            catch (IOException e) {
                throw new IllegalArgumentException("Failed to get secure path list.", e);
            }
        }
        return indexStoreUri;
    }

    @Config(HetuConstant.INDEXSTORE_URI)
    @ConfigDescription("default indexstore uri")
    public HetuConfig setIndexStoreUri(String indexStoreUri)
    {
        this.indexStoreUri = indexStoreUri;
        return this;
    }

    @NotNull
    public String getIndexStoreFileSystemProfile()
    {
        return indexStoreFileSystemProfile;
    }

    @Config(HetuConstant.INDEXSTORE_FILESYSTEM_PROFILE)
    @ConfigDescription("filesystem client profile for indexstore")
    public HetuConfig setIndexStoreFileSystemProfile(String indexStoreFileSystemProfile)
    {
        this.indexStoreFileSystemProfile = indexStoreFileSystemProfile;
        return this;
    }

    public Duration getIndexCacheTTL()
    {
        return this.indexCacheTTL;
    }

    @Config(HetuConstant.FILTER_CACHE_TTL)
    @ConfigDescription("The duration after which index cache expires")
    public HetuConfig setIndexCacheTTL(Duration indexCacheTTL)
    {
        this.indexCacheTTL = indexCacheTTL;
        return this;
    }

    public Boolean getIndexAutoload()
    {
        return indexAutoload;
    }

    @Config(HetuConstant.FILTER_CACHE_AUTOLOAD_DEFAULT)
    @ConfigDescription("The default value for autoload property when creating index. If set to true, all indexes will be automatically loaded to cache after creation or update. See CREATE INDEX docs for details.")
    public HetuConfig setIndexAutoload(Boolean autoloadEnabled)
    {
        this.indexAutoload = autoloadEnabled;
        return this;
    }

    public String getIndexToPreload()
    {
        return indexToPreload;
    }

    @Config(HetuConstant.FILTER_CACHE_PRELOAD_INDICES)
    @ConfigDescription("Comma separated list of index names to preload when server starts, or ALL to load all indices")
    public HetuConfig setIndexToPreload(String indices)
    {
        this.indexToPreload = indices;
        return this;
    }

    public Duration getIndexCacheLoadingDelay()
    {
        return this.indexCacheLoadingDelay;
    }

    @Config(HetuConstant.FILTER_CACHE_LOADING_DELAY)
    @ConfigDescription("The delay to wait before async loading task")
    public HetuConfig setIndexCacheLoadingDelay(Duration indexCacheLoadingDelay)
    {
        this.indexCacheLoadingDelay = indexCacheLoadingDelay;
        return this;
    }

    public long getIndexCacheLoadingThreads()
    {
        return this.indexCacheLoadingThreads;
    }

    @Config(HetuConstant.FILTER_CACHE_LOADING_THREADS)
    @ConfigDescription("The number of threads used to load indices in parallel")
    public HetuConfig setIndexCacheLoadingThreads(long indexCacheLoadingThreads)
    {
        this.indexCacheLoadingThreads = indexCacheLoadingThreads;
        return this;
    }

    public DataSize getIndexCacheMaxMemory()
    {
        return this.indexCacheMaxMemory;
    }

    @Config(HetuConstant.FILTER_CACHE_MAX_MEMORY)
    @ConfigDescription("The maximum memory size of indices cache")
    public HetuConfig setIndexCacheMaxMemory(DataSize indexCacheMaxMemory)
    {
        this.indexCacheMaxMemory = indexCacheMaxMemory;
        return this;
    }

    public Boolean isIndexCacheSoftReferenceEnabled()
    {
        return this.indexCacheSoftReference;
    }

    @Config(HetuConstant.FILTER_CACHE_SOFT_REFERENCE)
    @ConfigDescription("Experimental: Enabling index cache soft reference allows the Garbage Collector to" +
            " remove entries from the cache if memory is low")
    public HetuConfig setIndexCacheSoftReferenceEnabled(Boolean indexCacheSoftReference)
    {
        this.indexCacheSoftReference = indexCacheSoftReference;
        return this;
    }

    public Boolean isEmbeddedStateStoreEnabled()
    {
        return this.enableEmbeddedStateStore;
    }

    @Config("hetu.embedded-state-store.enabled")
    @ConfigDescription("Is embedded state store enabled")
    public HetuConfig setEmbeddedStateStoreEnabled(Boolean enableEmbeddedStateStore)
    {
        this.enableEmbeddedStateStore = enableEmbeddedStateStore;
        return this;
    }

    @NotNull
    public boolean isMultipleCoordinatorEnabled()
    {
        return this.enableMultipleCoordinator;
    }

    @Config("hetu.multiple-coordinator.enabled")
    @ConfigDescription("Is multiple coordinators enabled")
    public HetuConfig setMultipleCoordinatorEnabled(boolean enabled)
    {
        this.enableMultipleCoordinator = enabled;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getQuerySubmitTimeout()
    {
        return querySubmitTimeout;
    }

    @Config("hetu.multiple-coordinator.query-submit-timeout")
    @ConfigDescription("Timeout a coordinator allows to try to get a distributed lock to submit new query")
    public HetuConfig setQuerySubmitTimeout(Duration querySubmitTimeout)
    {
        this.querySubmitTimeout = querySubmitTimeout;
        return this;
    }

    @NotNull
    @MinDuration("10ms")
    public Duration getStateUpdateInterval()
    {
        return stateUpdateInterval;
    }

    @Config("hetu.multiple-coordinator.state-update-interval")
    @ConfigDescription("Time interval for updating states to state store")
    public HetuConfig setStateUpdateInterval(Duration stateUpdateInterval)
    {
        this.stateUpdateInterval = stateUpdateInterval;
        return this;
    }

    @NotNull
    @MinDuration("10ms")
    public Duration getStateFetchInterval()
    {
        return stateFetchInterval;
    }

    @Config("hetu.multiple-coordinator.state-fetch-interval")
    @ConfigDescription("Time interval for fetching states from state store")
    public HetuConfig setStateFetchInterval(Duration stateFetchInterval)
    {
        this.stateFetchInterval = stateFetchInterval;
        return this;
    }

    @NotNull
    @MinDuration("5s")
    public Duration getStateExpireTime()
    {
        return stateExpireTime;
    }

    @Config("hetu.multiple-coordinator.state-expire-time")
    @ConfigDescription("Time gap to detect if a state has expired")
    public HetuConfig setStateExpireTime(Duration stateExpireTime)
    {
        this.stateExpireTime = stateExpireTime;
        return this;
    }

    public int getDataCenterSplits()
    {
        return dataCenterSplits;
    }

    @Config("hetu.data.center.split.count")
    @ConfigDescription("Number of splits generated for each data center query results")
    public HetuConfig setDataCenterSplits(int dataCenterSplits)
    {
        this.dataCenterSplits = dataCenterSplits;
        return this;
    }

    public Duration getDataCenterConsumerTimeout()
    {
        return dataCenterConsumerTimeout;
    }

    @Config("hetu.data.center.consumer.timeout")
    @ConfigDescription("Maximum inactive time a data center query can survive without active consumers")
    public HetuConfig setDataCenterConsumerTimeout(Duration dataCenterConsumerTimeout)
    {
        this.dataCenterConsumerTimeout = dataCenterConsumerTimeout;
        return this;
    }

    public boolean isExecutionPlanCacheEnabled()
    {
        return executionPlanCacheEnabled;
    }

    @Config("hetu.executionplan.cache.enabled")
    @ConfigDescription("Enable or disable execution plan cache. Enabled by default.")
    public HetuConfig setExecutionPlanCacheEnabled(boolean executionPlanCacheEnabled)
    {
        this.executionPlanCacheEnabled = executionPlanCacheEnabled;
        return this;
    }

    public long getExecutionPlanCacheMaxItems()
    {
        return executionPlanCacheMaxItems;
    }

    @Config("hetu.executionplan.cache.limit")
    @ConfigDescription("Maximum number of execution plans to keep in the cache")
    public HetuConfig setExecutionPlanCacheMaxItems(long executionPlanCacheMaxItems)
    {
        this.executionPlanCacheMaxItems = executionPlanCacheMaxItems;
        return this;
    }

    public long getExecutionPlanCacheTimeout()
    {
        return executionPlanCacheTimeout;
    }

    @Config("hetu.executionplan.cache.timeout")
    @ConfigDescription("Time in milliseconds to expire cached execution plans after the last access.")
    public HetuConfig setExecutionPlanCacheTimeout(long executionPlanCacheTimeout)
    {
        this.executionPlanCacheTimeout = executionPlanCacheTimeout;
        return this;
    }

    public boolean isSplitCacheMapEnabled()
    {
        return splitCacheMapEnabled;
    }

    @Config("hetu.split-cache-map.enabled")
    @ConfigDescription("Enable or disable split cache map. Disabled by default.")
    public HetuConfig setSplitCacheMapEnabled(boolean splitCacheMapEnabled)
    {
        this.splitCacheMapEnabled = splitCacheMapEnabled;
        return this;
    }

    @MinDuration("10ms")
    public Duration getSplitCacheStateUpdateInterval()
    {
        return splitCacheStateUpdateInterval;
    }

    @Config("hetu.split-cache-map.state-update-interval")
    @ConfigDescription("Time interval for updating split cache map to state store")
    public HetuConfig setSplitCacheStateUpdateInterval(Duration stateUpdateInterval)
    {
        this.splitCacheStateUpdateInterval = stateUpdateInterval;
        return this;
    }

    public boolean isTraceStackVisible()
    {
        return this.isTraceStackVisible;
    }

    @Config(HetuConstant.TRACE_STACK_VISIBLE)
    @ConfigDescription("Is stack trace visible for user")
    public HetuConfig setTraceStackVisible(boolean isTraceStackVisible)
    {
        this.isTraceStackVisible = isTraceStackVisible;
        System.setProperty("stack-trace-visible", String.valueOf(isTraceStackVisible));
        return this;
    }
}
