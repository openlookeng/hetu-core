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
package io.prestosql.spi;

/**
 * HetuConstant contains constant variables used by Hetu classes
 * @since 2019-11-29
 */
public class HetuConstant
{
    private HetuConstant() {}

    public static final String FILTER_ENABLED = "hetu.heuristicindex.filter.enabled";
    public static final String FILTER_CACHE_MAX_MEMORY = "hetu.heuristicindex.filter.cache.max-memory";
    public static final String FILTER_CACHE_LOADING_THREADS = "hetu.heuristicindex.filter.cache.loading-threads";
    public static final String FILTER_CACHE_LOADING_DELAY = "hetu.heuristicindex.filter.cache.loading-delay";
    public static final String FILTER_CACHE_TTL = "hetu.heuristicindex.filter.cache.ttl";
    public static final String FILTER_CACHE_SOFT_REFERENCE = "hetu.heuristicindex.filter.cache.soft-reference";
    public static final String INDEXSTORE_URI = "hetu.heuristicindex.indexstore.uri";
    public static final String INDEXSTORE_FILESYSTEM_PROFILE = "hetu.heuristicindex.indexstore.filesystem.profile";
    public static final String DATA_CENTER_CONNECTOR_NAME = "dc";
    public static final String CONNECTION_USER = "connection-user";
    public static final String CONNECTION_URL = "connection-url";
    public static final String MULTI_COORDINATOR_ENABLED = "hetu.multi-coordinator.enabled";
    public static final String SPLIT_CACHE_MAP_ENABLED = "hetu.split-cache-map.enabled";
    public static final String SPLIT_CACHE_STATE_UPDATE_INTERVAL = "hetu.split-cache-map.state-update-interval";
    public static final String ENCRYPTED_PROPERTIES = "encrypted-properties";
    public static final long KILOBYTE = 1024L;
}
