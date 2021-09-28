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
package io.prestosql.spi;

/**
 * HetuConstant contains constant variables used by Hetu classes
 *
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
    public static final String FILTER_CACHE_PRELOAD_INDICES = "hetu.heuristicindex.filter.cache.preload-indices";
    public static final String FILTER_CACHE_AUTOLOAD_DEFAULT = "hetu.heuristicindex.filter.cache.autoload-default";
    public static final String INDEXSTORE_URI = "hetu.heuristicindex.indexstore.uri";
    public static final String INDEXSTORE_FILESYSTEM_PROFILE = "hetu.heuristicindex.indexstore.filesystem.profile";
    public static final String DATA_CENTER_CONNECTOR_NAME = "dc";
    public static final String CONNECTION_USER = "connection-user";
    public static final String CONNECTION_URL = "connection-url";
    public static final String MULTI_COORDINATOR_ENABLED = "hetu.multi-coordinator.enabled";
    public static final String SPLIT_CACHE_MAP_ENABLED = "hetu.split-cache-map.enabled";
    public static final String SPLIT_CACHE_STATE_UPDATE_INTERVAL = "hetu.split-cache-map.state-update-interval";
    public static final String ENCRYPTED_PROPERTIES = "encrypted-properties";
    public static final String TRACE_STACK_VISIBLE = "stack-trace-visible";
    public static final long KILOBYTE = 1024L;
    public static final String DATASOURCE_CATALOG = "connector.name";
    public static final String DATASOURCE_FILE_PATH = "datasource_file_path";
    public static final String DATASOURCE_FILE_MODIFICATION = "datasource_file_modification";
    public static final String DATASOURCE_INDEX_LEVEL = "datasource_index_level";
    public static final String DATASOURCE_STRIPE_NUMBER = "datasource_stripe_number";
    public static final String DATASOURCE_STRIPE_OFFSET = "datasource_stripe_offset";
    public static final String DATASOURCE_STRIPE_LENGTH = "datasource_stripe_length";
    public static final String DATASOURCE_TOTAL_PAGES = "datasource_total_pages";
    public static final String DATASOURCE_PAGE_NUMBER = "datasource_page_number";
    public static final String CONNECTOR_PLANOPTIMIZER_RULE_BLACKLIST = "connector-planoptimizer-rule-blacklist";
    public static final String INDEX_TABLE_DELETED = "Table Deleted";
    public static final String INDEX_OUT_OF_SYNC = "Out-of-sync";
    public static final String INDEX_OK = "OK";

    // error message
    public static final String HINDEX_CONFIG_ERROR_MSG = "Heuristic Index is not enabled in config.properties or is configured incorrectly.";
}
