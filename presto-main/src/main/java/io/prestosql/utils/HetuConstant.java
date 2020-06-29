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
package io.prestosql.utils;

/**
 * HetuConstant contains constant variables used by Hetu classes
 * @since 2019-11-29
 */
public class HetuConstant
{
    private HetuConstant() {}

    public static final String FILTER_ENABLED = "hetu.filter.enabled";
    public static final String FILTER_MAX_INDICES_IN_CACHE = "hetu.filter.cache.max-indices-number";
    public static final String FILTER_PLUGINS = "hetu.filter.plugins";
    public static final String INDEXSTORE_KEYS_PREFIX = "hetu.filter.indexstore.";
    public static final String INDEXSTORE_URI = "hetu.filter.indexstore.uri";
    public static final String INDEXSTORE_TYPE = "hetu.filter.indexstore.type";
    public static final String INDEXSTORE_TYPE_HDFS = "hdfs";
    public static final String INDEXSTORE_TYPE_LOCAL = "local";
    public static final String INDEXSTORE_HDFS_CONFIG_RESOURCES = "hetu.filter.indexstore.hdfs.config.resources";
    public static final String INDEXSTORE_HDFS_AUTHENTICATION_TYPE = "hetu.filter.indexstore.hdfs.authentication.type";
    public static final String INDEXSTORE_HDFS_AUTHENTICATION_TYPE_KERBEROS = "KERBEROS";
    public static final String INDEXSTORE_HDFS_AUTHENTICATION_TYPE_NONE = "NONE";
    public static final String INDEXSTORE_HDFS_KRB5_CONFIG_PATH = "hetu.filter.indexstore.hdfs.krb5.conf.path";
    public static final String INDEXSTORE_HDFS_KRB5_KEYTAB_PATH = "hetu.filter.indexstore.hdfs.krb5.keytab.path";
    public static final String INDEXSTORE_HDFS_KRB5_PRINCIPAL = "hetu.filter.indexstore.hdfs.krb5.principal";
    public static final String DATA_CENTER_CONNECTOR_NAME = "dc";
    public static final String CONNECTION_USER = "connection-user";
    public static final String CONNECTION_URL = "connection-url";
    public static final String MULTI_COORDINATOR_ENABLED = "hetu.multi-coordinator.enabled";
    public static final String SPLIT_CACHE_MAP_ENABLED = "hetu.split-cache-map.enabled";
    public static final String SPLIT_CACHE_STATE_UPDATE_INTERVAL = "hetu.split-cache-map.state-update-interval";
}
