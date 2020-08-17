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
package io.hetu.core.plugin.heuristicindex.datasource.hive;

/**
 * Class that holds constant values for creating external index
 */
public final class ConstantsHelper
{
    /**
     * The HDFS authentication type property key
     */
    public static final String HDFS_AUTHENTICATION_TYPE = "hive.hdfs.authentication.type";

    /**
     * Kerberos authentication
     */
    public static final String HDFS_AUTHENTICATION_KERBEROS = "KERBEROS";

    /**
     * The HDFS kerberos principal property key
     */
    public static final String HDFS_AUTH_PRINCIPLE = "hive.hdfs.presto.principal";

    /**
     * The HDFS kerberos keytab property key
     */
    public static final String HDFS_KEYTAB_FILEPATH = "hive.hdfs.presto.keytab";

    /**
     * Max rows per split property name for HdfsDataSource
     */
    public static final String HDFS_SOURCE_CONCURRENCY = "hetu.heuristicindex.filter.datasource.hdfs.maxthreads";

    /**
     * The HDFS config resource property key
     */
    public static final String HIVE_CONFIG_RESOURCES = "hive.config.resources";

    /**
     * The authentication type of hive maetastore
     */
    public static final String HIVE_METASTORE_AUTH_TYPE = "hive.metastore.authentication.type";

    /**
     * The Hive metastore krb5 path property key
     */
    public static final String HIVE_METASTORE_KRB5_CONF = "hive.metastore.krb5.conf.path";

    /**
     * The Hive metastore principal property key
     */
    public static final String HIVE_METASTORE_SERVICE_PRINCIPAL = "hive.metastore.service.principal";

    /**
     * The Hive metastore uri property key
     */
    public static final String HIVE_METASTORE_URI = "hive.metastore.uri";

    /**
     * The Hive wire-encryption toggle property key
     */
    public static final String HIVE_WIRE_ENCRYPTION_ENABLED = "hive.hdfs.wire-encryption.enabled";

    /**
     * The krb5 path property key
     */
    public static final String KRB5_CONF_KEY = "java.security.krb5.conf";

    /**
     * If connection to Hive metastore should use SSL.
     */
    public static final String HIVE_METASTORE_SSL_ENABLED = "hive.metastore.thrift.client.ssl.enabled";

    private ConstantsHelper()
    {
    }
}
