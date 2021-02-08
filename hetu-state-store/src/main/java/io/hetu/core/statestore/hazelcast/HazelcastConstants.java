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
package io.hetu.core.statestore.hazelcast;

public final class HazelcastConstants
{
    /**
     * Default cluster name for Hazelcast cluster
     */
    public static final String DEFAULT_CLUSTER_ID = "hetu-state-store";

    /**
     * Hazelcast discovery mode config name
     */
    public static final String DISCOVERY_MODE_CONFIG_NAME = "hazelcast.discovery.mode";

    /**
     * Hazelcast discovery port config name
     */
    public static final String DISCOVERY_PORT_CONFIG_NAME = "hazelcast.discovery.port";

    /**
     * Hazelcast default discovery port
     */
    public static final String DEFAULT_DISCOVERY_PORT = "5701";

    /**
     * Hazelcast multicast discovery mode
     */
    public static final String DISCOVERY_MODE_MULTICAST = "multicast";

    /**
     * Hazelcast TCP-IP discovery mode
     */
    public static final String DISCOVERY_MODE_TCPIP = "tcp-ip";

    /**
     * Hazelcast tcp-ip seeds
     */
    public static final String DISCOVERY_TCPIP_SEEDS = "hazelcast.discovery.tcp-ip.seeds";

    /**
     * Minimum member count for Hazelcast CP system
     */
    public static final int MINIMUM_CP_MEMBER_COUNT = 3;

    /**
     * Hazelcast discovery enabled
     */
    public static final String DISCOVERY_ENABLED = "hazelcast.discovery.enabled";

    /**
     * Hazelcast multicast discovery strategy class name
     */

    public static final String DISCOVERY_MULTICAST_STRATEGY_CLASS_NAME =
            "com.hazelcast.spi.discovery.multicast.MulticastDiscoveryStrategy";

    /**
     * Enable hazelcast kerberos authentication
     */
    public static final String KERBEROS_ENABLED = "hazelcast.kerberos.enable";

    /**
     * Hazelcast kerberos login context name
     */
    public static final String KERBEROS_LOGIN_CONTEXT_NAME = "hazelcast.kerberos.login.context.name";

    /**
     * Hazelcast kerberos service principal
     */
    public static final String KERBEROS_SERVICE_PRINCIPAL = "hazelcast.kerberos.service.principal";

    /**
     * Hazelcast kerberos krb5 config file
     */
    public static final String KRB5_CONFIG_FILE = "hazelcast.kerberos.krb5.conf";

    /**
     * Hazelcast kerberos authentication login config
     */
    public static final String JAAS_CONFIG_FILE = "hazelcast.kerberos.auth.login.config";

    /**
     * Enable hazelcast SSL
     */
    public static final String HAZELCAST_SSL_ENABLED = "hazelcast.ssl.enabled";

    /**
     * Hazelcast SSL keystore path
     */
    public static final String SSL_KEYSTORE_PATH = "hazelcast.ssl.keystore.path";

    /**
     * Hazelcast SSL keystore password
     */
    public static final String SSL_KEYSTORE_PASSWORD = "hazelcast.ssl.keystore.password";

    /**
     * Hazelcast SSL truststore path
     */
    public static final String SSL_TRUSTSTORE_PATH = "hazelcast.ssl.truststore.path";

    /**
     * Hazelcast SSL truststore password
     */
    public static final String SSL_TRUSTSTORE_PASSWORD = "hazelcast.ssl.truststore.password";

    /**
     * Hazelcast SSL cipher suites
     */
    public static final String SSL_CIPHER_SUITES = "hazelcast.ssl.cipher.suites";
    /**
     * Hazelcast SSL protocols
     */
    public static final String SSL_PROTOCOLS = "hazelcast.ssl.protocols";

    /**
     * Hazelcast heartbeat interval
     */
    public static final int HEARTBEAT_INTERVAL_SECONDS = 5;

    /**
     * Hazelcast heartbeat timeout
     */
    // !Important change the value if the heartbeat timeout value of Hetu nodes changed
    public static final int HEARTBEAT_TIMEOUT_SECONDS = 10;

    private HazelcastConstants()
    {
    }
}
