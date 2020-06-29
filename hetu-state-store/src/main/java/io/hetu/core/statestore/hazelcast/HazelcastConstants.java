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
     * Hazelcast TCP-IP port config name
     */
    public static final String PORT_CONFIG_NAME = "hazelcast.tcp-ip.port";

    /**
     * Hazelcast multicast discovery mode
     */
    public static final String DISCOVERY_MODE_MULTICAST = "multicast";

    /**
     * Hazelcast TCP-IP discovery mode
     */
    public static final String DISCOVERY_MODE_TCPIP = "tcp-ip";

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

    private HazelcastConstants()
    {
    }
}
