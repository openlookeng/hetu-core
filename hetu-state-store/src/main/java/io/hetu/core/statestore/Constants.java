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
package io.hetu.core.statestore;

public final class Constants
{
    /**
     * State store seed ip
     */
    public static final String STATE_STORE_CLUSTER_SEED_IP = "state-store.seed-ip";

    /**
     * State store cluster name
     */
    public static final String STATE_STORE_CLUSTER_CONFIG_NAME = "state-store.cluster";

    /**
     * State store encryption config name
     */
    public static final String STATE_STORE_ENCRYPTION_CONFIG_NAME = "state-store.encryption-type";

    private Constants()
    {
    }
}
