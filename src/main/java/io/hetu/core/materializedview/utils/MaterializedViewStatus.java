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

package io.hetu.core.materializedview.utils;

/**
 * Mv status
 *
 * @since 2020-03-28
 */
public enum MaterializedViewStatus
{
    /**
     * enable: mv is enable to use
     */
    ENABLE,
    /**
     * disable: mv is creating or refreshing, it can't be used
     */
    DISABLE,
    /**
     * UNKNOWN: unknown status, it will throw exception
     */
    UNKNOWN;

    /**
     * get status string from status
     *
     * @param status status
     * @return status string
     */
    public static String getStatusString(MaterializedViewStatus status)
    {
        switch (status) {
            case ENABLE:
                return "ENABLE";
            case DISABLE:
                return "DISABLE";
            default:
                return "UNKNOWN";
        }
    }

    /**
     * get Status from string
     *
     * @param status status string
     * @return status
     */
    public static MaterializedViewStatus getStatusFromString(String status)
    {
        switch (status) {
            case "ENABLE":
                return ENABLE;
            case "DISABLE":
                return DISABLE;
            default:
                return UNKNOWN;
        }
    }
}
