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

package io.hetu.core.spi.cube;

public enum CubeStatus
{
    INACTIVE(0),
    READY(1);

    private final int value;

    CubeStatus(int value)
    {
        this.value = value;
    }

    public int getValue()
    {
        return this.value;
    }

    public static CubeStatus forValue(int value)
    {
        if (value == 0) {
            return INACTIVE;
        }
        else if (value == 1) {
            return READY;
        }
        return null;
    }
}
