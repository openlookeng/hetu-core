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

package io.hetu.core.plugin.oracle.config;

import java.math.BigDecimal;

/**
 * RoundingMode
 *
 * @since 2019-07-18
 */
public enum RoundingMode
{
    /**
     * enum for unnecessary
     */
    UNNECESSARY(BigDecimal.ROUND_UNNECESSARY),
    /**
     * enum for ceiling
     */
    CEILING(BigDecimal.ROUND_CEILING),
    /**
     * enum for floor
     */
    FLOOR(BigDecimal.ROUND_FLOOR),
    /**
     * enum for half down
     */
    HALF_DOWN(BigDecimal.ROUND_HALF_DOWN),
    /**
     * enum for even
     */
    HALF_EVEN(BigDecimal.ROUND_HALF_EVEN),
    /**
     * enum for half up
     */
    HALF_UP(BigDecimal.ROUND_HALF_UP),
    /**
     * enum for up
     */
    UP(BigDecimal.ROUND_UP),
    /**
     * enum for down
     */
    DOWN(BigDecimal.ROUND_DOWN);

    private final int value;

    RoundingMode(int value)
    {
        this.value = value;
    }

    public int getValue()
    {
        return value;
    }
}
