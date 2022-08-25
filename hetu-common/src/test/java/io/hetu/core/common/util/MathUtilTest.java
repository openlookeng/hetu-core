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
package io.hetu.core.common.util;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MathUtilTest
{
    @Test
    public void testMultiplyHigh()
    {
        assertEquals(0L, MathUtil.multiplyHigh(0L, 0L));
    }

    @Test
    public void testMultiplyHigh2()
    {
        assertEquals(0L, MathUtil.multiplyHigh(-1L, -1L));
    }
}
