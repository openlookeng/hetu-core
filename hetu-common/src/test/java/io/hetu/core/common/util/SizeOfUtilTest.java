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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.ToLongFunction;

import static org.testng.Assert.assertEquals;

public class SizeOfUtilTest<T>
{
    @Test
    public void testSizeOf1()
    {
        SizeOfUtil.sizeOf(new boolean[]{false});
    }

    @Test
    public void testSizeOf2()
    {
        SizeOfUtil.sizeOf("content".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testSizeOf3()
    {
        SizeOfUtil.sizeOf(new short[]{(short) 0});
    }

    @Test
    public void testSizeOf4()
    {
        SizeOfUtil.sizeOf(new char[]{'a'});
    }

    @Test
    public void testSizeOf5()
    {
        SizeOfUtil.sizeOf(new int[]{0});
    }

    @Test
    public void testSizeOf6()
    {
        SizeOfUtil.sizeOf(new long[]{0L});
    }

    @Test
    public void testSizeOf7()
    {
        SizeOfUtil.sizeOf(new float[]{0.0f});
    }

    @Test
    public void testSizeOf8()
    {
        SizeOfUtil.sizeOf(new double[]{0.0});
    }

    @Test
    public void testSizeOf9()
    {
        SizeOfUtil.sizeOf(new Object[]{"array"});
    }

    @Test
    public void testSizeOf10()
    {
        SizeOfUtil.sizeOf(false);
    }

    @Test
    public void testSizeOf11()
    {
        SizeOfUtil.sizeOf((byte) 0b0);
    }

    @Test
    public void testSizeOf12()
    {
        SizeOfUtil.sizeOf((short) 0);
    }

    @Test
    public void testSizeOf13()
    {
        SizeOfUtil.sizeOf('a');
    }

    @Test
    public void testSizeOf14()
    {
        SizeOfUtil.sizeOf(0);
    }

    @Test
    public void testSizeOf15()
    {
        SizeOfUtil.sizeOf(0L);
    }

    @Test
    public void testSizeOf16()
    {
        SizeOfUtil.sizeOf(0.0f);
    }

    @Test
    public void testSizeOf17()
    {
        SizeOfUtil.sizeOf(0.0);
    }

    @Test
    public void testSizeOf18()
    {
        // Setup
        final Optional<T> optional = Optional.empty();
        final ToLongFunction<T> valueSize = val -> {
            return 0L;
        };

        // Run the test
        final long result = SizeOfUtil.sizeOf(optional, valueSize);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testSizeOf19()
    {
        // Setup
        final OptionalInt optional = OptionalInt.of(0);

        // Run the test
        final long result = SizeOfUtil.sizeOf(optional);
    }

    @Test
    public void testSizeOf20()
    {
        // Setup
        final OptionalLong optional = OptionalLong.of(0);

        // Run the test
        final long result = SizeOfUtil.sizeOf(optional);
    }

    @Test
    public void testSizeOf21()
    {
        // Setup
        final OptionalDouble optional = OptionalDouble.of(0.0);

        // Run the test
        final long result = SizeOfUtil.sizeOf(optional);
    }

    @Test
    public void testEstimatedSizeOf1()
    {
        SizeOfUtil.estimatedSizeOf("string");
    }

    @Test
    public void testEstimatedSizeOf2()
    {
        // Setup
        final List<T> list = Arrays.asList();
        final ToLongFunction<T> valueSize = val -> {
            return 0L;
        };

        // Run the test
        SizeOfUtil.estimatedSizeOf(list, valueSize);
    }

    @Test
    public void testEstimatedSizeOf3()
    {
        // Setup
        final Set<T> set = new HashSet<>();
        final ToLongFunction<T> valueSize = val -> {
            return 0L;
        };

        // Run the test
        SizeOfUtil.estimatedSizeOf(set, valueSize);
    }

    @Test
    public void testSizeOfBooleanArray()
    {
        SizeOfUtil.sizeOfBooleanArray(0);
    }

    @Test
    public void testSizeOfByteArray()
    {
        SizeOfUtil.sizeOfByteArray(0);
    }

    @Test
    public void testSizeOfShortArray()
    {
        SizeOfUtil.sizeOfShortArray(0);
    }

    @Test
    public void testSizeOfCharArray()
    {
        SizeOfUtil.sizeOfCharArray(0);
    }

    @Test
    public void testSizeOfIntArray()
    {
        SizeOfUtil.sizeOfIntArray(0);
    }

    @Test
    public void testSizeOfLongArray()
    {
        SizeOfUtil.sizeOfLongArray(0);
    }

    @Test
    public void testSizeOfFloatArray()
    {
        SizeOfUtil.sizeOfFloatArray(0);
    }

    @Test
    public void testSizeOfDoubleArray()
    {
        SizeOfUtil.sizeOfDoubleArray(0);
    }

    @Test
    public void testSizeOfObjectArray()
    {
        SizeOfUtil.sizeOfObjectArray(0);
    }
}
