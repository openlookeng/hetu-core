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
        long l = SizeOfUtil.sizeOf(new boolean[]{false});
        assertEquals(l, 17);
    }

    @Test
    public void testSizeOf2()
    {
        long l = SizeOfUtil.sizeOf("content".getBytes(StandardCharsets.UTF_8));
        assertEquals(l, 23);
    }

    @Test
    public void testSizeOf3()
    {
        long l = SizeOfUtil.sizeOf(new short[]{(short) 0});
        assertEquals(l, 18);
    }

    @Test
    public void testSizeOf4()
    {
        long l = SizeOfUtil.sizeOf(new char[]{'a'});
        assertEquals(l, 18);
    }

    @Test
    public void testSizeOf5()
    {
        long l = SizeOfUtil.sizeOf(new int[]{0});
        assertEquals(l, 20);
    }

    @Test
    public void testSizeOf6()
    {
        long l = SizeOfUtil.sizeOf(new long[]{0L});
        assertEquals(l, 24);
    }

    @Test
    public void testSizeOf7()
    {
        long l = SizeOfUtil.sizeOf(new float[]{0.0f});
        assertEquals(l, 20);
    }

    @Test
    public void testSizeOf8()
    {
        long l = SizeOfUtil.sizeOf(new double[]{0.0});
        assertEquals(l, 24);
    }

    @Test
    public void testSizeOf9()
    {
        long l = SizeOfUtil.sizeOf(new Object[]{"array"});
        assertEquals(l, 20);
    }

    @Test
    public void testSizeOf10()
    {
        long l = SizeOfUtil.sizeOf(false);
        assertEquals(l, 16);
    }

    @Test
    public void testSizeOf11()
    {
        long l = SizeOfUtil.sizeOf((byte) 0b0);
        assertEquals(l, 16);
    }

    @Test
    public void testSizeOf12()
    {
        long l = SizeOfUtil.sizeOf((short) 0);
        assertEquals(l, 16);
    }

    @Test
    public void testSizeOf13()
    {
        long a = SizeOfUtil.sizeOf('a');
        assertEquals(a, 16);
    }

    @Test
    public void testSizeOf14()
    {
        long l = SizeOfUtil.sizeOf(0);
        assertEquals(l, 16);
    }

    @Test
    public void testSizeOf15()
    {
        long l = SizeOfUtil.sizeOf(0L);
        assertEquals(l, 24);
    }

    @Test
    public void testSizeOf16()
    {
        long l = SizeOfUtil.sizeOf(0.0f);
        assertEquals(l, 16);
    }

    @Test
    public void testSizeOf17()
    {
        long l = SizeOfUtil.sizeOf(0.0);
        assertEquals(l, 24);
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
        assertEquals(24, result);
    }

    @Test
    public void testSizeOf20()
    {
        // Setup
        final OptionalLong optional = OptionalLong.of(0);

        // Run the test
        final long result = SizeOfUtil.sizeOf(optional);
        assertEquals(24, result);
    }

    @Test
    public void testSizeOf21()
    {
        // Setup
        final OptionalDouble optional = OptionalDouble.of(0.0);

        // Run the test
        final long result = SizeOfUtil.sizeOf(optional);
        assertEquals(24, result);
    }

    @Test
    public void testEstimatedSizeOf1()
    {
        long string = SizeOfUtil.estimatedSizeOf("string");
        assertEquals(36, string);
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
        long l = SizeOfUtil.estimatedSizeOf(list, valueSize);
        assertEquals(16, l);
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
        long l = SizeOfUtil.estimatedSizeOf(set, valueSize);
        assertEquals(16, l);
    }

    @Test
    public void testSizeOfBooleanArray()
    {
        long l = SizeOfUtil.sizeOfBooleanArray(0);
        assertEquals(16, l);
    }

    @Test
    public void testSizeOfByteArray()
    {
        long l = SizeOfUtil.sizeOfByteArray(0);
        assertEquals(16, l);
    }

    @Test
    public void testSizeOfShortArray()
    {
        long l = SizeOfUtil.sizeOfShortArray(0);
        assertEquals(16, l);
    }

    @Test
    public void testSizeOfCharArray()
    {
        long l = SizeOfUtil.sizeOfCharArray(0);
        assertEquals(16, l);
    }

    @Test
    public void testSizeOfIntArray()
    {
        long l = SizeOfUtil.sizeOfIntArray(0);
        assertEquals(16, l);
    }

    @Test
    public void testSizeOfLongArray()
    {
        long l = SizeOfUtil.sizeOfLongArray(0);
        assertEquals(16, l);
    }

    @Test
    public void testSizeOfFloatArray()
    {
        long l = SizeOfUtil.sizeOfFloatArray(0);
        assertEquals(16, l);
    }

    @Test
    public void testSizeOfDoubleArray()
    {
        long l = SizeOfUtil.sizeOfDoubleArray(0);
        assertEquals(16, l);
    }

    @Test
    public void testSizeOfObjectArray()
    {
        long l = SizeOfUtil.sizeOfObjectArray(0);
        assertEquals(16, l);
    }
}
