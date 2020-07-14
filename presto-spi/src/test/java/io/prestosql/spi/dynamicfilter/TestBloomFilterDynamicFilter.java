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
package io.prestosql.spi.dynamicfilter;

import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.HashSet;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestBloomFilterDynamicFilter
{
    @Test
    public void testDynamicFilterTypeConversion()
    {
        int v1 = 1;
        String v2 = "test";
        long v3 = 2L;
        double v4 = 0.9;
        Slice v5 = utf8Slice("test2");

        HashSet hs = new HashSet();
        hs.add(v1);
        hs.add(v2);
        hs.add(v3);
        hs.add(v4);
        hs.add(v5);
        HashSetDynamicFilter hsdf = new HashSetDynamicFilter("19", null, hs, DynamicFilter.Type.LOCAL);
        BloomFilterDynamicFilter bfdf = BloomFilterDynamicFilter.fromHashSetDynamicFilter(hsdf);

        assertEquals(bfdf.contains(String.valueOf(v1)), true);
        assertEquals(bfdf.contains(String.valueOf(v2)), true);
        assertEquals(bfdf.contains(String.valueOf(v3)), true);
        assertEquals(bfdf.contains(String.valueOf(v4)), true);
        assertEquals(bfdf.contains(new String(v5.getBytes())), true);
        assertEquals(bfdf.contains(String.valueOf(5)), false);
    }
}
