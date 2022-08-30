/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.exchange.filesystem.checksum;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class GuavaHasherTest
{
    @Test
    public void testContinuousUpdate()
    {
        HashFunction function = Hashing.murmur3_128();
        Hasher hasher1 = function.newHasher();
        Hasher hasher2 = function.newHasher();
        hasher1.putBytes(new byte[]{1, 2, 3});
        hasher1.putBytes(new byte[]{4, 5, 6});
        hasher2.putBytes(new byte[]{1, 2, 3, 4, 5, 6});
        assertEquals(hasher1.hash().toString(), hasher2.hash().toString());
    }

    @Test
    public void testPerformance()
    {
        HashFunction function = Hashing.murmur3_128();
        Hasher hasher = function.newHasher();
        String data = "1234567890";
        byte[] bytes = data.getBytes(UTF_8);
        for (int i = 0; i < 1024 * 1024 * 100; i++) { // 1000MB
            hasher.putBytes(bytes);
        }
        System.out.println(hasher.hash().toString());
    }
}
