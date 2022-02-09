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
package io.prestosql.spi.util;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBloomFilter
{
    private final String[] values;
    private final String[] testValues;
    private static final int COUNT = 1024 * 1024;

    public TestBloomFilter()
    {
        Random rnd = new Random();
        values = new String[COUNT];
        testValues = new String[COUNT];
        for (int i = 0; i < COUNT; i++) {
            values[i] = "item " + i;
            testValues[i] = "item " + rnd.nextInt(1024 * 1024);
        }
    }

    @Test
    public void testMerge()
    {
        BloomFilter pbf1 = new BloomFilter(COUNT, 0.1);
        BloomFilter pbf2 = new BloomFilter(COUNT, 0.1);
        BloomFilter pbf3 = new BloomFilter(COUNT, 0.1);

        for (int i = 0; i < COUNT / 3; i++) {
            pbf1.add(values[i].getBytes());
        }

        for (int i = (int) Math.ceil(COUNT / 3); i < 2 * COUNT / 3; i++) {
            pbf2.add(values[i].getBytes());
        }

        for (int i = (int) Math.ceil(2 * COUNT / 3); i < COUNT; i++) {
            pbf3.add(values[i].getBytes());
        }

        System.out.println("approximate count before merging: " + pbf1.approximateElementCount());

        BloomFilter pbf = pbf1;
        pbf1.merge(pbf2);
        pbf1.merge(pbf3);

        long testStart = System.nanoTime();
        for (int i = 0; i < COUNT; i++) {
            if (!pbf.test(values[i].getBytes())) {
                throw new RuntimeException("error");
            }
        }
        System.out.println("Time testing 1M values: " + (System.nanoTime() - testStart) / 1000000 + " ms");

        int negativeCount = 0;
        for (int i = 0; i < COUNT; i++) {
            if (!pbf.test(("abc" + i).getBytes())) {
                negativeCount++;
            }
        }

        System.out.println("negativeCount: " + negativeCount + ", real fpp: " + (double) (COUNT - negativeCount) / COUNT + ", expected fpp: " + pbf.expectedFpp());
        System.out.println("approximate count after merging: " + pbf.approximateElementCount());
    }

    @Test
    public void testSerDe()
            throws IOException
    {
        BloomFilter bloomFilter = new BloomFilter(COUNT, 0.1);
        for (String value : values) {
            bloomFilter.add(value.getBytes(StandardCharsets.UTF_8));
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long serializationStart = System.nanoTime();
        bloomFilter.writeTo(out);
        System.out.println("Serialization 1M values took: " + (System.nanoTime() - serializationStart) / 1000000 + " ms");

        long deserializationStart = System.nanoTime();
        BloomFilter deserializedBloomFilter = BloomFilter.readFrom(new ByteArrayInputStream(out.toByteArray()));
        System.out.println("Deserialization 1M values took: " + (System.nanoTime() - deserializationStart) / 1000000 + " ms");

        for (String value : values) {
            assertTrue(deserializedBloomFilter.test(value.getBytes(StandardCharsets.UTF_8)), "Value should exist in deserialized BloomFilter");
        }

        BloomFilter bloomFilter1 = new BloomFilter(COUNT, 0.01);
        assertTrue(bloomFilter1.isEmpty());
        bloomFilter1.add(0L);
        assertFalse(bloomFilter1.isEmpty());
        ByteArrayOutputStream out1 = new ByteArrayOutputStream();
        bloomFilter1.writeTo(out1);

        BloomFilter deserializedBloomFilter1 = BloomFilter.readFrom(new ByteArrayInputStream(out1.toByteArray()));
        assertFalse(deserializedBloomFilter1.isEmpty());
    }
}
