/*
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
package io.prestosql.spi.block;

import io.prestosql.spi.util.BloomFilter;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

public class LongArrayBlockTest
{
    @Test
    public void testFilter()
    {
        testFilter(1000);
        testFilter(10000);
        testFilter(100000);
        testFilter(1000000);
        testFilter(10000000);
    }

    public void testFilter(int size)
    {
        int count = 1024;
        LongArrayBlock block1 = new LongArrayBlock(count, Optional.empty(), getValues(count));
        LongArrayBlock block2 = new LongArrayBlock(count, Optional.empty(), getValues(count));
        BloomFilter bf1 = getBf(size);
        BloomFilter bf2 = getBf(size);

        long total1 = 0;
        long total2 = 0;
        long gtotal = 0;
        long value = 0;
        for (int j = 0; j < 100; j++) {
            boolean[] result1 = new boolean[count];
            boolean[] result2 = new boolean[count];
            Arrays.fill(result1, Boolean.TRUE);
            Arrays.fill(result2, Boolean.TRUE);

            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                long s = System.nanoTime();
                value = block1.get(i);
                gtotal += (System.nanoTime() - s);
                result1[i] = bf1.test(value);
            }
            total1 += System.nanoTime() - start;

            start = System.nanoTime();
            block2.filter(bf2, result2);
            total2 += System.nanoTime() - start;

            for (int i = 0; i < count; i++) {
                if (result1[i] != result2[i]) {
                    throw new RuntimeException("error" + i);
                }
            }
        }

        System.out.println("bfsize: " + size + "  origi: " + total1);
        System.out.println("bfsize: " + size + "  block: " + total2);
    }

    private long[] getValues(int count)
    {
        long[] values = new long[count];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        return values;
    }

    private BloomFilter getBf(int size)
    {
        BloomFilter bf = new BloomFilter(size, 0.01);
        for (int i = 10; i < 100; i++) {
            bf.addLong((long) i);
        }
        return bf;
    }
}
