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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.util.BloomFilter;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;

public class VariableWidthBlockTest
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
        boolean[] valid = new boolean[count];
        Arrays.fill(valid, Boolean.TRUE);
        VariableWidthBlock block = getBlock();
        String[] values = new String[block.getPositionCount()];

        BloomFilter bf1 = getBf(size);
        BloomFilter bf2 = getBf(size);
        for (int i = 0; i < block.getPositionCount(); i++) {
            values[i] = block.getString(i, 0, 0);
        }
        for (int j = 0; j < 10; j++) {
            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                bf1.test(block.getString(i, 0, 0).getBytes());
            }
            System.out.println("original: " + (System.nanoTime() - start));

            start = System.nanoTime();
            block.filter(bf2, valid);
            System.out.println("   block: " + (System.nanoTime() - start));
        }
    }

    private VariableWidthBlock getBlock()
    {
        int count = 1024;
        Random rnd = new Random();

        //returns test data
        int[] offsets = new int[count + 1];
        int offset = 0;
        StringBuilder buffer = new StringBuilder();

        for (int i = 0; i < count; i++) {
            offsets[i + 1] = offset;
            String value = "value" + rnd.nextLong();
            buffer.append(value);
            offset += value.getBytes(StandardCharsets.UTF_8).length;
        }
        Slice slice = Slices.wrappedBuffer(buffer.toString().getBytes(StandardCharsets.UTF_8));

        return new VariableWidthBlock(count, slice, offsets, Optional.empty());
    }

    private BloomFilter getBf(int size)
    {
        Random rnd = new Random();

        BloomFilter bf = new BloomFilter(size, 0.01);
        for (int i = 0; i < 100; i++) {
            bf.test(("value" + rnd.nextLong()).getBytes());
        }
        return bf;
    }
}
