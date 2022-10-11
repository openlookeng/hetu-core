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
package io.prestosql.parquet.writer.repdef;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.ColumnarArray;
import io.prestosql.spi.block.ColumnarMap;
import io.prestosql.spi.block.ColumnarRow;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DefLevelIterablesTest
{
    @Test
    public void testOf1()
    {
        // Setup
        final Block block = null;

        // Run the test
        final DefLevelIterable result = DefLevelIterables.of(block, 0);

        // Verify the results
    }

    @Test
    public void testOf2()
    {
        // Setup
        final ColumnarRow columnarRow = ColumnarRow.toColumnarRow(null);

        // Run the test
        final DefLevelIterable result = DefLevelIterables.of(columnarRow, 0);

        // Verify the results
    }

    @Test
    public void testOf3()
    {
        // Setup
        final ColumnarArray columnarArray = ColumnarArray.toColumnarArray(null);

        // Run the test
        final DefLevelIterable result = DefLevelIterables.of(columnarArray, 0);

        // Verify the results
    }

    @Test
    public void testOf4()
    {
        // Setup
        final ColumnarMap columnarMap = ColumnarMap.toColumnarMap(null);

        // Run the test
        final DefLevelIterable result = DefLevelIterables.of(columnarMap, 0);

        // Verify the results
    }

    @Test
    public void testGetIterator()
    {
        // Setup
        final List<DefLevelIterable> iterables = Arrays.asList();

        // Run the test
        final Iterator<Integer> result = DefLevelIterables.getIterator(iterables);

        // Verify the results
    }
}
