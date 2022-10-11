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
package io.prestosql.spi.statistics;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ComputedStatisticsTest
{
    @Test
    public void testBuilder() throws Exception
    {
        // Setup
        final List<Block> groupingValues = Arrays.asList();

        // Run the test
        final ComputedStatistics.Builder result = ComputedStatistics.builder(Arrays.asList("value"), groupingValues);

        // Verify the results
    }

    @Test
    public void testRestoreComputedStatistics() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        final ComputedStatistics result = ComputedStatistics.restoreComputedStatistics("state", serdeProvider);
        assertEquals(Arrays.asList("value"), result.getGroupingColumns());
        assertEquals(Arrays.asList(), result.getGroupingValues());
        assertEquals(new HashMap<>(), result.getTableStatistics());
        assertEquals(new HashMap<>(), result.getColumnStatistics());
        final BlockEncodingSerdeProvider serdeProvider1 = null;
        assertEquals("result", result.capture(serdeProvider1));
        assertTrue(result.supportsConsolidatedWrites());
        assertEquals(0L, result.getUsedMemory());
    }
}
