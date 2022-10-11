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
package io.prestosql.parquet.reader;

import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class FilteredOffsetIndexTest
{
    @Test
    public void testFilterOffsetIndex()
    {
        // Setup
        final OffsetIndex offsetIndex = null;
        final RowRanges rowRanges = null;

        // Run the test
        final FilteredOffsetIndex result = FilteredOffsetIndex.filterOffsetIndex(offsetIndex, rowRanges, 0L);
        assertEquals(0, result.getPageCount());
        assertEquals(0L, result.getOffset(0));
        assertEquals(0, result.getCompressedPageSize(0));
        assertEquals(0L, result.getFirstRowIndex(0));
        assertEquals(0L, result.getLastRowIndex(0, 0L));
        assertEquals("result", result.toString());
        assertEquals(Arrays.asList(new FilteredOffsetIndex.OffsetRange(0L, 0L)), result.calculateOffsetRanges(0L));
    }
}
