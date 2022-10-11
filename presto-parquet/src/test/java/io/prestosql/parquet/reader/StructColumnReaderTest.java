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

import io.prestosql.parquet.Field;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class StructColumnReaderTest
{
    @Test
    public void testCalculateStructOffsets()
    {
        // Setup
        final Field field = null;
        final BooleanList expectedResult = null;

        // Run the test
        final BooleanList result = StructColumnReader.calculateStructOffsets(field, new int[]{0}, new int[]{0});

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
