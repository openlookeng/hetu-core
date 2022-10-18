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
package io.prestosql.spi.type;

import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class VarcharsTest
{
    @Test
    public void testIsVarcharType() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final boolean result = Varchars.isVarcharType(type);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testTruncateToLength1() throws Exception
    {
        // Setup
        final Slice slice = null;
        final Type type = null;
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Varchars.truncateToLength(slice, type);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testTruncateToLength2() throws Exception
    {
        // Setup
        final Slice slice = null;
        final VarcharType varcharType = VarcharType.createVarcharType(0);
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Varchars.truncateToLength(slice, varcharType);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testTruncateToLength3()
    {
        // Setup
        final Slice slice = null;
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Varchars.truncateToLength(slice, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testByteCount() throws Exception
    {
        // Setup
        final Slice slice = null;

        // Run the test
        final int result = Varchars.byteCount(slice, 0, 0, 0);

        // Verify the results
        assertEquals(0, result);
    }
}
