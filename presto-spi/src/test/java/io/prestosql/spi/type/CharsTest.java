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

public class CharsTest
{
    @Test
    public void testIsCharType() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final boolean result = Chars.isCharType(type);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testPadSpaces1() throws Exception
    {
        // Setup
        final Slice slice = null;
        final CharType charType = CharType.createCharType(0L);
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Chars.padSpaces(slice, charType);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testPadSpaces2() throws Exception
    {
        // Setup
        final Slice slice = null;
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Chars.padSpaces(slice, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testTruncateToLengthAndTrimSpaces1() throws Exception
    {
        // Setup
        final Slice slice = null;
        final Type type = null;
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Chars.truncateToLengthAndTrimSpaces(slice, type);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testTruncateToLengthAndTrimSpaces2() throws Exception
    {
        // Setup
        final Slice slice = null;
        final CharType charType = CharType.createCharType(0L);
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Chars.truncateToLengthAndTrimSpaces(slice, charType);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testTruncateToLengthAndTrimSpaces3() throws Exception
    {
        // Setup
        final Slice slice = null;
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Chars.truncateToLengthAndTrimSpaces(slice, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testTrimTrailingSpaces() throws Exception
    {
        // Setup
        final Slice slice = null;
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Chars.trimTrailingSpaces(slice);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testByteCountWithoutTrailingSpace1() throws Exception
    {
        // Setup
        final Slice slice = null;

        // Run the test
        final int result = Chars.byteCountWithoutTrailingSpace(slice, 0, 0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testByteCountWithoutTrailingSpace2() throws Exception
    {
        // Setup
        final Slice slice = null;

        // Run the test
        final int result = Chars.byteCountWithoutTrailingSpace(slice, 0, 0, 0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testCompareChars() throws Exception
    {
        // Setup
        final Slice left = null;
        final Slice right = null;

        // Run the test
        final int result = Chars.compareChars(left, right);

        // Verify the results
        assertEquals(0, result);
    }
}
