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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class LongTimestampTest
{
    private LongTimestamp longTimestampUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        longTimestampUnderTest = new LongTimestamp(0L, 0);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(longTimestampUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, longTimestampUnderTest.hashCode());
    }

    @Test
    public void testCompareTo() throws Exception
    {
        // Setup
        final LongTimestamp other = new LongTimestamp(0L, 0);

        // Run the test
        final int result = longTimestampUnderTest.compareTo(other);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testCompareTo_ThrowsNullPointerException() throws Exception
    {
        // Setup
        final LongTimestamp other = new LongTimestamp(0L, 0);

        // Run the test
        assertThrows(NullPointerException.class, () -> longTimestampUnderTest.compareTo(other));
    }

    @Test
    public void testCompareTo_ThrowsClassCastException() throws Exception
    {
        // Setup
        final LongTimestamp other = new LongTimestamp(0L, 0);

        // Run the test
        assertThrows(ClassCastException.class, () -> longTimestampUnderTest.compareTo(other));
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", longTimestampUnderTest.toString());
    }
}
