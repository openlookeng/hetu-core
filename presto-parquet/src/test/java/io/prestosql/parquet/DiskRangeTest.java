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
package io.prestosql.parquet;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class DiskRangeTest
{
    private DiskRange diskRangeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        diskRangeUnderTest = new DiskRange(0L, 0);
    }

    @Test
    public void testGetEnd()
    {
        assertEquals(0L, diskRangeUnderTest.getEnd());
    }

    @Test
    public void testContains() throws Exception
    {
        // Setup
        final DiskRange diskRange = new DiskRange(0L, 0);

        // Run the test
        final boolean result = diskRangeUnderTest.contains(diskRange);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testSpan()
    {
        // Setup
        final DiskRange otherDiskRange = new DiskRange(0L, 0);
        final DiskRange expectedResult = new DiskRange(0L, 0);

        // Run the test
        final DiskRange result = diskRangeUnderTest.span(otherDiskRange);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testHashCode()
    {
        assertEquals(0, diskRangeUnderTest.hashCode());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(diskRangeUnderTest.equals("obj"));
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", diskRangeUnderTest.toString());
    }
}
