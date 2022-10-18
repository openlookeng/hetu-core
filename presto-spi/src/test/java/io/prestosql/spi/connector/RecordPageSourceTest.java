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
package io.prestosql.spi.connector;

import io.prestosql.spi.Page;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RecordPageSourceTest
{
    @Mock
    private RecordCursor mockCursor;

    private RecordPageSource recordPageSourceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        recordPageSourceUnderTest = new RecordPageSource(Arrays.asList(), mockCursor);
    }

    @Test
    public void testGetMemoryUsage() throws Exception
    {
        // Setup
        when(mockCursor.getSystemMemoryUsage()).thenReturn(0L);

        // Run the test
        final long result = recordPageSourceUnderTest.getMemoryUsage();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetCompletedBytes() throws Exception
    {
        // Setup
        when(mockCursor.getCompletedBytes()).thenReturn(0L);

        // Run the test
        final long result = recordPageSourceUnderTest.getCompletedBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetReadTimeNanos() throws Exception
    {
        // Setup
        when(mockCursor.getReadTimeNanos()).thenReturn(0L);

        // Run the test
        final long result = recordPageSourceUnderTest.getReadTimeNanos();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetSystemMemoryUsage() throws Exception
    {
        // Setup
        when(mockCursor.getSystemMemoryUsage()).thenReturn(0L);

        // Run the test
        final long result = recordPageSourceUnderTest.getSystemMemoryUsage();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        recordPageSourceUnderTest.close();

        // Verify the results
        verify(mockCursor).close();
    }

    @Test
    public void testIsFinished() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = recordPageSourceUnderTest.isFinished();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetNextPage() throws Exception
    {
        // Setup
        when(mockCursor.advanceNextPosition()).thenReturn(false);
        when(mockCursor.isNull(0)).thenReturn(false);
        when(mockCursor.getBoolean(0)).thenReturn(false);
        when(mockCursor.getLong(0)).thenReturn(0L);
        when(mockCursor.getDouble(0)).thenReturn(0.0);
        when(mockCursor.getSlice(0)).thenReturn(null);
        when(mockCursor.getObject(0)).thenReturn("result");

        // Run the test
        final Page result = recordPageSourceUnderTest.getNextPage();

        // Verify the results
    }
}
