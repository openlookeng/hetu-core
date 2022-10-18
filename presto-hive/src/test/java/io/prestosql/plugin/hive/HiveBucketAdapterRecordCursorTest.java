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
package io.prestosql.plugin.hive;

import io.airlift.slice.Slice;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class HiveBucketAdapterRecordCursorTest
{
    @Mock
    private TypeManager mockTypeManager;
    @Mock
    private RecordCursor mockDelegate;

    private HiveBucketAdapterRecordCursor hiveBucketAdapterRecordCursorUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        List<HiveType> hiveTypes = Arrays.asList(HiveType.HIVE_DOUBLE);
        hiveBucketAdapterRecordCursorUnderTest = new HiveBucketAdapterRecordCursor(
                new int[]{0},
                hiveTypes,
                HiveBucketing.BucketingVersion.BUCKETING_V1,
                0,
                0,
                0,
                mockTypeManager,
                mockDelegate);
    }

    @Test
    public void testGetCompletedBytes()
    {
        // Setup
        when(mockDelegate.getCompletedBytes()).thenReturn(0L);

        // Run the test
        final long result = hiveBucketAdapterRecordCursorUnderTest.getCompletedBytes();
    }

    @Test
    public void testGetType() throws Exception
    {
        // Setup
        when(mockDelegate.getType(0)).thenReturn(null);

        // Run the test
        final Type result = hiveBucketAdapterRecordCursorUnderTest.getType(0);
    }

    @Test
    public void testAdvanceNextPosition()
    {
        // Setup
        when(mockDelegate.advanceNextPosition()).thenReturn(false);
        when(mockDelegate.isNull(0)).thenReturn(false);
        when(mockDelegate.getBoolean(0)).thenReturn(false);
        when(mockDelegate.getLong(0)).thenReturn(0L);
        when(mockDelegate.getDouble(0)).thenReturn(0.0);
        when(mockDelegate.getSlice(0)).thenReturn(null);
        when(mockDelegate.getObject(0)).thenReturn("result");

        // Run the test
        final boolean result = hiveBucketAdapterRecordCursorUnderTest.advanceNextPosition();
    }

    @Test
    public void testGetBoolean()
    {
        // Setup
        when(mockDelegate.getBoolean(0)).thenReturn(false);

        // Run the test
        final boolean result = hiveBucketAdapterRecordCursorUnderTest.getBoolean(0);
    }

    @Test
    public void testGetLong()
    {
        // Setup
        when(mockDelegate.getLong(0)).thenReturn(0L);

        // Run the test
        final long result = hiveBucketAdapterRecordCursorUnderTest.getLong(0);
    }

    @Test
    public void testGetDouble()
    {
        // Setup
        when(mockDelegate.getDouble(0)).thenReturn(0.0);

        // Run the test
        final double result = hiveBucketAdapterRecordCursorUnderTest.getDouble(0);
    }

    @Test
    public void testGetSlice()
    {
        // Setup
        final Slice expectedResult = null;
        when(mockDelegate.getSlice(0)).thenReturn(null);

        // Run the test
        final Slice result = hiveBucketAdapterRecordCursorUnderTest.getSlice(0);
    }

    @Test
    public void testGetObject()
    {
        // Setup
        when(mockDelegate.getObject(0)).thenReturn("result");

        // Run the test
        final Object result = hiveBucketAdapterRecordCursorUnderTest.getObject(0);
    }

    @Test
    public void testIsNull()
    {
        // Setup
        when(mockDelegate.isNull(0)).thenReturn(false);

        // Run the test
        final boolean result = hiveBucketAdapterRecordCursorUnderTest.isNull(0);
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        hiveBucketAdapterRecordCursorUnderTest.close();
    }

    @Test
    public void testGetReadTimeNanos()
    {
        // Setup
        when(mockDelegate.getReadTimeNanos()).thenReturn(0L);

        // Run the test
        final long result = hiveBucketAdapterRecordCursorUnderTest.getReadTimeNanos();
    }

    @Test
    public void testGetSystemMemoryUsage()
    {
        // Setup
        when(mockDelegate.getSystemMemoryUsage()).thenReturn(0L);

        // Run the test
        final long result = hiveBucketAdapterRecordCursorUnderTest.getSystemMemoryUsage();
    }
}
