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
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class HiveCoercionRecordCursorTest
{
    @Mock
    private TypeManager mockTypeManager;
    @Mock
    private RecordCursor mockDelegate;

    private HiveCoercionRecordCursor hiveCoercionRecordCursorUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hiveCoercionRecordCursorUnderTest = new HiveCoercionRecordCursor(Arrays.asList(
                HivePageSourceProvider.ColumnMapping.interim(
                        new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                                new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                                HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false), 0)),
                mockTypeManager, mockDelegate);
    }

    @Test
    public void testGetCompletedBytes() throws Exception
    {
        final long result = hiveCoercionRecordCursorUnderTest.getCompletedBytes();
    }

    @Test
    public void testGetType() throws Exception
    {
        final Type result = hiveCoercionRecordCursorUnderTest.getType(0);

        // Verify the results
    }

    @Test
    public void testAdvanceNextPosition()
    {
        final boolean result = hiveCoercionRecordCursorUnderTest.advanceNextPosition();
    }

    @Test
    public void testGetBoolean()
    {
        final boolean result = hiveCoercionRecordCursorUnderTest.getBoolean(0);
    }

    @Test
    public void testGetLong()
    {
        // Run the test
        final long result = hiveCoercionRecordCursorUnderTest.getLong(0);
    }

    @Test
    public void testGetDouble()
    {
        final double result = hiveCoercionRecordCursorUnderTest.getDouble(0);
    }

    @Test
    public void testGetSlice()
    {
        final Slice result = hiveCoercionRecordCursorUnderTest.getSlice(0);
    }

    @Test
    public void testGetObject()
    {
        final Object result = hiveCoercionRecordCursorUnderTest.getObject(0);

        // Verify the results
    }

    @Test
    public void testIsNull()
    {
        final boolean result = hiveCoercionRecordCursorUnderTest.isNull(0);
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        hiveCoercionRecordCursorUnderTest.close();
    }

    @Test
    public void testGetReadTimeNanos()
    {
        final long result = hiveCoercionRecordCursorUnderTest.getReadTimeNanos();
    }

    @Test
    public void testGetSystemMemoryUsage() throws Exception
    {
        final long result = hiveCoercionRecordCursorUnderTest.getSystemMemoryUsage();
    }

    @Test
    public void testGetRegularColumnRecordCursor()
    {
        // Setup
        // Run the test
        final RecordCursor result = hiveCoercionRecordCursorUnderTest.getRegularColumnRecordCursor();

        // Verify the results
    }
}
