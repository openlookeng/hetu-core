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

import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TimestampColumnReaderTest
{
    @Mock
    private RichColumnDescriptor mockDescriptor;
    @Mock
    private DateTimeZone mockTimeZone;

    private TimestampColumnReader timestampColumnReaderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        timestampColumnReaderUnderTest = new TimestampColumnReader(mockDescriptor, mockTimeZone);
    }

    @Test
    public void testReadValue() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;
        final Type type = null;
        when(mockDescriptor.getMaxDefinitionLevel()).thenReturn(0);
        when(mockTimeZone.convertUTCToLocal(0L)).thenReturn(0L);
        when(mockDescriptor.isRequired()).thenReturn(false);

        // Run the test
        timestampColumnReaderUnderTest.readValue(blockBuilder, type);

        // Verify the results
    }

    @Test
    public void testSkipValue()
    {
        // Setup
        when(mockDescriptor.getMaxDefinitionLevel()).thenReturn(0);

        // Run the test
        timestampColumnReaderUnderTest.skipValue();

        // Verify the results
    }
}
