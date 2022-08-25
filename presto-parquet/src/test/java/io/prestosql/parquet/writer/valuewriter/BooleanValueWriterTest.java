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
package io.prestosql.parquet.writer.valuewriter;

import io.prestosql.spi.block.Block;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class BooleanValueWriterTest
{
    @Mock
    private ValuesWriter mockValuesWriter;

    private BooleanValueWriter booleanValueWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        booleanValueWriterUnderTest = new BooleanValueWriter(mockValuesWriter, new PrimitiveType(
                Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"));
    }

    @Test
    public void testWrite()
    {
        // Setup
        final Block block = null;

        // Run the test
        booleanValueWriterUnderTest.write(block);

        // Verify the results
        verify(mockValuesWriter).writeBoolean(false);
    }
}
