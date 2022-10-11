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
import io.prestosql.spi.type.Type;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class BinaryValueWriterTest
{
    @Mock
    private ValuesWriter mockValuesWriter;
    @Mock
    private Type mockType;

    private BinaryValueWriter binaryValueWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        binaryValueWriterUnderTest = new BinaryValueWriter(mockValuesWriter, mockType,
                new PrimitiveType(org.apache.parquet.schema.Type.Repetition.REQUIRED,
                        PrimitiveType.PrimitiveTypeName.INT64, 0, "name"));
    }

    @Test
    public void testWrite()
    {
        // Setup
        final Block block = null;
        when(mockType.getSlice(any(Block.class), eq(0))).thenReturn(null);

        // Run the test
        binaryValueWriterUnderTest.write(block);

        // Verify the results
        verify(mockValuesWriter).writeBytes(Binary.fromReusedByteArray("content".getBytes(StandardCharsets.UTF_8)));
    }
}
