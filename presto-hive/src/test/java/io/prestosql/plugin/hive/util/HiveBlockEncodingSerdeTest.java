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
package io.prestosql.plugin.hive.util;

import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class HiveBlockEncodingSerdeTest
{
    private HiveBlockEncodingSerde hiveBlockEncodingSerdeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        hiveBlockEncodingSerdeUnderTest = new HiveBlockEncodingSerde();
    }

    @Test
    public void testReadBlock()
    {
        // Setup
        final SliceInput input = new InputStreamSliceInput(new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)));

        // Run the test
        final Block result = hiveBlockEncodingSerdeUnderTest.readBlock(input);

        // Verify the results
    }

    @Test
    public void testWriteBlock()
    {
        // Setup
        final SliceOutput output = null;
        final Block block = null;

        // Run the test
        hiveBlockEncodingSerdeUnderTest.writeBlock(output, block);

        // Verify the results
    }

    @Test
    public void testReadType()
    {
        // Setup
        final SliceInput sliceInput = null;

        // Run the test
        final Type result = hiveBlockEncodingSerdeUnderTest.readType(sliceInput);

        // Verify the results
    }

    @Test
    public void testWriteType()
    {
        // Setup
        final SliceOutput sliceOutput = null;
        final Type type = null;

        // Run the test
        hiveBlockEncodingSerdeUnderTest.writeType(sliceOutput, type);

        // Verify the results
    }
}
