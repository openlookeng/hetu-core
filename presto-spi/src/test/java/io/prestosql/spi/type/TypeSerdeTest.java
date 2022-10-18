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

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

public class TypeSerdeTest
{
    @Test
    public void testWriteType1() throws Exception
    {
        // Setup
        final SliceOutput sliceOutput = null;
        final Type type = null;

        // Run the test
        TypeSerde.writeType(sliceOutput, type);

        // Verify the results
    }

    @Test
    public void testReadType1() throws Exception
    {
        // Setup
        final TypeManager typeManager = null;
        final SliceInput sliceInput = null;

        // Run the test
        final Type result = TypeSerde.readType(typeManager, sliceInput);

        // Verify the results
    }

    @Test
    public void testWriteType2() throws Exception
    {
        // Setup
        final Output output = new Output(0, 0);
        final Type type = null;

        // Run the test
        TypeSerde.writeType(output, type);

        // Verify the results
    }

    @Test
    public void testReadType2() throws Exception
    {
        // Setup
        final TypeManager typeManager = null;
        final Input sliceInput = new Input("content".getBytes(StandardCharsets.UTF_8), 0, 0);

        // Run the test
        final Type result = TypeSerde.readType(typeManager, sliceInput);

        // Verify the results
    }
}
