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
package io.prestosql.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.Test;

import java.util.Optional;

public class EncoderUtilTest
{
    @Test
    public void testEncodeNullsAsBits() throws Exception
    {
        // Setup
        final SliceOutput sliceOutput = null;
        final Block block = null;

        // Run the test
        EncoderUtil.encodeNullsAsBits(sliceOutput, block);

        // Verify the results
    }

    @Test
    public void testDecodeNullBits() throws Exception
    {
        // Setup
        final SliceInput sliceInput = null;

        // Run the test
        final Optional<boolean[]> result = EncoderUtil.decodeNullBits(sliceInput, 0);

        // Verify the results
    }
}
