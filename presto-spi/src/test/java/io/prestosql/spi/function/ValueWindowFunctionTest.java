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
package io.prestosql.spi.function;

import io.prestosql.spi.block.BlockBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ValueWindowFunctionTest
{
    private ValueWindowFunction valueWindowFunctionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        valueWindowFunctionUnderTest = new ValueWindowFunction() {
            @Override
            public void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition)
            {
            }

            @Override
            public boolean supportsConsolidatedWrites()
            {
                return false;
            }

            @Override
            public long getUsedMemory()
            {
                return 0;
            }
        };
    }

    @Test
    public void testReset1() throws Exception
    {
        // Setup
        final WindowIndex windowIndex = null;

        // Run the test
        valueWindowFunctionUnderTest.reset(windowIndex);

        // Verify the results
    }

    @Test
    public void testProcessRow1()
    {
        // Setup
        final BlockBuilder output = null;

        // Run the test
        valueWindowFunctionUnderTest.processRow(output, 0, 0, 0, 0);

        // Verify the results
    }

    @Test
    public void testReset2() throws Exception
    {
        // Setup
        // Run the test
        valueWindowFunctionUnderTest.reset();

        // Verify the results
    }

    @Test
    public void testCapture() throws Exception
    {
        assertEquals("currentPosition", valueWindowFunctionUnderTest.capture(null));
    }

    @Test
    public void testRestore() throws Exception
    {
        // Setup
        // Run the test
        valueWindowFunctionUnderTest.restore("state", null);

        // Verify the results
    }
}
