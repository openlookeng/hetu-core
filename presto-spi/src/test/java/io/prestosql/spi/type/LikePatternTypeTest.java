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

import io.prestosql.spi.block.BlockBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class LikePatternTypeTest
{
    private LikePatternType likePatternTypeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        likePatternTypeUnderTest = new LikePatternType();
    }

    @Test
    public void testGetObjectValue() throws Exception
    {
        assertEquals("result", likePatternTypeUnderTest.getObjectValue(null, null, 0));
    }

    @Test
    public void testAppendTo() throws Exception
    {
        // Setup
        // Run the test
        likePatternTypeUnderTest.appendTo(null, 0, null);

        // Verify the results
    }

    @Test
    public void testCreateBlockBuilder1() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = likePatternTypeUnderTest.createBlockBuilder(null, 0, 0);

        // Verify the results
    }

    @Test
    public void testCreateBlockBuilder2() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = likePatternTypeUnderTest.createBlockBuilder(null, 0);

        // Verify the results
    }
}
