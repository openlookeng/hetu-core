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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class DictionaryIdTest
{
    private DictionaryId dictionaryIdUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        dictionaryIdUnderTest = new DictionaryId(0L, 0L, 0L);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(dictionaryIdUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, dictionaryIdUnderTest.hashCode());
    }

    @Test
    public void testRandomDictionaryId() throws Exception
    {
        // Run the test
        final DictionaryId result = DictionaryId.randomDictionaryId();
        assertEquals(0L, result.getMostSignificantBits());
        assertEquals(0L, result.getLeastSignificantBits());
        assertEquals(0L, result.getSequenceId());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
    }
}
