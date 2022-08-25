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
package io.prestosql.parquet.dictionary;

import io.prestosql.parquet.DictionaryPage;
import org.apache.parquet.io.api.Binary;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class BinaryDictionaryTest
{
    @Mock
    private DictionaryPage mockDictionaryPage;

    private BinaryDictionary binaryDictionaryUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        binaryDictionaryUnderTest = new BinaryDictionary(mockDictionaryPage, 0);
    }

    @Test
    public void testDecodeToBinary()
    {
        // Setup
        final Binary expectedResult = Binary.fromReusedByteArray("content".getBytes());

        // Run the test
        final Binary result = binaryDictionaryUnderTest.decodeToBinary(0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", binaryDictionaryUnderTest.toString());
    }
}
