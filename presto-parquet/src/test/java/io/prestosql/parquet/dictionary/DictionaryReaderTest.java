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

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.api.Binary;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class DictionaryReaderTest
{
    @Mock
    private Dictionary mockDictionary;

    private DictionaryReader dictionaryReaderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        dictionaryReaderUnderTest = new DictionaryReader(mockDictionary);
    }

    @Test
    public void testInitFromPage() throws Exception
    {
        // Setup
        byte[] bytes = new byte[1];
        final ByteBufferInputStream in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes));

        // Run the test
        dictionaryReaderUnderTest.initFromPage(0, in);

        // Verify the results
    }

    @Test
    public void testInitFromPage_ThrowsIOException()
    {
        // Setup
        final ByteBufferInputStream in = ByteBufferInputStream.wrap(ByteBuffer.wrap("content".getBytes()));
    }

    @Test
    public void testReadValueDictionaryId() throws IOException
    {
        // Setup
        // Run the test
        byte[] bytes = new byte[1];
        final ByteBufferInputStream in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes));
        dictionaryReaderUnderTest.initFromPage(0, in);

        final int result = dictionaryReaderUnderTest.readValueDictionaryId();

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testReadBytes()
    {
        // Setup
        final Binary expectedResult = Binary.fromReusedByteArray("content".getBytes());

        // Configure Dictionary.decodeToBinary(...).
        final Binary binary = Binary.fromReusedByteArray("content".getBytes());
        when(mockDictionary.decodeToBinary(0)).thenReturn(binary);

        // Run the test
        final Binary result = dictionaryReaderUnderTest.readBytes();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testReadFloat()
    {
        // Setup
        when(mockDictionary.decodeToFloat(0)).thenReturn(0.0f);

        // Run the test
        final float result = dictionaryReaderUnderTest.readFloat();

        // Verify the results
        assertEquals(0.0f, result, 0.0001);
    }

    @Test
    public void testReadDouble() throws Exception
    {
        // Setup
        when(mockDictionary.decodeToDouble(0)).thenReturn(0.0);

        // Run the test
        final double result = dictionaryReaderUnderTest.readDouble();

        // Verify the results
        assertEquals(0.0, result, 0.0001);
    }

    @Test
    public void testReadInteger()
    {
        // Setup
        when(mockDictionary.decodeToInt(0)).thenReturn(0);

        // Run the test
        final int result = dictionaryReaderUnderTest.readInteger();

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testReadLong()
    {
        // Setup
        when(mockDictionary.decodeToLong(0)).thenReturn(0L);

        // Run the test
        final long result = dictionaryReaderUnderTest.readLong();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testSkip()
    {
        // Setup
        // Run the test
        dictionaryReaderUnderTest.skip();

        // Verify the results
    }
}
