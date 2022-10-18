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
package io.prestosql.parquet.reader;

import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class LevelRLEReaderTest
{
    @Mock
    private RunLengthBitPackingHybridDecoder mockDelegate;

    private LevelRLEReader levelRLEReaderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        levelRLEReaderUnderTest = new LevelRLEReader(mockDelegate);
    }

    @Test
    public void testReadLevel() throws Exception
    {
        // Setup
        when(mockDelegate.readInt()).thenReturn(0);

        // Run the test
        final int result = levelRLEReaderUnderTest.readLevel();

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testReadLevel_RunLengthBitPackingHybridDecoderThrowsIOException() throws Exception
    {
        // Setup
        when(mockDelegate.readInt()).thenThrow(IOException.class);

        // Run the test
        final int result = levelRLEReaderUnderTest.readLevel();

        // Verify the results
        assertEquals(0, result);
    }
}
