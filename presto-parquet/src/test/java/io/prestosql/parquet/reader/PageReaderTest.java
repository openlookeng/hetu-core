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

import io.prestosql.parquet.DataPage;
import io.prestosql.parquet.DictionaryPage;
import io.prestosql.parquet.ParquetEncoding;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class PageReaderTest
{
    @Mock
    private DictionaryPage mockCompressedDictionaryPage;

    private PageReader pageReaderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        pageReaderUnderTest = new PageReader(CompressionCodecName.UNCOMPRESSED, Arrays.asList(),
                mockCompressedDictionaryPage);
    }

    @Test
    public void testGetTotalValueCount()
    {
        assertEquals(0L, pageReaderUnderTest.getTotalValueCount());
    }

    @Test
    public void testReadPage() throws Exception
    {
        // Setup
        // Run the test
        final DataPage result = pageReaderUnderTest.readPage();

        // Verify the results
    }

    @Test
    public void testReadDictionaryPage()
    {
        // Setup
        when(mockCompressedDictionaryPage.getSlice()).thenReturn(null);
        when(mockCompressedDictionaryPage.getUncompressedSize()).thenReturn(0);
        when(mockCompressedDictionaryPage.getDictionarySize()).thenReturn(0);
        when(mockCompressedDictionaryPage.getEncoding()).thenReturn(ParquetEncoding.PLAIN);

        // Run the test
        final DictionaryPage result = pageReaderUnderTest.readDictionaryPage();

        // Verify the results
    }
}
