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
package io.prestosql.parquet;

import org.apache.parquet.column.statistics.Statistics;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class DataPageV2Test
{
    @Mock
    private Statistics<?> mockStatistics;

    private DataPageV2 dataPageV2UnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        dataPageV2UnderTest = new DataPageV2(0, 0, 0, null, null, ParquetEncoding.PLAIN, null, 0, mockStatistics,
                false);
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", dataPageV2UnderTest.toString());
    }
}
