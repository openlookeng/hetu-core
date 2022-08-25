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
package io.prestosql.parquet.predicate;

import io.prestosql.parquet.DictionaryPage;
import io.prestosql.parquet.ParquetEncoding;
import org.apache.parquet.column.ColumnDescriptor;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class DictionaryDescriptorTest
{
    @Mock
    private ColumnDescriptor mockColumnDescriptor;

    private DictionaryDescriptor dictionaryDescriptorUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        dictionaryDescriptorUnderTest = new DictionaryDescriptor(mockColumnDescriptor,
                Optional.of(new DictionaryPage(null, 0, 0, ParquetEncoding.PLAIN)));
    }

    @Test
    public void testGetColumnDescriptor()
    {
        dictionaryDescriptorUnderTest.getColumnDescriptor();
    }

    @Test
    public void testGetDictionaryPage()
    {
        dictionaryDescriptorUnderTest.getDictionaryPage();
    }
}
