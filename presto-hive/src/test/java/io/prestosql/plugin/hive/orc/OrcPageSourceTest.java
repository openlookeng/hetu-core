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
package io.prestosql.plugin.hive.orc;

import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.util.FieldSetterFactoryTest;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.mockito.MockitoAnnotations.initMocks;

public class OrcPageSourceTest
{
    @Mock
    private OrcRecordReader mockRecordReader;
    @Mock
    private OrcDataSource mockOrcDataSource;
    @Mock
    private OrcDeletedRows mockDeletedRows;
    @Mock
    private AggregatedMemoryContext mockSystemMemoryContext;
    @Mock
    private FileFormatDataSourceStats mockStats;

    private OrcPageSource orcPageSourceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        orcPageSourceUnderTest = new OrcPageSource(mockRecordReader,
                Arrays.asList(OrcPageSource.ColumnAdaptation.sourceColumn(0)), mockOrcDataSource, mockDeletedRows,
                false, mockSystemMemoryContext, mockStats);
    }

    @Test
    public void test()
    {
        OrcPageSource.ColumnAdaptation.nullColumn(new FieldSetterFactoryTest.Type());
        OrcPageSource.ColumnAdaptation.sourceColumn(1, true);
    }

    @Test
    public void testToString() throws Exception
    {
        final String result = orcPageSourceUnderTest.toString();
    }
}
