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
import io.prestosql.orc.OrcSelectiveRecordReader;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePageSourceProvider;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class OrcSelectivePageSourceTest
{
    @Mock
    private OrcSelectiveRecordReader mockRecordReader;
    @Mock
    private OrcDataSource mockOrcDataSource;
    @Mock
    private AggregatedMemoryContext mockSystemMemoryContext;
    @Mock
    private FileFormatDataSourceStats mockStats;
    @Mock
    private TypeManager mockTypeManager;

    private OrcSelectivePageSource orcSelectivePageSourceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        orcSelectivePageSourceUnderTest = new OrcSelectivePageSource(mockRecordReader, mockOrcDataSource,
                mockSystemMemoryContext, mockStats,
                Arrays.asList(HivePageSourceProvider.ColumnMapping.interim(
                        new HiveColumnHandle("name", HIVE_STRING,
                                new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                                HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false), 0)),
                mockTypeManager);
    }

    @Test
    public void testGetCompletedBytes()
    {
        // Setup
        when(mockOrcDataSource.getReadBytes()).thenReturn(0L);

        // Run the test
        final long result = orcSelectivePageSourceUnderTest.getCompletedBytes();
    }

    @Test
    public void testGetReadTimeNanos()
    {
        // Setup
        when(mockOrcDataSource.getReadTimeNanos()).thenReturn(0L);

        // Run the test
        final long result = orcSelectivePageSourceUnderTest.getReadTimeNanos();
    }

    @Test
    public void testIsFinished()
    {
        orcSelectivePageSourceUnderTest.isFinished();
    }

    @Test
    public void testGetNextPage() throws Exception
    {
        final Page result = orcSelectivePageSourceUnderTest.getNextPage();
    }

    @Test
    public void testGetNextPage_OrcSelectiveRecordReaderGetNextPageThrowsIOException() throws Exception
    {
        final Page result = orcSelectivePageSourceUnderTest.getNextPage();
    }

    @Test
    public void testGetNextPage_OrcSelectiveRecordReaderCloseThrowsIOException() throws Exception
    {
        final Page result = orcSelectivePageSourceUnderTest.getNextPage();
    }

    @Test
    public void testClose() throws Exception
    {
        orcSelectivePageSourceUnderTest.close();
    }

    @Test
    public void testToString() throws Exception
    {
        orcSelectivePageSourceUnderTest.toString();
    }

    @Test
    public void testGetSystemMemoryUsage()
    {
        final long result = orcSelectivePageSourceUnderTest.getSystemMemoryUsage();
    }

    @Test
    public void testCloseWithSuppression() throws Exception
    {
        orcSelectivePageSourceUnderTest.closeWithSuppression(new Exception("message"));
    }

    @Test
    public void testCloseWithSuppression_OrcSelectiveRecordReaderCloseThrowsIOException() throws Exception
    {
        orcSelectivePageSourceUnderTest.closeWithSuppression(new Exception("message"));
    }
}
