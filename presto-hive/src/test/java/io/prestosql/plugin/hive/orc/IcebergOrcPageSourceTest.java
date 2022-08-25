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

import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.WriteIdInfo;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalLong;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class IcebergOrcPageSourceTest
{
    @Mock
    private OrcRecordReader mockRecordReader;
    @Mock
    private OrcDataSource mockOrcDataSource;
    @Mock
    private AggregatedMemoryContext mockMemoryContext;
    @Mock
    private FileFormatDataSourceStats mockStats;

    private IcebergOrcPageSource icebergOrcPageSourceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        HdfsConfigurationInitializer updator = new HdfsConfigurationInitializer(new HiveConfig());
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(updator, ImmutableSet.of());
        icebergOrcPageSourceUnderTest = new IcebergOrcPageSource(mockRecordReader,
                Arrays.asList(IcebergOrcPageSource.ColumnAdaptation.sourceColumn(0)), mockOrcDataSource,
                Optional.of(new OrcDeletedRows("sourceFileName", Optional.of(
                        new DeleteDeltaLocations("partitionLocation", Arrays.asList(new WriteIdInfo(0L, 0L, 0)))),
                        new OrcDeleteDeltaPageSourceFactory("sessionUser", new Configuration(false),
                                new HdfsEnvironment(hdfsConfiguration, new HiveConfig(), new NoHdfsAuthentication()),
                                new DataSize(0.0, DataSize.Unit.BYTE), new DataSize(0.0, DataSize.Unit.BYTE),
                                new DataSize(0.0, DataSize.Unit.BYTE), new DataSize(0.0, DataSize.Unit.BYTE),
                                new DataSize(0.0, DataSize.Unit.BYTE), false, false, new FileFormatDataSourceStats()),
                        "sessionUser", new Configuration(false), new HdfsEnvironment(hdfsConfiguration, new HiveConfig(), new NoHdfsAuthentication()),
                        Optional.of(0L))),
                Optional.of(0L), mockMemoryContext, mockStats);
    }

    @Test
    public void testGetCompletedBytes()
    {
        final long result = icebergOrcPageSourceUnderTest.getCompletedBytes();
    }

    @Test
    public void testGetCompletedPositions()
    {
        final OptionalLong result = icebergOrcPageSourceUnderTest.getCompletedPositions();
    }

    @Test
    public void testGetReadTimeNanos()
    {
        final long result = icebergOrcPageSourceUnderTest.getReadTimeNanos();
    }

    @Test
    public void testIsFinished()
    {
        icebergOrcPageSourceUnderTest.isFinished();
    }

    @Test
    public void testGetNextPage() throws Exception
    {
        final Page result = icebergOrcPageSourceUnderTest.getNextPage();
    }

    @Test
    public void testGetNextPage_OrcRecordReaderNextPageThrowsIOException() throws Exception
    {
        final Page result = icebergOrcPageSourceUnderTest.getNextPage();
    }

    @Test
    public void testGetNextPage_OrcRecordReaderCloseThrowsIOException() throws Exception
    {
        final Page result = icebergOrcPageSourceUnderTest.getNextPage();
    }

    @Test
    public void testGetSystemMemoryUsage()
    {
        final long result = icebergOrcPageSourceUnderTest.getSystemMemoryUsage();
    }

    @Test
    public void testClose() throws Exception
    {
        icebergOrcPageSourceUnderTest.close();
    }

    @Test
    public void testClose_OrcRecordReaderCloseThrowsIOException() throws Exception
    {
        // Setup
        when(mockRecordReader.getMaxCombinedBytesPerRow()).thenReturn(0L);
        doThrow(IOException.class).when(mockRecordReader).close();

        // Run the test
        icebergOrcPageSourceUnderTest.close();
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        when(mockOrcDataSource.getId()).thenReturn(new OrcDataSourceId("id"));

        // Run the test
        final String result = icebergOrcPageSourceUnderTest.toString();
    }

    @Test
    public void testGetMemoryUsage()
    {
        // Setup
        when(mockMemoryContext.getBytes()).thenReturn(0L);

        // Run the test
        final long result = icebergOrcPageSourceUnderTest.getMemoryUsage();
    }

    @Test
    public void testHandleException() throws Exception
    {
        // Setup
        final OrcDataSourceId dataSourceId = new OrcDataSourceId("id");

        // Run the test
        final PrestoException result = IcebergOrcPageSource.handleException(dataSourceId, new Exception("message"));
    }
}
