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
package io.hetu.core.plugin.iceberg;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.orc.OrcReaderConfig;
import io.prestosql.plugin.hive.orc.OrcWriterConfig;
import io.prestosql.plugin.hive.parquet.ParquetReaderConfig;
import io.prestosql.plugin.hive.parquet.ParquetWriterConfig;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class IcebergSessionPropertiesTest
{
    @Mock
    private IcebergConfig mockIcebergConfig;
    @Mock
    private OrcReaderConfig mockOrcReaderConfig;
    @Mock
    private OrcWriterConfig mockOrcWriterConfig;
    @Mock
    private ParquetReaderConfig mockParquetReaderConfig;
    @Mock
    private ParquetWriterConfig mockParquetWriterConfig;
    @Mock
    private HiveConfig mockHiveConfig;

    private IcebergSessionProperties icebergSessionPropertiesUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        IcebergConfig hiveCatalogName = new IcebergConfig().setHiveCatalogName("hiveCatalogName");
        icebergSessionPropertiesUnderTest = new IcebergSessionProperties(hiveCatalogName, mockOrcReaderConfig,
                mockOrcWriterConfig, mockParquetReaderConfig, mockParquetWriterConfig, mockHiveConfig);
    }

    @Test
    public void testGetSessionProperties()
    {
        icebergSessionPropertiesUnderTest.getSessionProperties();
    }

    @Test
    public void testIsOrcBloomFiltersEnabled()
    {
        // Run the test
        IcebergSessionProperties.isOrcBloomFiltersEnabled(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetOrcMaxMergeDistance()
    {
        final DataSize result = IcebergSessionProperties.getOrcMaxMergeDistance(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetOrcMaxBufferSize()
    {
        // Run the test
        final DataSize result = IcebergSessionProperties.getOrcMaxBufferSize(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetOrcStreamBufferSize()
    {
        // Run the test
        final DataSize result = IcebergSessionProperties.getOrcStreamBufferSize(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetOrcTinyStripeThreshold()
    {
        // Run the test
        final DataSize result = IcebergSessionProperties.getOrcTinyStripeThreshold(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetOrcMaxReadBlockSize()
    {
        // Run the test
        final DataSize result = IcebergSessionProperties.getOrcMaxReadBlockSize(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetOrcLazyReadSmallRanges()
    {
        // Run the test
        final boolean result = IcebergSessionProperties.getOrcLazyReadSmallRanges(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testIsOrcNestedLazy()
    {
        // Run the test
        final boolean result = IcebergSessionProperties.isOrcNestedLazy(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetOrcStringStatisticsLimit()
    {
        // Run the test
        final DataSize result = IcebergSessionProperties.getOrcStringStatisticsLimit(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testIsOrcWriterValidate()
    {
        final boolean result = IcebergSessionProperties.isOrcWriterValidate(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetOrcWriterValidateMode()
    {
        IcebergSessionProperties.getOrcWriterValidateMode(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetOrcWriterMinStripeSize()
    {
        final DataSize result = IcebergSessionProperties.getOrcWriterMinStripeSize(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetOrcWriterMaxStripeSize()
    {
        final DataSize result = IcebergSessionProperties.getOrcWriterMaxStripeSize(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetOrcWriterMaxStripeRows()
    {
        final int result = IcebergSessionProperties.getOrcWriterMaxStripeRows(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetOrcWriterMaxDictionaryMemory()
    {
        final DataSize result = IcebergSessionProperties.getOrcWriterMaxDictionaryMemory(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetCompressionCodec()
    {
        IcebergSessionProperties.getCompressionCodec(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testIsUseFileSizeFromMetadata()
    {
        final boolean result = IcebergSessionProperties.isUseFileSizeFromMetadata(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetParquetMaxReadBlockSize()
    {
        final DataSize result = IcebergSessionProperties.getParquetMaxReadBlockSize(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetParquetWriterPageSize()
    {
        final DataSize result = IcebergSessionProperties.getParquetWriterPageSize(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetParquetWriterBlockSize()
    {
        final DataSize result = IcebergSessionProperties.getParquetWriterBlockSize(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetParquetWriterBatchSize()
    {
        final int result = IcebergSessionProperties.getParquetWriterBatchSize(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetDynamicFilteringWaitTimeout()
    {
        final Duration result = IcebergSessionProperties.getDynamicFilteringWaitTimeout(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testIsStatisticsEnabled()
    {
        final boolean result = IcebergSessionProperties.isStatisticsEnabled(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testIsProjectionPushdownEnabled()
    {
        final boolean result = IcebergSessionProperties.isProjectionPushdownEnabled(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetTargetMaxFileSize()
    {
        final long result = IcebergSessionProperties.getTargetMaxFileSize(new HdfsFileIoProviderTest.ConnectorSession());
    }

    @Test
    public void testGetHiveCatalogName()
    {
        final Optional<String> result = IcebergSessionProperties.getHiveCatalogName(new HdfsFileIoProviderTest.ConnectorSession());
    }
}
