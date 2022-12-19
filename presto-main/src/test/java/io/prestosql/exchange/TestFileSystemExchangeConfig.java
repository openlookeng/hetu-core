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
package io.prestosql.exchange;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

public class TestFileSystemExchangeConfig
{
    @Test
    public void testDefault()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(FileSystemExchangeConfig.class)
                .setBaseDirectories(null)
                .setExchangeEncryptionEnabled(false)
                .setExchangeCompressionEnabled(false)
                .setMaxPageStorageSize(new DataSize(16, DataSize.Unit.MEGABYTE))
                .setExchangeSinkBufferPoolMinSize(10)
                .setExchangeSinkBuffersPerPartition(2)
                .setExchangeSinkMaxFileSize(new DataSize(1, DataSize.Unit.GIGABYTE))
                .setExchangeSourceConcurrentReaders(4)
                .setMaxOutputPartitionCount(50)
                .setExchangeFileListingParallelism(50)
                .setExchangeFilesystemType("local")
                .setDirectSerializationType(FileSystemExchangeConfig.DirectSerialisationType.JAVA)
                .setDirectSerialisationBufferSize(new DataSize(16, DataSize.Unit.KILOBYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("exchange.base-directories", "/temp")
                .put("exchange.encryption-enabled", "true")
                .put("exchange.compression-enabled", "true")
                .put("exchange.max-page-storage-size", "24MB")
                .put("exchange.sink-buffer-pool-min-size", "5")
                .put("exchange.sink-buffers-per-partition", "4")
                .put("exchange.sink-max-file-size", "2GB")
                .put("exchange.source-concurrent-readers", "5")
                .put("exchange.max-output-partition-count", "100")
                .put("exchange.file-listing-parallelism", "60")
                .put("exchange-filesystem-type", "hdfs")
                .put("exchange.direct-serialization-type", "KRYO")
                .put("exchange.direct-serialization-buffer-size", "32kB")
                .build();

        FileSystemExchangeConfig expected = new FileSystemExchangeConfig()
                .setBaseDirectories("/temp")
                .setExchangeEncryptionEnabled(true)
                .setExchangeCompressionEnabled(true)
                .setMaxPageStorageSize(new DataSize(24, DataSize.Unit.MEGABYTE))
                .setExchangeSinkBufferPoolMinSize(5)
                .setExchangeSinkBuffersPerPartition(4)
                .setExchangeSinkMaxFileSize(new DataSize(2, DataSize.Unit.GIGABYTE))
                .setExchangeSourceConcurrentReaders(5)
                .setMaxOutputPartitionCount(100)
                .setExchangeFileListingParallelism(60)
                .setExchangeFilesystemType("hdfs")
                .setDirectSerializationType(FileSystemExchangeConfig.DirectSerialisationType.KRYO)
                .setDirectSerialisationBufferSize(new DataSize(32, DataSize.Unit.KILOBYTE));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
