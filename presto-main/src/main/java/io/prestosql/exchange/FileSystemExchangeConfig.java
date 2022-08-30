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

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.prestosql.spi.checksum.CheckSumAlgorithm;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.exchange.FileSystemExchangeManager.PATH_SEPARATOR;
import static io.prestosql.spi.checksum.CheckSumAlgorithm.MURMUR3;

public class FileSystemExchangeConfig
{
    private List<URI> baseDirectories = ImmutableList.of();
    private boolean exchangeEncryptionEnabled;
    private boolean exchangeCompressionEnabled;

    private DataSize maxPageStorageSize = new DataSize(16, MEGABYTE);
    private int exchangeSinkBufferPoolMinSize = 10;
    private int exchangeSinkBuffersPerPartition = 2;
    private DataSize exchangeSinkMaxFileSize = new DataSize(1, GIGABYTE);
    private int exchangeSourceConcurrentReaders = 4;
    private int maxOutputPartitionCount = 50;
    private int exchangeFileListingParallelism = 50;
    private String exchangeFilesystemType = "local";
    private int maxNumberOfPagesPerMarker = 10;
    private DataSize maxSizePerMarker = new DataSize(1, MEGABYTE);
    private CheckSumAlgorithm checkSumAlgorithm = MURMUR3;
    DirectSerialisationType directSerialisationType = DirectSerialisationType.OFF;
    private DataSize directSerialisationBufferSize = new DataSize(16, KILOBYTE);

    public enum DirectSerialisationType
    {
        OFF,
        JAVA,
        KRYO
    }

    @NotNull
    @NotEmpty(message = "At least one base directory needs to be configured")
    public List<URI> getBaseDirectories()
    {
        return baseDirectories;
    }

    @Config("exchange.base-directories")
    @ConfigDescription("List of base directories separated by comma")
    public FileSystemExchangeConfig setBaseDirectories(String baseDirectories)
    {
        if (!isNullOrEmpty(baseDirectories)) {
            ImmutableList.Builder<URI> builder = ImmutableList.builder();
            Arrays.stream(baseDirectories.split(",")).forEach(dir -> {
                if (!dir.endsWith(PATH_SEPARATOR)) {
                    dir += PATH_SEPARATOR;
                }
                builder.add(URI.create(dir));
            });
            this.baseDirectories = builder.build();
        }
        return this;
    }

    public boolean isExchangeEncryptionEnabled()
    {
        return exchangeEncryptionEnabled;
    }

    @Config("exchange.encryption-enabled")
    public FileSystemExchangeConfig setExchangeEncryptionEnabled(boolean exchangeEncryptionEnabled)
    {
        this.exchangeEncryptionEnabled = exchangeEncryptionEnabled;
        return this;
    }

    public boolean isExchangeCompressionEnabled()
    {
        return exchangeCompressionEnabled;
    }

    @Config("exchange.compression-enabled")
    public FileSystemExchangeConfig setExchangeCompressionEnabled(boolean exchangeCompressionEnabled)
    {
        this.exchangeCompressionEnabled = exchangeCompressionEnabled;
        return this;
    }

    public DataSize getMaxPageStorageSize()
    {
        return maxPageStorageSize;
    }

    @Config("exchange.max-page-storage-size")
    @ConfigDescription("Max storage size of a page written to a sink, including the page itself and its size represented by an int")
    public FileSystemExchangeConfig setMaxPageStorageSize(DataSize maxPageStorageSize)
    {
        this.maxPageStorageSize = maxPageStorageSize;
        return this;
    }

    public int getExchangeSinkBufferPoolMinSize()
    {
        return exchangeSinkBufferPoolMinSize;
    }

    @Config("exchange.sink-buffer-pool-min-size")
    public FileSystemExchangeConfig setExchangeSinkBufferPoolMinSize(int exchangeSinkBufferPoolMinSize)
    {
        this.exchangeSinkBufferPoolMinSize = exchangeSinkBufferPoolMinSize;
        return this;
    }

    @Min(2)
    public int getExchangeSinkBuffersPerPartition()
    {
        return exchangeSinkBuffersPerPartition;
    }

    @Config("exchange.sink-buffers-per-partition")
    public FileSystemExchangeConfig setExchangeSinkBuffersPerPartition(int exchangeSinkBuffersPerPartition)
    {
        this.exchangeSinkBuffersPerPartition = exchangeSinkBuffersPerPartition;
        return this;
    }

    public DataSize getExchangeSinkMaxFileSize()
    {
        return exchangeSinkMaxFileSize;
    }

    @Config("exchange.sink-max-file-size")
    @ConfigDescription("Max size of files written by sinks")
    public FileSystemExchangeConfig setExchangeSinkMaxFileSize(DataSize exchangeSinkMaxFileSize)
    {
        this.exchangeSinkMaxFileSize = exchangeSinkMaxFileSize;
        return this;
    }

    @Min(1)
    public int getExchangeSourceConcurrentReaders()
    {
        return exchangeSourceConcurrentReaders;
    }

    @Config("exchange.source-concurrent-readers")
    public FileSystemExchangeConfig setExchangeSourceConcurrentReaders(int exchangeSourceConcurrentReaders)
    {
        this.exchangeSourceConcurrentReaders = exchangeSourceConcurrentReaders;
        return this;
    }

    @Min(1)
    public int getMaxOutputPartitionCount()
    {
        return maxOutputPartitionCount;
    }

    @Config("exchange.max-output-partition-count")
    public FileSystemExchangeConfig setMaxOutputPartitionCount(int maxOutputPartitionCount)
    {
        this.maxOutputPartitionCount = maxOutputPartitionCount;
        return this;
    }

    @Min(1)
    public int getExchangeFileListingParallelism()
    {
        return exchangeFileListingParallelism;
    }

    @Config("exchange.file-listing-parallelism")
    @ConfigDescription("Max parallelism of file listing calls when enumerating spooling files.")
    public FileSystemExchangeConfig setExchangeFileListingParallelism(int exchangeFileListingParallelism)
    {
        this.exchangeFileListingParallelism = exchangeFileListingParallelism;
        return this;
    }

    @NotNull
    public String getExchangeFilesystemType()
    {
        return exchangeFilesystemType;
    }

    @Config("exchange-filesystem-type")
    @ConfigDescription("Possible values: [local, hdfs]")
    public FileSystemExchangeConfig setExchangeFilesystemType(String exchangeFilesystemType)
    {
        this.exchangeFilesystemType = exchangeFilesystemType;
        return this;
    }

    @NotNull
    public int getMaxNumberOfPagesPerMarker()
    {
        return maxNumberOfPagesPerMarker;
    }

    @Config("exchange.max-number-of-pages-per-marker")
    public FileSystemExchangeConfig setMaxNumberOfPagesPerMarker(int numberOfPagesPerMarker)
    {
        this.maxNumberOfPagesPerMarker = numberOfPagesPerMarker;
        return this;
    }

    @NotNull
    public DataSize getMaxSizePerMarker()
    {
        return maxSizePerMarker;
    }

    @Config("exchange.max-size-per-marker")
    @ConfigDescription("max size per marker (MB)")
    public FileSystemExchangeConfig setMaxSizePerMarker(DataSize sizePerMarkerInBytes)
    {
        this.maxSizePerMarker = sizePerMarkerInBytes;
        return this;
    }

    @NotNull
    public CheckSumAlgorithm getCheckSumAlgorithm()
    {
        return checkSumAlgorithm;
    }

    @Config("exchange.checksum-algorithm")
    @ConfigDescription("valid values {MD5, SHA256, SHA512, MURMUR3}")
    public FileSystemExchangeConfig setCheckSumAlgorithm(String checkSumAlgorithm)
    {
        this.checkSumAlgorithm = CheckSumAlgorithm.valueOf(checkSumAlgorithm);
        return this;
    }

    public DirectSerialisationType getDirectSerializationType()
    {
        return directSerialisationType;
    }

    @Config("exchange.direct-serialization-type")
    @ConfigDescription("direct serialization type")
    public FileSystemExchangeConfig setDirectSerializationType(DirectSerialisationType directSerialisationType)
    {
        this.directSerialisationType = directSerialisationType;
        return this;
    }

    @Config("exchange.direct-serialization-buffer-size")
    @ConfigDescription("Size of buffer for Kryo input")
    public FileSystemExchangeConfig setDirectSerialisationBufferSize(DataSize directSerialisationBufferSize)
    {
        this.directSerialisationBufferSize = directSerialisationBufferSize;
        return this;
    }

    public DataSize getDirectSerialisationBufferSize()
    {
        return directSerialisationBufferSize;
    }
}
