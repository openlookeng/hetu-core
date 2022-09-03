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
import io.prestosql.exchange.FileSystemExchangeConfig.DirectSerialisationType;
import io.prestosql.exchange.storage.FileSystemExchangeStorage;
import io.prestosql.spi.PrestoException;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.inject.Inject;

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.exchange.FileSystemExchangeErrorCode.MAX_OUTPUT_PARTITION_COUNT_EXCEEDED;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class FileSystemExchangeManager
        implements ExchangeManager
{
    public static final String PATH_SEPARATOR = "/";

    private static final int KEY_BITS = 256;

    private final FileSystemExchangeStorage exchangeStorage;
    private final FileSystemExchangeStats stats;
    private final List<URI> baseDirectories;
    private final boolean exchangeEncryptionEnabled;
    private final boolean exchangeCompressionEnabled;
    private final int maxPageStorageSizeInBytes;
    private final int exchangeSinkBufferPoolMinSize;
    private final int exchangeSinkBuffersPerPartition;
    private final long exchangeSinkMaxFileSizeInBytes;
    private final int exchangeSourceConcurrentReaders;
    private final int maxOutputPartitionCount;
    private final int exchangeFileListingParallelism;
    private final ExecutorService executor;
    private final DirectSerialisationType directSerialisationType;
    private final int directSerialisationBufferSize;

    FileSystemExchangeConfig exchangeConfig;

    @Inject
    public FileSystemExchangeManager(
            FileSystemExchangeStorage exchangeStorage,
            FileSystemExchangeStats stats,
            FileSystemExchangeConfig config)
    {
        requireNonNull(config, "config is null");
        this.exchangeStorage = exchangeStorage;
        this.stats = requireNonNull(stats, "stats is null");
        this.baseDirectories = ImmutableList.copyOf(requireNonNull(config.getBaseDirectories(), "baseDirectories is null"));
        this.exchangeEncryptionEnabled = config.isExchangeEncryptionEnabled();
        this.exchangeCompressionEnabled = config.isExchangeCompressionEnabled();
        this.maxPageStorageSizeInBytes = toIntExact(config.getMaxPageStorageSize().toBytes());
        this.exchangeSinkBufferPoolMinSize = config.getExchangeSinkBufferPoolMinSize();
        this.exchangeSinkBuffersPerPartition = config.getExchangeSinkBuffersPerPartition();
        this.exchangeSinkMaxFileSizeInBytes = config.getExchangeSinkMaxFileSize().toBytes();
        this.exchangeSourceConcurrentReaders = config.getExchangeSourceConcurrentReaders();
        this.maxOutputPartitionCount = config.getMaxOutputPartitionCount();
        this.exchangeFileListingParallelism = config.getExchangeFileListingParallelism();
        this.directSerialisationBufferSize = toIntExact(config.getDirectSerialisationBufferSize().toBytes());
        this.executor = newCachedThreadPool(daemonThreadsNamed("exchange-source-handles-creation-%s"));
        this.exchangeConfig = config;
        //TODO(Kishore): Need to enable compression and encryption for direct serde later
        directSerialisationType = (exchangeCompressionEnabled || exchangeEncryptionEnabled) ? DirectSerialisationType.OFF : config.getDirectSerializationType();
    }

    @Override
    public Exchange createExchange(ExchangeContext context, int outputPartitionCount)
    {
        if (outputPartitionCount > maxOutputPartitionCount) {
            throw new PrestoException(MAX_OUTPUT_PARTITION_COUNT_EXCEEDED,
                    format("Max number of output partitions exceeded for exchange %s. Allowed %s. Found: %s.", context.getExchangeId(), maxOutputPartitionCount, outputPartitionCount));
        }
        Optional<SecretKey> secretKey = Optional.empty();
        if (exchangeEncryptionEnabled) {
            try {
                KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
                keyGenerator.init(KEY_BITS);
                secretKey = Optional.of(keyGenerator.generateKey());
            }
            catch (NoSuchAlgorithmException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to generate secret key: " + e.getMessage(), e);
            }
        }
        return new FileSystemExchange(
                baseDirectories,
                exchangeStorage,
                stats,
                context,
                outputPartitionCount,
                exchangeFileListingParallelism,
                secretKey,
                exchangeCompressionEnabled,
                executor);
    }

    @Override
    public ExchangeSink createSink(ExchangeSinkInstanceHandle handle, boolean preserveRecordsOrder)
    {
        FileSystemExchangeSinkInstanceHandle instanceHandle = (FileSystemExchangeSinkInstanceHandle) handle;
        return new FileSystemExchangeSink(
                exchangeStorage,
                stats,
                instanceHandle.getOutputDirectory(),
                instanceHandle.getOutputPartitionCount(),
                instanceHandle.getSinkHandle().getSecretKey().map(key -> new SecretKeySpec(key, 0, key.length, "AES")),
                instanceHandle.getSinkHandle().getExchangeCompressionEnabled(),
                preserveRecordsOrder,
                maxPageStorageSizeInBytes,
                exchangeSinkBufferPoolMinSize,
                exchangeSinkBuffersPerPartition,
                exchangeSinkMaxFileSizeInBytes,
                directSerialisationType,
                directSerialisationBufferSize);
    }

    @Override
    public ExchangeSource createSource(List<ExchangeSourceHandle> handles)
    {
        List<ExchangeSourceFile> sourceFiles = handles.stream()
                .map(FileSystemExchangeSourceHandle.class::cast)
                .map(handle -> {
                    Optional<SecretKey> secretKey = handle.getSecretKey().map(key -> new SecretKeySpec(key, 0, key.length, "AES"));
                    boolean compressionEnabled = handle.getExchangeCompressionEnabled();
                    Object[] encryptionAndCompression = new Object[]{secretKey, compressionEnabled};
                    return new AbstractMap.SimpleEntry<>(handle, encryptionAndCompression);
                })
                .flatMap(entry -> entry.getKey().getFiles().stream().map(fileStatus ->
                        new ExchangeSourceFile(
                                URI.create(fileStatus.getFilePath()),
                                (Optional<SecretKey>) entry.getValue()[0],
                                (boolean) entry.getValue()[1],
                                fileStatus.getFileSize())))
                .collect(toImmutableList());
        return new FileSystemExchangeSource(
                exchangeStorage,
                stats,
                sourceFiles,
                maxPageStorageSizeInBytes,
                exchangeSourceConcurrentReaders,
                directSerialisationType,
                directSerialisationBufferSize);
    }
}
