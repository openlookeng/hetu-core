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
package io.hetu.core.plugin.exchange.filesystem.storage;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.hetu.core.plugin.exchange.filesystem.ExchangeSourceFile;
import io.hetu.core.plugin.exchange.filesystem.FileStatus;
import io.hetu.core.plugin.exchange.filesystem.FileSystemExchangeConfig;
import io.prestosql.spi.filesystem.HetuFileSystemClient;

import javax.crypto.SecretKey;
import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.Math.toIntExact;

public class HetuFileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private HetuFileSystemClient fileSystemClient;
    private static final int BUFFER_SIZE_IN_BYTES = toIntExact(new DataSize(4, KILOBYTE).toBytes());

    @Inject
    public HetuFileSystemExchangeStorage(FileSystemExchangeConfig config)
    {
    }

    public void setFileSystemClient(HetuFileSystemClient fsClient)
    {
        fileSystemClient = fsClient;
    }

    @Override
    public void createDirectories(URI dir) throws IOException
    {
        fileSystemClient.createDirectories(Paths.get(dir.toString()));
    }

    @Override
    public ListenableFuture<Void> createEmptyFile(URI file)
    {
        try {
            fileSystemClient.createFile(Paths.get(file.toString()));
        }
        catch (IOException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(null);
    }

    @Override
    public ListenableFuture<Void> deleteRecursively(List<URI> directories)
    {
        try {
            directories.forEach(uri -> {
                try {
                    fileSystemClient.deleteRecursively(Paths.get(uri.toString()));
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        catch (RuntimeException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(null);
    }

    private void listFilesRecursively(URI dir, ImmutableList.Builder<FileStatus> builder) throws IOException
    {
        fileSystemClient.list(Paths.get(dir.toString())).forEach(file -> {
            try {
                if (fileSystemClient.isDirectory(file)) {
                    listFilesRecursively(URI.create(file.toString()), builder);
                }
                else {
                    builder.add(new FileStatus(file.toUri().toString(), (Long) fileSystemClient.getAttribute(file, "size")));
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public ListenableFuture<List<FileStatus>> listFilesRecursively(URI dir)
    {
        ImmutableList.Builder<FileStatus> fileStatusBuilder = ImmutableList.builder();
        try {
            listFilesRecursively(dir, fileStatusBuilder);
        }
        catch (IOException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(fileStatusBuilder.build());
    }

    @Override
    public int getWriterBufferSize()
    {
        return BUFFER_SIZE_IN_BYTES;
    }

    @Override
    public void close()
    {
    }

    @Override
    public ExchangeStorageReader createExchangeReader(Queue<ExchangeSourceFile> sourceFiles, int maxPageSize)
    {
        return new HetuFileSystemExchangeReader(sourceFiles, fileSystemClient);
    }

    @Override
    public ExchangeStorageWriter createExchangeWriter(URI file, Optional<SecretKey> secretKey)
    {
        return new HetuFileSystemExchangeWriter(file, fileSystemClient, secretKey);
    }
}
