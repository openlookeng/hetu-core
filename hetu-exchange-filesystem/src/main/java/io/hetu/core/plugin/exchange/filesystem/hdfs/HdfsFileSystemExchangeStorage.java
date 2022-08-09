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
package io.hetu.core.plugin.exchange.filesystem.hdfs;

import com.google.common.util.concurrent.ListenableFuture;
import io.hetu.core.plugin.exchange.filesystem.ExchangeSourceFile;
import io.hetu.core.plugin.exchange.filesystem.ExchangeStorageReader;
import io.hetu.core.plugin.exchange.filesystem.ExchangeStorageWriter;
import io.hetu.core.plugin.exchange.filesystem.FileStatus;
import io.hetu.core.plugin.exchange.filesystem.FileSystemExchangeStorage;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

public class HdfsFileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    public static org.apache.hadoop.fs.Path toHdfsPath(Path path)
    {
        return new org.apache.hadoop.fs.Path(path.toString());
    }

    @Override
    public void createDirectories(URI dir) throws IOException
    {
        Files.createDirectories(Paths.get(dir.getPath()));
    }

    @Override
    public ExchangeStorageReader createExchangeReader(Queue<ExchangeSourceFile> sourceFiles, int maxPageSize)
    {
        return null;
    }

    @Override
    public ExchangeStorageWriter createExchangeWriter(URI file, Optional<SecretKey> secretKey)
    {
        return null;
    }

    @Override
    public ListenableFuture<Void> createEmptyFile(URI file)
    {
        return null;
    }

    @Override
    public ListenableFuture<Void> deleteRecursively(List<URI> directories)
    {
        return null;
    }

    @Override
    public ListenableFuture<List<FileStatus>> listFilesRecursively(URI dir)
    {
        return null;
    }

    @Override
    public int getWriterBufferSize()
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {
    }
}
