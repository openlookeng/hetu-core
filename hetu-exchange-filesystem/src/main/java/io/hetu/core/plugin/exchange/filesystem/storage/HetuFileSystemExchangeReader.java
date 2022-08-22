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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.hetu.core.plugin.exchange.filesystem.ExchangeSourceFile;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.filesystem.HetuFileSystemClient;

import javax.annotation.concurrent.GuardedBy;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.Queue;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class HetuFileSystemExchangeReader
        implements ExchangeStorageReader
{
    private static final Logger LOG = Logger.get(HetuFileSystemExchangeReader.class);

    private static final int BUFFER_SIZE_IN_BYTES = toIntExact(new DataSize(4, KILOBYTE).toBytes());

    HetuFileSystemClient fsClient;

    private final Queue<ExchangeSourceFile> sourceFiles;

    @GuardedBy("this")
    private InputStreamSliceInput sliceInput;
    @GuardedBy("this")
    private boolean closed;

    public HetuFileSystemExchangeReader(Queue<ExchangeSourceFile> sourceFiles, HetuFileSystemClient fileSystemClient)
    {
        this.sourceFiles = requireNonNull(sourceFiles, "sourceFiles is null");
        this.fsClient = requireNonNull(fileSystemClient, "fileSystemClient is null");
    }

    @Override
    public synchronized Slice read()
    {
        int markerData;

        if (closed) {
            return null;
        }

        if (sliceInput == null) {
            ExchangeSourceFile sourceFile = sourceFiles.poll();
            if (sourceFile == null) {
                close();
                return null;
            }
            sliceInput = getSliceInput(sourceFile);
        }

        if (sliceInput.isReadable()) {
            markerData = sliceInput.readInt();
            LOG.debug("reading: markerData: " + markerData);
            // Currently marker contains size of serialized page
            return sliceInput.readSlice(markerData);
        }
        else {
            return null;
        }
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return immediateFuture(null);
    }

    @Override
    public synchronized long getRetainedSize()
    {
        return sliceInput == null ? 0 : sliceInput.getRetainedSize();
    }

    @Override
    public synchronized boolean isFinished()
    {
        return closed;
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (sliceInput != null) {
            sliceInput.close();
            sliceInput = null;
        }
    }

    private InputStreamSliceInput getSliceInput(ExchangeSourceFile sourceFile)
    {
        Path file = Paths.get(sourceFile.getFileUri());
        Optional<SecretKey> secretKey = sourceFile.getSecretKey();
        if (secretKey.isPresent()) {
            try {
                Cipher cipher = Cipher.getInstance("AES/GCM/PKCS5Padding");
                cipher.init(Cipher.DECRYPT_MODE, secretKey.get());
                return new InputStreamSliceInput(new CipherInputStream(fsClient.newInputStream(file), cipher), BUFFER_SIZE_IN_BYTES);
            }
            catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create CipherInputStream: " + e.getMessage(), e);
            }
        }
        else {
            try {
                return new InputStreamSliceInput(fsClient.newInputStream(file), BUFFER_SIZE_IN_BYTES);
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create CipherInputStream: " + e.getMessage(), e);
            }
        }
    }
}
