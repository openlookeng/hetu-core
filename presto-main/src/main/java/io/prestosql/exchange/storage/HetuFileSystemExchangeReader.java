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
package io.prestosql.exchange.storage;

import com.esotericsoftware.kryo.io.Input;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.compress.snappy.SnappyFramedInputStream;
import io.airlift.log.Logger;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeUtil;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.exchange.ExchangeSourceFile;
import io.prestosql.exchange.FileSystemExchangeConfig.DirectSerialisationType;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.GuardedBy;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
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
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HetuFileSystemExchangeReader.class).instanceSize();
    private static final String CIPHER_TRANSFORMATION = "AES/CBC/PKCS5Padding";

    private static final Logger LOG = Logger.get(HetuFileSystemExchangeReader.class);

    private static final int BUFFER_SIZE_IN_BYTES = toIntExact(new DataSize(4, KILOBYTE).toBytes());

    HetuFileSystemClient fsClient;

    private final Queue<ExchangeSourceFile> sourceFiles;
    private final AlgorithmParameterSpec algorithmParameterSpec;

    @GuardedBy("this")
    private InputStreamSliceInput sliceInput;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private Input kryoInput;
    private final DirectSerialisationType directSerialisationType;
    private final int directSerialisationBufferSize;

    public HetuFileSystemExchangeReader(Queue<ExchangeSourceFile> sourceFiles, HetuFileSystemClient fileSystemClient, AlgorithmParameterSpec algorithmParameterSpec, DirectSerialisationType directSerialisationType, int directSerialisationBufferSize)
    {
        this.sourceFiles = requireNonNull(sourceFiles, "sourceFiles is null");
        this.fsClient = requireNonNull(fileSystemClient, "fileSystemClient is null");
        this.algorithmParameterSpec = requireNonNull(algorithmParameterSpec, "gcmParameterSpec is null");
        this.directSerialisationType = directSerialisationType;
        this.directSerialisationBufferSize = directSerialisationBufferSize;
    }

    @Override
    public synchronized Slice read()
    {
        int markerData;

        if (closed) {
            return null;
        }

        if (sliceInput != null && sliceInput.isReadable()) {
            markerData = sliceInput.readInt();
            LOG.debug("reading: markerData: " + markerData);
            // Currently marker contains size of serialized page
            return sliceInput.readSlice(markerData);
        }

        ExchangeSourceFile sourceFile = sourceFiles.poll();
        if (sourceFile == null) {
            close();
            return null;
        }

        sliceInput = getSliceInput(sourceFile);
        markerData = sliceInput.readInt();
        return sliceInput.readSlice(markerData);
    }

    @Override
    public synchronized SerializedPage readSer()
    {
        int markerData;

        if (closed) {
            return null;
        }

        if (sliceInput != null && sliceInput.isReadable()) {
            markerData = sliceInput.readInt();
            LOG.debug("reading: markerData: " + markerData);
            // Currently marker contains size of serialized page
            Slice slice = Slices.allocate(markerData);
            return PagesSerdeUtil.readSerializedPage(slice);
        }

        ExchangeSourceFile sourceFile = sourceFiles.poll();
        if (sourceFile == null) {
            close();
            return null;
        }

        sliceInput = getSliceInput(sourceFile);
        markerData = sliceInput.readInt();
        Slice slice = Slices.allocate(markerData);
        return PagesSerdeUtil.readSerializedPage(slice);
    }

    @Override
    public synchronized Page read(PagesSerde serde) throws IOException
    {
        if (closed) {
            return null;
        }

        if (sliceInput == null || (kryoInput == null && directSerialisationType == DirectSerialisationType.KRYO)) {
            ExchangeSourceFile sourceFile = sourceFiles.poll();
            if (sourceFile == null) {
                close();
                return null;
            }
            sliceInput = getSliceInput(sourceFile);
            if (directSerialisationType == DirectSerialisationType.KRYO) {
                kryoInput = new Input(sliceInput, directSerialisationBufferSize);
            }
        }

        if (directSerialisationType == DirectSerialisationType.KRYO) {
            if (kryoInput.end()) {
                ExchangeSourceFile sourceFile = sourceFiles.poll();
                if (sourceFile == null) {
                    close();
                    return null;
                }
                sliceInput = getSliceInput(sourceFile);
                kryoInput = new Input(sliceInput, directSerialisationBufferSize);
            }
            return serde.deserialize(kryoInput);
        }
        else {
            if (!sliceInput.isReadable()) {
                ExchangeSourceFile sourceFile = sourceFiles.poll();
                if (sourceFile == null) {
                    close();
                    return null;
                }
                sliceInput = getSliceInput(sourceFile);
            }
            return serde.deserialize(sliceInput);
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
        return INSTANCE_SIZE + (sliceInput == null ? 0 : sliceInput.getRetainedSize());
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
        if (kryoInput != null) {
            kryoInput.close();
        }
        else if (sliceInput != null) {
            sliceInput.close();
            sliceInput = null;
        }
    }

    private InputStreamSliceInput getSliceInput(ExchangeSourceFile sourceFile)
    {
        try {
            Path file = Paths.get(sourceFile.getFileUri());
            Optional<SecretKey> secretKey = sourceFile.getSecretKey();
            boolean exchangeCompressionEnabled = sourceFile.isExchangeCompressionEnabled();
            if (secretKey.isPresent() && exchangeCompressionEnabled) {
                Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
                cipher.init(Cipher.DECRYPT_MODE, secretKey.get(), algorithmParameterSpec);
                return new InputStreamSliceInput(new SnappyFramedInputStream(new CipherInputStream(fsClient.newInputStream(file), cipher)), BUFFER_SIZE_IN_BYTES);
            }
            else if (secretKey.isPresent()) {
                Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
                cipher.init(Cipher.DECRYPT_MODE, secretKey.get(), algorithmParameterSpec);
                return new InputStreamSliceInput(new CipherInputStream(fsClient.newInputStream(file), cipher), BUFFER_SIZE_IN_BYTES);
            }
            else if (exchangeCompressionEnabled) {
                return new InputStreamSliceInput(new SnappyFramedInputStream(fsClient.newInputStream(file)), BUFFER_SIZE_IN_BYTES);
            }
            else {
                return new InputStreamSliceInput(fsClient.newInputStream(file), BUFFER_SIZE_IN_BYTES);
            }
        }
        catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException | NoSuchPaddingException |
               InvalidKeyException | IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create InputStream: " + e.getMessage(), e);
        }
    }
}
