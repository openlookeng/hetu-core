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

import com.esotericsoftware.kryo.io.Output;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.compress.snappy.SnappyFramedOutputStream;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.exchange.FileSystemExchangeConfig;
import io.prestosql.exchange.FileSystemExchangeConfig.DirectSerialisationType;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import org.openjdk.jol.info.ClassLayout;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class HetuFileSystemExchangeWriter
        implements ExchangeStorageWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HetuFileSystemExchangeWriter.class).instanceSize();
    private static final String CIPHER_TRANSFORMATION = "AES/CBC/PKCS5Padding";
    private final OutputStream outputStream;
    private final DirectSerialisationType directSerialisationType;
    private final int directSerialisationBufferSize;

    public HetuFileSystemExchangeWriter(URI file, HetuFileSystemClient fileSystemClient, Optional<SecretKey> secretKey, boolean exchangeCompressionEnabled, AlgorithmParameterSpec algorithmParameterSpec, FileSystemExchangeConfig.DirectSerialisationType directSerialisationType, int directSerialisationBufferSize)
    {
        this.directSerialisationBufferSize = directSerialisationBufferSize;
        this.directSerialisationType = directSerialisationType;
        try {
            Path path = Paths.get(file.toString());
            if (secretKey.isPresent() && exchangeCompressionEnabled) {
                Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
                cipher.init(Cipher.ENCRYPT_MODE, secretKey.get(), algorithmParameterSpec);
                this.outputStream = new SnappyFramedOutputStream(new CipherOutputStream(fileSystemClient.newOutputStream(path), cipher));
            }
            else if (secretKey.isPresent()) {
                Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
                cipher.init(Cipher.ENCRYPT_MODE, secretKey.get(), algorithmParameterSpec);
                this.outputStream = new CipherOutputStream(fileSystemClient.newOutputStream(path), cipher);
            }
            else if (exchangeCompressionEnabled) {
                this.outputStream = new SnappyFramedOutputStream(fileSystemClient.newOutputStream(path));
            }
            else {
                if (directSerialisationType == DirectSerialisationType.KRYO) {
                    this.outputStream = new Output(fileSystemClient.newOutputStream(path), directSerialisationBufferSize);
                }
                else if (directSerialisationType == DirectSerialisationType.JAVA) {
                    this.outputStream = new OutputStreamSliceOutput(fileSystemClient.newOutputStream(path), directSerialisationBufferSize);
                }
                else {
                    this.outputStream = new OutputStreamSliceOutput(fileSystemClient.newOutputStream(path), directSerialisationBufferSize);
                }
            }
        }
        catch (IOException | NoSuchAlgorithmException | InvalidAlgorithmParameterException | NoSuchPaddingException |
               InvalidKeyException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create OutputStream: " + e.getMessage(), e);
        }
    }

    @Override
    public ListenableFuture<Void> write(Slice slice)
    {
        try {
            outputStream.write(slice.getBytes());
        }
        catch (IOException | RuntimeException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(null);
    }

    @Override
    public ListenableFuture<Void> write(Page page, PagesSerde serde)
    {
        checkState(directSerialisationType != DirectSerialisationType.OFF, "Should be used with direct serialization is enabled!");
        serde.serialize(outputStream, page);
        return immediateFuture(null);
    }

    @Override
    public ListenableFuture<Void> finish()
    {
        try {
            outputStream.close();
        }
        catch (IOException | RuntimeException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(null);
    }

    @Override
    public ListenableFuture<Void> abort()
    {
        try {
            outputStream.close();
        }
        catch (IOException | RuntimeException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(null);
    }

    @Override
    public long getRetainedSize()
    {
        return INSTANCE_SIZE;
    }
}
