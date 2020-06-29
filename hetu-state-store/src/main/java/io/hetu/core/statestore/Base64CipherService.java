/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.statestore;

import io.prestosql.spi.statestore.CipherService;
import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.apache.commons.lang3.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;

/**
 * CipherService that uses base64 encoding and decoding to encrypt values
 *
 * @param <T> type of object to be encrypted
 * @since 2020-03-20
 */
public class Base64CipherService<T extends Serializable>
        implements CipherService<T>
{
    @Override
    public String encrypt(T value)
    {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(value);
            return Base64.getEncoder().encodeToString(bos.toByteArray());
        }
        catch (IOException e) {
            throw new SerializationException(String.format("Unable to encrypt value: %s, %s", value, e.getMessage()));
        }
    }

    @Override
    public T decrypt(String encryptedValue)
    {
        byte[] decryptedDataBytes = Base64.getDecoder().decode(encryptedValue);
        try (ValidatingObjectInputStream stream = new ValidatingObjectInputStream(new ByteArrayInputStream(decryptedDataBytes))) {
            // Add whitelisting here to prevent security issue during deserialization
            // Add class to decrypt in the accept method so it's in the whitelist
            stream.accept("java.lang.*", "java.util.*", "io.hetu.core.*");
            return (T) stream.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            throw new SerializationException(String.format("Unable to decrypt value: %s, %s", encryptedValue, e.getMessage()));
        }
    }
}
