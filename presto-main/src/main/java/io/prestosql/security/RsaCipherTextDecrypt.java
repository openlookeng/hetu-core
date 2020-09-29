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

package io.prestosql.security;

import io.prestosql.spi.security.CipherTextDecrypt;
import io.prestosql.spi.security.SecurityKeyManager;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.inject.Inject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.KeyFactory;
import java.security.Security;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class RsaCipherTextDecrypt
        implements CipherTextDecrypt
{
    private static final Charset CHARSET = UTF_8;
    private static final String RSA = "RSA";
    private static final String RSA_PADDING = "RSA/NONE/OAEPWITHSHA256AndMGF1Padding";
    private static final String BC_PROVIDER = "BC";
    private final SecurityKeyManager keyManager;

    @Inject
    public RsaCipherTextDecrypt(SecurityKeyManager keyManagerProvider)
    {
        this.keyManager = keyManagerProvider;
        Security.addProvider(new BouncyCastleProvider());
    }

    @Override
    public String decrypt(String keyName, String cipherText)
    {
        try {
            KeyFactory factory = KeyFactory.getInstance(RSA);
            // decode base64 of public key
            byte[] key = Base64.getDecoder().decode(keyManager.loadKey(keyName));
            // generate the public key
            X509EncodedKeySpec spec = new X509EncodedKeySpec(key);
            RSAPublicKey publicKey = (RSAPublicKey) factory.generatePublic(spec);
            Cipher cipher = Cipher.getInstance(RSA_PADDING, BC_PROVIDER);
            cipher.init(Cipher.DECRYPT_MODE, publicKey);
            // decode base64 of cipher text
            byte[] content = Base64.getDecoder().decode(cipherText);
            int keySize = publicKey.getModulus().bitLength();
            // decrypt cipher text
            return new String(rsaSplitCodec(cipher, content, keySize), CHARSET);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] rsaSplitCodec(Cipher cipher, byte[] content, int keySize)
    {
        int maxBlock = keySize / 8;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            int offset = 0;
            byte[] buff;
            int i = 0;
            while (content.length > offset) {
                if (content.length - offset > maxBlock) {
                    buff = cipher.doFinal(content, offset, maxBlock);
                }
                else {
                    buff = cipher.doFinal(content, offset, content.length - offset);
                }
                out.write(buff, 0, buff.length);
                i++;
                offset = i * maxBlock;
            }
            return out.toByteArray();
        }
        catch (IOException | BadPaddingException | IllegalBlockSizeException e) {
            throw new RuntimeException(e);
        }
    }
}
