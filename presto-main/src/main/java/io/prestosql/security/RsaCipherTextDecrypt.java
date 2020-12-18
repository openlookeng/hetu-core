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
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RsaCipherTextDecrypt
        implements CipherTextDecrypt
{
    private static final Charset CHARSET = UTF_8;
    private static final String RSA = "RSA";
    private static final String BC_PROVIDER = "BC";
    private final SecurityKeyManager keyManager;
    private final PasswordSecurityConfig config;

    @Inject
    public RsaCipherTextDecrypt(SecurityKeyManager keyManagerProvider, PasswordSecurityConfig config)
    {
        this.keyManager = keyManagerProvider;
        this.config = config;
        Security.addProvider(new BouncyCastleProvider());
    }

    @Override
    public String decrypt(String keyName, String cipherText)
    {
        try {
            KeyFactory factory = KeyFactory.getInstance(RSA);
            // decode base64 of private key
            char[] secretKey = keyManager.getKey(keyName);
            if (secretKey == null) {
                throw new RuntimeException(format("%s not exist.", keyName));
            }
            byte[] key = Base64.getDecoder().decode(new String(secretKey));
            // generate the private key
            PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(key);
            RSAPrivateKey privateKey = (RSAPrivateKey) factory.generatePrivate(pkcs8EncodedKeySpec);
            Cipher cipher = Cipher.getInstance(config.getRsaPadding(), BC_PROVIDER);
            cipher.init(Cipher.DECRYPT_MODE, privateKey);
            // decode base64 of cipher text
            byte[] content = Base64.getDecoder().decode(cipherText);
            int keySize = privateKey.getModulus().bitLength();
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
