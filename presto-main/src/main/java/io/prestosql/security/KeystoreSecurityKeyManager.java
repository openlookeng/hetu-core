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

import io.airlift.log.Logger;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.security.SecurityKeyException;
import io.prestosql.spi.security.SecurityKeyManager;
import org.codehaus.plexus.util.IOUtil;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Base64;

import static java.lang.String.format;

public class KeystoreSecurityKeyManager
        implements SecurityKeyManager
{
    private static final Logger LOG = Logger.get(KeystoreSecurityKeyManager.class);
    private static final String SHARE_FS_CLIENT_CONFIG_NAME = "hdfs-config-catalog";
    private static final String UTF_8 = "UTF-8";
    private static final String PKCS12 = "pkcs12";
    private final FileSystemClientManager fileSystemClientManager;
    private final PasswordSecurityConfig config;

    @Inject
    public KeystoreSecurityKeyManager(FileSystemClientManager fileSystemClientManager, PasswordSecurityConfig config)
    {
        this.fileSystemClientManager = fileSystemClientManager;
        this.config = config;
    }

    /**
     * save publicKey or privateKey as a keystore into hdfs
     *
     * @param key publicKey or privateKey, type is String
     * @param catalogName key belong to catalogName
     * @throws SecurityKeyException throw exception as SecurityKeyException
     */
    @Override
    public synchronized void saveKey(String key, String catalogName)
            throws SecurityKeyException
    {
        if (key == null || key.isEmpty()) {
            LOG.info("key is null or empty, will not create keystore for catalog[%s].", catalogName);
            return;
        }
        createStoreDirIfNotExists();
        createAndSaveKeystore(key, catalogName);
    }

    /**
     * load publicKey or privateKey from keystore in hdfs
     *
     * @param catalogName catalog name that the key belong to
     * @return return the key as string
     * @throws SecurityKeyException throw exception as SecurityKeyException
     */
    @Override
    public synchronized String loadKey(String catalogName)
            throws SecurityKeyException
    {
        Path keystorePath = Paths.get(config.getFileStorePath());
        String keyStr = "";
        try (HetuFileSystemClient hetuFileSystemClient =
                     fileSystemClientManager.getFileSystemClient(SHARE_FS_CLIENT_CONFIG_NAME, Paths.get("/"));
                InputStream inputStream = hetuFileSystemClient.newInputStream(keystorePath)) {
            KeyStore keyStore = KeyStore.getInstance(PKCS12);
            keyStore.load(inputStream, config.getKeystorePassword().toCharArray());
            Key key = keyStore.getKey(catalogName, config.getKeystorePassword().toCharArray());

            if (key instanceof SecretKey) {
                keyStr = new String(Base64.getDecoder().decode(key.getEncoded()), Charset.forName(UTF_8));
                LOG.info("success to load key for catalog[%s]...", catalogName);
            }
            else if (key instanceof PrivateKey) {
                Certificate certificate = keyStore.getCertificate(catalogName);
                PublicKey publicKey = certificate.getPublicKey();
                keyStr = new String(Base64.getEncoder().encode(publicKey.getEncoded()), Charset.forName(UTF_8));
            }

            if (key == null) {
                Certificate certificate = keyStore.getCertificate(catalogName);
                if (certificate != null) {
                    PublicKey publicKey = certificate.getPublicKey();
                    keyStr = new String(Base64.getEncoder().encode(publicKey.getEncoded()), Charset.forName(UTF_8));
                }
            }
        }
        catch (KeyStoreException e) {
            throw new SecurityKeyException(format("something wrong when use KeyStore: %s", e.getMessage()));
        }
        catch (NoSuchAlgorithmException e) {
            throw new SecurityKeyException(format("not exists such algorithm:", e.getMessage()));
        }
        catch (CertificateException e) {
            throw new SecurityKeyException(format("certification is error: %s", e.getMessage()));
        }
        catch (UnrecoverableKeyException e) {
            throw new SecurityKeyException(format("not found the key for catalog[%s]: %s", catalogName, e.getMessage()));
        }
        catch (IOException e) {
            throw new SecurityKeyException(format("error happened when load key from keystore  %s", e.getMessage()));
        }
        return keyStr;
    }

    /**
     * get the key by catalog name
     *
     * @param catalogName catalog name
     * @return the key, if not exist, return null
     */
    @Override
    public synchronized String getKey(String catalogName)
    {
        String key;
        try {
            key = loadKey(catalogName);
        }
        catch (SecurityKeyException e) {
            key = null;
            LOG.warn("the %s is not exist.", catalogName);
        }
        return key;
    }

    @Override
    public synchronized void deleteKey(String catalogName)
            throws SecurityKeyException
    {
        Path keystorPath = Paths.get(config.getFileStorePath());
        KeyStore keyStore;
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try (HetuFileSystemClient hetuFileSystemClient =
                     fileSystemClientManager.getFileSystemClient(SHARE_FS_CLIENT_CONFIG_NAME, Paths.get("/"))) {
            inputStream = hetuFileSystemClient.newInputStream(keystorPath);
            keyStore = KeyStore.getInstance(PKCS12);
            keyStore.load(inputStream, config.getKeystorePassword().toCharArray());
            keyStore.deleteEntry(catalogName);
            outputStream = hetuFileSystemClient.newOutputStream(keystorPath);
            keyStore.store(outputStream, config.getKeystorePassword().toCharArray());
            LOG.info("success to delete the alias[%s] from keystore file.", catalogName);
        }
        catch (KeyStoreException e) {
            throw new SecurityKeyException(format("something wrong when use KeyStore: %s", e.getMessage()));
        }
        catch (NoSuchAlgorithmException e) {
            throw new SecurityKeyException("not exists 'AES' algorithm");
        }
        catch (CertificateException e) {
            throw new SecurityKeyException(format("certification is error: %s", e.getMessage()));
        }
        catch (IOException e) {
            throw new SecurityKeyException(format("error in I/O: fail to delete alias[%s] from keystore.", catalogName));
        }
        finally {
            IOUtil.close(inputStream);
            IOUtil.close(outputStream);
        }
    }

    private void createAndSaveKeystore(String key, String catalogName)
            throws SecurityKeyException
    {
        Path keystorPath = Paths.get(config.getFileStorePath());

        byte[] keyBytes = Base64.getEncoder().encode(key.getBytes(Charset.forName(UTF_8)));
        SecretKey secretKey = new SecretKeySpec(keyBytes, 0, keyBytes.length, "AES");

        InputStream inputStream = null;
        OutputStream outputStream = null;
        try (HetuFileSystemClient hetuFileSystemClient =
                     fileSystemClientManager.getFileSystemClient(SHARE_FS_CLIENT_CONFIG_NAME, Paths.get("/"))) {
            boolean isStoreFileExists = hetuFileSystemClient.exists(keystorPath);
            KeyStore keyStore = KeyStore.getInstance(PKCS12);
            if (isStoreFileExists) {
                inputStream = hetuFileSystemClient.newInputStream(keystorPath);
                keyStore.load(inputStream, config.getKeystorePassword().toCharArray());
            }
            else {
                keyStore.load(null, null);
            }
            keyStore.setEntry(catalogName, new KeyStore.SecretKeyEntry(secretKey), new KeyStore.PasswordProtection(config.getKeystorePassword().toCharArray()));

            outputStream = hetuFileSystemClient.newOutputStream(keystorPath);
            keyStore.store(outputStream, config.getKeystorePassword().toCharArray());
            LOG.info("success to save the key for catalog[%s]..", catalogName);
        }
        catch (KeyStoreException e) {
            throw new SecurityKeyException(format("something wrong when use KeyStore: %s", e.getMessage()));
        }
        catch (NoSuchAlgorithmException e) {
            throw new SecurityKeyException("not exists 'RSA' algorithm");
        }
        catch (CertificateException e) {
            throw new SecurityKeyException(format("certification is error: %s", e.getMessage()));
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new SecurityKeyException("error in I/O: create file failed.");
        }
        finally {
            IOUtil.close(inputStream);
            IOUtil.close(outputStream);
        }
    }

    private void createStoreDirIfNotExists()
    {
        String file = config.getFileStorePath();
        try (HetuFileSystemClient hetuFileSystemClient =
                     fileSystemClientManager.getFileSystemClient(SHARE_FS_CLIENT_CONFIG_NAME, Paths.get("/"))) {
            int lastIndex = file.lastIndexOf(File.separator);
            String tmpFileDir = file.substring(0, lastIndex);
            if (hetuFileSystemClient.exists(Paths.get(tmpFileDir))) {
                return;
            }

            hetuFileSystemClient.createDirectories(Paths.get(tmpFileDir));
            LOG.info("success to create the store directories...");
        }
        catch (IOException e) {
            LOG.error("fail to create the store directories: %s", e.getMessage());
            throw new RuntimeException("fail to create the store directories.");
        }
    }
}
