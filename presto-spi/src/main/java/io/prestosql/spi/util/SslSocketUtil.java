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
package io.prestosql.spi.util;

import io.airlift.log.Logger;
import io.airlift.security.pem.PemReader;
import io.prestosql.spi.PrestoException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Collections.list;

public class SslSocketUtil
{
    private static final Logger log = Logger.get(SslSocketUtil.class);

    public static final String SSL_KEYSTORE = "javax.net.ssl.keyStore";
    public static final String SSL_KEYSTORE_PWD = "javax.net.ssl.keyStorePassword";
    public static final String SSL_TRUESTSTORE = "javax.net.ssl.trustStore";
    public static final String SSL_TRUESTSTORE_PWD = "javax.net.ssl.trustStorePassword";

    private SslSocketUtil() {}

    public static Optional<SSLContext> buildSslContext(boolean tlsEnabled)
    {
        if (!tlsEnabled) {
            return Optional.empty();
        }

        return SslSocketUtil.buildSslContext(
                Optional.ofNullable(System.getProperty(SSL_KEYSTORE)),
                Optional.ofNullable(System.getProperty(SSL_KEYSTORE_PWD)),
                Optional.ofNullable(System.getProperty(SSL_TRUESTSTORE)),
                Optional.ofNullable(System.getProperty(SSL_TRUESTSTORE_PWD)));
    }

    public static Optional<SSLContext> buildSslContext(
            Optional<String> keyStorePath,
            Optional<String> keyStorePassword,
            Optional<String> trustStorePath,
            Optional<String> trustStorePassword)
    {
        if ((!keyStorePath.isPresent() || keyStorePath.get().isEmpty()) && (!trustStorePath.isPresent() || trustStorePath.get().isEmpty())) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, " Trust store or keystore must be provided when TLS is enabled");
        }

        try {
            // load KeyStore if configured and get KeyManagers
            KeyManager[] keyManagers = null;
            KeyStore keyStore = null;
            if (keyStorePath.isPresent() && !keyStorePath.get().isEmpty()) {
                char[] keyManagerPassword;
                try {
                    // attempt to read the key store as a PEM file
                    keyStore = PemReader.loadKeyStore(new File(keyStorePath.get()), new File(keyStorePath.get()), keyStorePassword);
                    // for PEM encoded keys, the password is used to decrypt the specific key (and does not protect the keystore itself)
                    keyManagerPassword = new char[0];
                }
                catch (IOException | GeneralSecurityException ignored) {
                    keyManagerPassword = keyStorePassword.map(String::toCharArray).orElse(null);

                    keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                    try (InputStream in = new FileInputStream(keyStorePath.get())) {
                        keyStore.load(in, keyManagerPassword);
                    }
                }

                validateCertificates(keyStore);
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, keyManagerPassword);
                keyManagers = keyManagerFactory.getKeyManagers();
                log.info("KeyStore is loaded.");
            }

            // load TrustStore if configured, otherwise use KeyStore
            KeyStore trustStore = keyStore;
            if (trustStorePath.isPresent() && !trustStorePath.get().isEmpty()) {
                // load TrustStore
                trustStore = TrustStore.loadTrustStore(new File(trustStorePath.get()), trustStorePassword);
                log.info("TrustStore is loaded.");
            }

            // create TrustManagerFactory
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            // get X509TrustManager
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unexpected default trust managers:" + Arrays.toString(trustManagers));
            }

            // create SSLContext
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(keyManagers, trustManagers, null);
            log.info("SSLContext is initialized.");
            return Optional.of(sslContext);
        }
        catch (GeneralSecurityException | IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e.getMessage());
        }
    }

    private static void validateCertificates(KeyStore keyStore)
            throws GeneralSecurityException
    {
        for (String alias : list(keyStore.aliases())) {
            if (!keyStore.isKeyEntry(alias)) {
                continue;
            }
            Certificate certificate = keyStore.getCertificate(alias);
            if (!(certificate instanceof X509Certificate)) {
                continue;
            }

            try {
                ((X509Certificate) certificate).checkValidity();
            }
            catch (CertificateExpiredException e) {
                throw new CertificateExpiredException("KeyStore certificate is expired: " + e.getMessage());
            }
            catch (CertificateNotYetValidException e) {
                throw new CertificateNotYetValidException("KeyStore certificate is not yet valid: " + e.getMessage());
            }
        }
    }
}
