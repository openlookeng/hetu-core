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
package io.hetu.core.security.networking.ssl;

import io.airlift.security.pem.PemReader;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

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
import java.util.List;
import java.util.Optional;

import static java.util.Collections.list;

public final class SslContextBuilder
{
    private final boolean forClient;
    private boolean startTls;
    private long sessionTimeout = 10000;
    private SSLEngine engine;

    private SslContextBuilder(boolean forClient)
    {
        this.forClient = forClient;
    }

    public static SslContextBuilder forClient()
    {
        return new SslContextBuilder(true);
    }

    public static SslContextBuilder forServer()
    {
        return new SslContextBuilder(false);
    }

    public SslContextBuilder startTls(boolean startTls)
    {
        this.startTls = startTls;
        return this;
    }

    public SslContextBuilder sessionTimeout(long sessionTimeout)
    {
        this.sessionTimeout = sessionTimeout;
        return this;
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

    private static KeyStore loadTrustStore(File trustStorePath, Optional<String> trustStorePassword)
            throws IOException, GeneralSecurityException
    {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            // attempt to read the trust store as a PEM file
            List<X509Certificate> certificateChain = PemReader.readCertificateChain(trustStorePath);
            if (!certificateChain.isEmpty()) {
                trustStore.load(null, null);
                for (X509Certificate certificate : certificateChain) {
                    X500Principal principal = certificate.getSubjectX500Principal();
                    trustStore.setCertificateEntry(principal.getName(), certificate);
                }
                return trustStore;
            }
        }
        catch (IOException | GeneralSecurityException ignored) {
        }

        try (InputStream in = new FileInputStream(trustStorePath)) {
            trustStore.load(in, trustStorePassword.map(String::toCharArray).orElse(null));
        }
        return trustStore;
    }

    public SslContextBuilder setupSsl()
    {
        Optional<String> keyStorePath = SslConfig.getKeyStorePath();
        Optional<String> keyStorePassword = SslConfig.getKeyStorePassword();
        Optional<String> trustStorePath = SslConfig.getTrustStorePath();
        Optional<String> trustStorePassword = SslConfig.getTrustStorePassword();

        if (!keyStorePath.isPresent() && !trustStorePath.isPresent()) {
            return this;
        }

        try {
            // load KeyStore if configured and get KeyManagers
            KeyStore keyStore = null;
            KeyManager[] keyManagers = null;
            if (keyStorePath.isPresent()) {
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
            }

            // load TrustStore if configured, otherwise use KeyStore
            KeyStore trustStore = keyStore;
            if (trustStorePath.isPresent()) {
                trustStore = loadTrustStore(new File(trustStorePath.get()), trustStorePassword);
            }

            // create TrustManagerFactory
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            // get X509TrustManager
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            if ((trustManagers.length != 1) || !(trustManagers[0] instanceof X509TrustManager)) {
                throw new RuntimeException("Unexpected default trust managers");
            }
            X509TrustManager trustManager = (X509TrustManager) trustManagers[0];

            // create SSLContext
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagers, new TrustManager[] {trustManager}, null);
            this.engine = sslContext.createSSLEngine();
            this.engine.setUseClientMode(forClient);

            Optional<String> cipherSuites = SslConfig.getCipherSuites();
            Optional<String> sslProtocols = SslConfig.getSslProtocols();

            this.engine.setEnabledCipherSuites(cipherSuites.get().split(","));
            this.engine.setEnabledProtocols(sslProtocols.get().split(","));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException("Setup ssl failed, cause by " + e.getCause());
        }
        return this;
    }

    public SslContext build()
    {
        return new SslContext(engine, startTls, sessionTimeout);
    }
}
