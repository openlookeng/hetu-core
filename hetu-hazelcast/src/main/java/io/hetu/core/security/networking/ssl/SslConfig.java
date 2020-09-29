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

import java.util.Optional;

public class SslConfig
{
    private static boolean sslEnabled;
    private static Optional<String> keyStorePath = Optional.empty();
    private static Optional<String> keyStorePassword = Optional.empty();
    private static Optional<String> trustStorePath = Optional.empty();
    private static Optional<String> trustStorePassword = Optional.empty();
    private static Optional<String> cipherSuites = Optional.of("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
    private static Optional<String> sslProtocols = Optional.of("TLSv1.2");

    private SslConfig() {}

    public static boolean isSslEnabled()
    {
        return sslEnabled;
    }

    public static void setSslEnabled(boolean enabled)
    {
        sslEnabled = enabled;
    }

    public static Optional<String> getKeyStorePath()
    {
        return keyStorePath;
    }

    public static void setKeyStorePath(String path)
    {
        keyStorePath = Optional.ofNullable(path);
    }

    public static Optional<String> getKeyStorePassword()
    {
        return keyStorePassword;
    }

    public static void setKeyStorePassword(String password)
    {
        keyStorePassword = Optional.ofNullable(password);
    }

    public static Optional<String> getTrustStorePath()
    {
        return trustStorePath;
    }

    public static void setTrustStorePath(String path)
    {
        trustStorePath = Optional.ofNullable(path);
    }

    public static Optional<String> getTrustStorePassword()
    {
        return trustStorePassword;
    }

    public static void setTrustStorePassword(String password)
    {
        trustStorePassword = Optional.ofNullable(password);
    }

    public static void setCipherSuites(String suites)
    {
        if (suites != null && !suites.equals("")) {
            cipherSuites = Optional.of(suites);
        }
    }

    public static Optional<String> getCipherSuites()
    {
        return cipherSuites;
    }

    public static void setProtocols(String protocols)
    {
        if (protocols != null && !protocols.equals("")) {
            sslProtocols = Optional.of(protocols);
        }
    }

    public static Optional<String> getSslProtocols()
    {
        return sslProtocols;
    }
}
