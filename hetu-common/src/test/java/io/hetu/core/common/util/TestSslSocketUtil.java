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
package io.hetu.core.common.util;

import org.testng.annotations.Test;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestSslSocketUtil
{
    @Test
    public void testGetAvailablePort()
            throws IOException
    {
        // the method starts trying ports at 60000, so the first available port should be 60000
        assertEquals(SslSocketUtil.getAvailablePort(), 60000, "did not get expected available port");

        // because the method uses a static variable to keep track of which port to try next
        // the next port it will try is 60001, so make it unavailable to test the case
        // when port is already used
        ServerSocketFactory sslServerSocketFactory = SSLServerSocketFactory.getDefault();
        try (SSLServerSocket socket = (SSLServerSocket) sslServerSocketFactory.createServerSocket(60001)) {
            // because 60001 was already used, the next available port is 60002
            assertEquals(SslSocketUtil.getAvailablePort(), 60002, "did not get expected available port");
        }
    }

    @Test
    public void testBuildSslContext()
            throws GeneralSecurityException
    {
        System.setProperty("javax.net.ssl.keyStore", "src/test/resources/keystore.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "changed");

        Optional<SSLContext> context = SslSocketUtil.buildSslContext(true);
    }
}
