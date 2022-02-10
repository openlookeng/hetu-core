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

import io.airlift.log.Logger;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

public class SslSocketUtil
{
    private static final Logger log = Logger.get(SslSocketUtil.class);

    private static final int MIN_PORT_NUMBER = 1100;
    private static final int MAX_PORT_NUMBER = 65535;

    private static final AtomicInteger nextPortToTry = new AtomicInteger(60000);

    private SslSocketUtil()
    {
    }

    public static Optional<SSLContext> buildSslContext(boolean tlsEnabled)
            throws GeneralSecurityException
    {
        if (!tlsEnabled) {
            return Optional.empty();
        }
        return Optional.of(SSLContext.getDefault());
    }

    /**
     * Returns a random available port.
     *
     * @return
     */
    public static int getAvailablePort()
    {
        while (!isPortAvailable(nextPortToTry.get())) {
            nextPortToTry.getAndIncrement();
        }

        return nextPortToTry.getAndIncrement();
    }

    private static boolean isPortAvailable(int port)
    {
        if (port < MIN_PORT_NUMBER || port > MAX_PORT_NUMBER) {
            throw new IllegalArgumentException(format("Invalid port: %s, the port number must range from %s to %s", port, MIN_PORT_NUMBER, MAX_PORT_NUMBER));
        }

        ServerSocketFactory sslServerSocketFactory = SSLServerSocketFactory.getDefault();
        try (SSLServerSocket socket = (SSLServerSocket) sslServerSocketFactory.createServerSocket(port)) {
            return true;
        }
        catch (IOException e) {
            log.warn(e, "Port " + port + " was unavailable.");
            return false;
        }
    }
}
