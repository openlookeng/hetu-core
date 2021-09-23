/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.httpserver;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ServerSocketChannel;

public class HetuHttpServerInfo
        extends HttpServerInfo
{
    private static final Logger LOG = Logger.get(HetuHttpServerInfo.class);
    private URI httpUri;
    private URI httpExternalUri;
    private URI httpsUri;
    private URI httpsExternalUri;
    private URI adminUri;
    private URI adminExternalUri;
    private ServerSocketChannel httpChannel;
    private ServerSocketChannel httpsChannel;
    private ServerSocketChannel adminChannel;

    private static final int MIN_PORT_NUMBER = 1100;
    private static final int MAX_PORT_NUMBER = 65535;

    @Inject
    public HetuHttpServerInfo(HttpServerConfig config, NodeInfo nodeInfo)
    {
        super(new HttpServerConfig().setHttpEnabled(false).setHttpsEnabled(false).setAdminEnabled(false), nodeInfo);
        // http channel
        if (config.isHttpEnabled()) {
            this.httpChannel = createChannel(config.getHttpPort(), config.getHttpAcceptQueueSize());
            if (config.getHttpPort() != port(this.httpChannel)) {
                config.setHttpPort(port(this.httpChannel));
            }
            this.httpUri = buildUri("http", nodeInfo.getInternalAddress(), port(this.httpChannel));
            this.httpExternalUri = buildUri("http", nodeInfo.getExternalAddress(), this.httpUri.getPort());
            LOG.info(String.format("HetuHttpServerInfo http channel bind to port %s", port(this.httpChannel)));
        }
        else {
            httpChannel = null;
            httpUri = null;
            httpExternalUri = null;
        }
        setBaseChannel(HttpServerInfo.class, "httpChannel", httpChannel);
        // https channel
        if (config.isHttpsEnabled()) {
            this.httpsChannel = createChannel(config.getHttpsPort(), config.getHttpAcceptQueueSize());
            if (config.getHttpsPort() != port(this.httpsChannel)) {
                config.setHttpsPort(port(this.httpsChannel));
            }
            this.httpsUri = buildUri("https", nodeInfo.getInternalAddress(), port(this.httpsChannel));
            this.httpsExternalUri = buildUri("https", nodeInfo.getExternalAddress(), this.httpsUri.getPort());
            LOG.info(String.format("HetuHttpServerInfo https channel bind to port %s", port(this.httpsChannel)));
        }
        else {
            httpsChannel = null;
            httpsUri = null;
            httpsExternalUri = null;
        }
        setBaseChannel(HttpServerInfo.class, "httpsChannel", httpsChannel);
        // admin channel
        if (config.isAdminEnabled()) {
            this.adminChannel = createChannel(config.getAdminPort(), config.getHttpAcceptQueueSize());
            if (config.isHttpsEnabled()) {
                this.adminUri = buildUri("https", nodeInfo.getInternalAddress(), port(this.adminChannel));
                this.adminExternalUri = buildUri("https", nodeInfo.getExternalAddress(), this.adminUri.getPort());
            }
            else {
                this.adminUri = buildUri("http", nodeInfo.getInternalAddress(), port(this.adminChannel));
                this.adminExternalUri = buildUri("http", nodeInfo.getExternalAddress(), this.adminUri.getPort());
            }
            if (config.getAdminPort() != port(this.adminChannel)) {
                config.setAdminPort(port(this.adminChannel));
            }
        }
        else {
            adminChannel = null;
            adminUri = null;
            adminExternalUri = null;
        }
        setBaseChannel(HttpServerInfo.class, "adminChannel", adminChannel);
    }

    private void setBaseChannel(Class<HttpServerInfo> clazz, String name, Object value)
    {
        Field channel = null;
        try {
            channel = clazz.getDeclaredField(name);
        }
        catch (NoSuchFieldException e) {
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "No such channel found");
        }
        boolean oldVal = channel.isAccessible();
        try {
            channel.setAccessible(true);
            channel.set(this, value);
        }
        catch (IllegalAccessException e) {
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Channel not accessible");
        }
        finally {
            channel.setAccessible(oldVal);
        }
    }

    public URI getHttpUri()
    {
        return this.httpUri;
    }

    public URI getHttpExternalUri()
    {
        return this.httpExternalUri;
    }

    public URI getHttpsUri()
    {
        return this.httpsUri;
    }

    public URI getHttpsExternalUri()
    {
        return this.httpsExternalUri;
    }

    public URI getAdminUri()
    {
        return this.adminUri;
    }

    public URI getAdminExternalUri()
    {
        return this.adminExternalUri;
    }

    ServerSocketChannel getHttpChannel()
    {
        return this.httpChannel;
    }

    ServerSocketChannel getHttpsChannel()
    {
        return this.httpsChannel;
    }

    ServerSocketChannel getAdminChannel()
    {
        return this.adminChannel;
    }

    private static URI buildUri(String scheme, String host, int port)
    {
        try {
            return new URI(scheme, (String) null, host, port, (String) null, (String) null, (String) null);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @VisibleForTesting
    static int port(ServerSocketChannel channel)
    {
        try {
            return ((InetSocketAddress) channel.getLocalAddress()).getPort();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ServerSocketChannel createChannel(int port, int acceptQueueSize)
    {
        ServerSocketChannel channel = null;
        int nextPort = port;
        while (true) {
            try {
                channel = ServerSocketChannel.open();
                channel.socket().setReuseAddress(true);
                channel.socket().bind(new InetSocketAddress(nextPort), acceptQueueSize);
                return channel;
            }
            catch (IOException e) {
                // cannot bind to this port
                nextPort++;

                if (nextPort < MIN_PORT_NUMBER || nextPort > MAX_PORT_NUMBER) {
                    LOG.warn("Failed to bind port withing acceptable limit");
                    nextPort = MIN_PORT_NUMBER;
                }
            }
        }
    }
}
