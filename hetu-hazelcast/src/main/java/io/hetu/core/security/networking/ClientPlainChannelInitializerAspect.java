/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hetu.core.security.networking;

import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.connection.nio.ClientProtocolEncoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.util.ClientMessageEncoder;
import com.hazelcast.internal.networking.Channel;
import io.hetu.core.security.networking.ssl.SslConfig;
import io.hetu.core.security.networking.ssl.SslContext;
import io.hetu.core.security.networking.ssl.SslContextBuilder;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.function.Consumer;

import static com.hazelcast.client.config.SocketOptions.KILO_BYTE;
import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_KEEPALIVE;
import static com.hazelcast.internal.networking.ChannelOption.SO_LINGER;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_REUSEADDR;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_TIMEOUT;
import static com.hazelcast.internal.networking.ChannelOption.TCP_NODELAY;

@Aspect
@Component
public class ClientPlainChannelInitializerAspect
{
    @Around("execution(* com.hazelcast.client.impl.connection.nio.ClientPlainChannelInitializer.initChannel(Channel)) && args(channel)")
    public void aroundInitChannel(ProceedingJoinPoint joinPoint, Channel channel)
    {
        Field directBufferField = null;
        Field socketOptionsField = null;
        try {
            directBufferField = joinPoint.getTarget().getClass().getDeclaredField("directBuffer");
            directBufferField.setAccessible(true);
            boolean directBuffer = (boolean) directBufferField.get(joinPoint.getTarget());

            socketOptionsField = joinPoint.getTarget().getClass().getDeclaredField("socketOptions");
            socketOptionsField.setAccessible(true);
            SocketOptions socketOptions = (SocketOptions) socketOptionsField.get(joinPoint.getTarget());

            channel.options()
                    .setOption(SO_SNDBUF, KILO_BYTE * socketOptions.getBufferSize())
                    .setOption(SO_RCVBUF, KILO_BYTE * socketOptions.getBufferSize())
                    .setOption(SO_REUSEADDR, socketOptions.isReuseAddress())
                    .setOption(SO_KEEPALIVE, socketOptions.isKeepAlive())
                    .setOption(SO_LINGER, socketOptions.getLingerSeconds())
                    .setOption(SO_TIMEOUT, 0)
                    .setOption(TCP_NODELAY, socketOptions.isTcpNoDelay())
                    .setOption(DIRECT_BUF, directBuffer);

            final ClientConnection connection = (ClientConnection) channel.attributeMap().get(ClientConnection.class);

            ClientMessageDecoder decoder = new ClientMessageDecoder(connection, new Consumer<ClientMessage>() {
                @Override
                public void accept(ClientMessage message)
                {
                    connection.handleClientMessage(message);
                }
            }, null);

            if (SslConfig.isSslEnabled()) {
                SslContext sslContext = SslContextBuilder
                        .forClient()
                        .setupSsl()
                        .startTls(true)
                        .build();
                channel.inboundPipeline().addLast(sslContext.newSslInboundHandler());
                channel.inboundPipeline().addLast(decoder);
                channel.outboundPipeline().addLast(new ClientMessageEncoder());
                channel.outboundPipeline().addLast(new ClientProtocolEncoder());
                channel.outboundPipeline().addLast(sslContext.newSslOutboundHandler());
            }
            else {
                channel.inboundPipeline().addLast(decoder);
                channel.outboundPipeline().addLast(new ClientMessageEncoder());
                channel.outboundPipeline().addLast(new ClientProtocolEncoder());
            }
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Cann't get class[%s] field.", joinPoint.getTarget().getClass().getName()));
        }
        finally {
            if (directBufferField != null) {
                directBufferField.setAccessible(false);
            }
            if (socketOptionsField != null) {
                socketOptionsField .setAccessible(false);
            }
        }
    }
}
