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

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelOptions;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.tcp.UnifiedProtocolDecoder;
import com.hazelcast.internal.server.tcp.UnifiedProtocolEncoder;
import com.hazelcast.spi.properties.HazelcastProperties;
import io.hetu.core.security.networking.ssl.SslConfig;
import io.hetu.core.security.networking.ssl.SslContext;
import io.hetu.core.security.networking.ssl.SslContextBuilder;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_KEEPALIVE;
import static com.hazelcast.internal.networking.ChannelOption.SO_LINGER;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.ChannelOption.TCP_NODELAY;
import static com.hazelcast.internal.server.ServerContext.KILO_BYTE;
import static com.hazelcast.spi.properties.ClusterProperty.SOCKET_BUFFER_DIRECT;
import static com.hazelcast.spi.properties.ClusterProperty.SOCKET_KEEP_ALIVE;
import static com.hazelcast.spi.properties.ClusterProperty.SOCKET_LINGER_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.SOCKET_NO_DELAY;
import static com.hazelcast.spi.properties.ClusterProperty.SOCKET_RECEIVE_BUFFER_SIZE;
import static com.hazelcast.spi.properties.ClusterProperty.SOCKET_SEND_BUFFER_SIZE;

@Aspect
@Component
public class UnifiedChannelInitializerAspect
{
    @Around("execution(* com.hazelcast.internal.server.tcp.UnifiedChannelInitializer.initChannel(Channel)) && args(channel)")
    public void aroundInitChannel(ProceedingJoinPoint joinPoint, Channel channel)
    {
        Field ioServiceField = null;
        Field propsField = null;
        try {
            ioServiceField = joinPoint.getTarget().getClass().getDeclaredField("serverContext");
            ioServiceField.setAccessible(true);
            ServerContext serverContext = (ServerContext) ioServiceField.get(joinPoint.getTarget());

            propsField = joinPoint.getTarget().getClass().getDeclaredField("props");
            propsField.setAccessible(true);
            HazelcastProperties props = (HazelcastProperties) propsField.get(joinPoint.getTarget());

            ChannelOptions config = channel.options();
            config.setOption(DIRECT_BUF, props.getBoolean(SOCKET_BUFFER_DIRECT))
                    .setOption(TCP_NODELAY, props.getBoolean(SOCKET_NO_DELAY))
                    .setOption(SO_KEEPALIVE, props.getBoolean(SOCKET_KEEP_ALIVE))
                    .setOption(SO_SNDBUF, props.getInteger(SOCKET_SEND_BUFFER_SIZE) * KILO_BYTE)
                    .setOption(SO_RCVBUF, props.getInteger(SOCKET_RECEIVE_BUFFER_SIZE) * KILO_BYTE)
                    .setOption(SO_LINGER, props.getSeconds(SOCKET_LINGER_SECONDS));

            UnifiedProtocolEncoder encoder = new UnifiedProtocolEncoder(serverContext);
            UnifiedProtocolDecoder decoder = new UnifiedProtocolDecoder(serverContext, encoder);

            channel.outboundPipeline().addLast(encoder);

            if (SslConfig.isSslEnabled()) {
                SslContext sslContext;
                if (channel.isClientMode()) {
                    sslContext = SslContextBuilder
                            .forClient()
                            .setupSsl()
                            .startTls(true)
                            .build();
                }
                else {
                    sslContext = SslContextBuilder
                            .forServer()
                            .setupSsl()
                            .startTls(true)
                            .build();
                }

                channel.outboundPipeline().addLast(sslContext.newSslOutboundHandler());
                channel.inboundPipeline().addLast(sslContext.newSslInboundHandler());
            }

            channel.inboundPipeline().addLast(decoder);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Cann't get class[%s] field.", joinPoint.getTarget().getClass().getName()));
        }
        finally {
            if (ioServiceField != null) {
                ioServiceField.setAccessible(false);
            }
            if (propsField != null) {
                propsField .setAccessible(false);
            }
        }
    }
}
