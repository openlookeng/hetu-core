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

import com.hazelcast.internal.networking.ChannelOptions;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.nio.NioChannel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.IOUtil.newByteBuffer;
import static java.lang.String.format;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;

public class SslInboundHandler
        extends InboundHandler<ByteBuffer, ByteBuffer>
{
    private final ILogger logger;
    private SSLEngine engine;
    private AtomicBoolean handshakeFinished;
    private boolean ignoreHandlerAdded;

    public SslInboundHandler(SSLEngine engine, AtomicBoolean handshakeFinished)
    {
        this.engine = engine;
        this.logger = Logger.getLogger(getClass());
        this.handshakeFinished = handshakeFinished;
    }

    private void initDstBuffer()
    {
        ChannelOptions config = channel.options();
        this.dst = newByteBuffer(config.getOption(SO_RCVBUF), config.getOption(DIRECT_BUF));
    }

    @Override
    public void handlerAdded()
    {
        if (!ignoreHandlerAdded) {
            initSrcBuffer();
            initDstBuffer();
        }
        else {
            ignoreHandlerAdded = false;
        }
    }

    @Override
    public HandlerStatus onRead()
            throws Exception
    {
        src.flip();
        try {
            ChannelOptions config = channel.options();
            ByteBuffer byteBuffer = newByteBuffer(config.getOption(SO_RCVBUF), config.getOption(DIRECT_BUF));

            while (src.hasRemaining()) {
                if (logger.isFineEnabled()) {
                    logger.fine(format("channel=%s,before unwrap, src=[%s, %s, %s]", channel, src.position(), src.limit(), src.capacity()));
                }
                SSLEngineResult result = engine.unwrap(src, byteBuffer);
                if (logger.isFineEnabled()) {
                    logger.fine(format("channel=%s,after unwrap, src=[%s, %s, %s], byteBuffer=[%s, %s, %s], status=[%s, %s]",
                            channel, src.position(), src.limit(), src.capacity(), byteBuffer.position(), byteBuffer.limit(), byteBuffer.capacity(),
                            result.getStatus(), result.getHandshakeStatus()));
                }
                switch (result.getStatus()) {
                    case BUFFER_OVERFLOW:
                        byteBuffer = enlargeApplicationBuffer(byteBuffer);
                        break;
                    case BUFFER_UNDERFLOW:
                        if (engine.getSession().getPacketBufferSize() > src.capacity()) {
                            ByteBuffer newBuffer = enlargePacketBuffer(src);
                            newBuffer.put(src);
                            src = newBuffer;
                            updateInboundPipeline();
                            if (logger.isFineEnabled()) {
                                logger.fine(format("BUFFER_UNDERFLOW, enlarge src bytebuffer,channel=%s, src=[%s, %s, %s]", channel, src.position(), src.limit(), src.capacity()));
                            }
                        }
                        return HandlerStatus.CLEAN;
                    case OK:
                        break;
                    case CLOSED:
                        if (!engine.isOutboundDone()) {
                            engine.closeOutbound();
                        }
                        break;
                    default:
                        throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                }

                SSLEngineResult.HandshakeStatus handshakeStatus = result.getHandshakeStatus();

                outer:
                while (true) {
                    switch (handshakeStatus) {
                        case NOT_HANDSHAKING: // $FALL-THROUGH$
                        case FINISHED:
                            if (logger.isFineEnabled()) {
                                logger.fine("handshakeStatus: " + handshakeStatus);
                            }
                            setHandshakeSuccess();
                            break outer;
                        case NEED_TASK:
                            if (logger.isFineEnabled()) {
                                logger.fine("handshakeStatus: need-task");
                            }
                            runAllDelegatedTasks();
                            handshakeStatus = engine.getHandshakeStatus();
                            break;
                        case NEED_WRAP:
                            // We just wrap a non app data, and send out.
                            wrapNonAppData();
                            handshakeStatus = engine.getHandshakeStatus();
                            break;
                        case NEED_UNWRAP:
                            // If it is NEED_UNWRAP, it need some message to be unwrapped,
                            // so we need wakeup inbound pipeline to get more message from SocketChannel.
                            wakeupInbound();
                            break outer;
                        default:
                            throw new IllegalStateException("unknown handshake status: " + handshakeStatus);
                    }
                }

                if (handshakeStatus == NOT_HANDSHAKING || handshakeStatus == FINISHED) {
                    // If finished handshake, then putting the byteBuffer to dst.
                    byteBuffer.flip();
                    dst = adapterByteBufferIfNotEnough(dst, byteBuffer.limit());
                    dst.put(byteBuffer);
                    compactOrClear(byteBuffer);
                    if (logger.isFineEnabled()) {
                        logger.fine(format("channel=%s, write src to dst=[%s, %s, %s]", channel, dst.position(), dst.limit(), dst.capacity()));
                    }
                    // Do not call flip() here, because it will flip() at the start of next handler.
                }
            }
            return HandlerStatus.CLEAN;
        }
        catch (SSLException sslException) {
            engine.closeOutbound();
            throw new SSLException("A problem was encountered while processing data caused the SSLEngine to abort. try to close connection..." + sslException.getMessage());
        }
        finally {
            compactOrClear(src);
        }
    }

    private void wakeupInbound()
    {
        channel.inboundPipeline().wakeup();
    }

    private void wakeupOutbound()
    {
        channel.outboundPipeline().wakeup();
    }

    private void updateInboundPipeline()
    {
        ignoreHandlerAdded = true;
        channel.inboundPipeline().replace(this, this);
    }

    private void wrapNonAppData()
            throws IOException
    {
        ChannelOptions config = channel.options();
        ByteBuffer appData = newByteBuffer(0, config.getOption(DIRECT_BUF));
        ByteBuffer netData = newByteBuffer(config.getOption(SO_SNDBUF), config.getOption(DIRECT_BUF));
        engine.wrap(appData, netData);
        // Flip dst buffer, then buffer could be read.
        netData.flip();
        // Send message directly.
        ((NioChannel) channel).socketChannel().write(netData);
    }

    private void setHandshakeSuccess()
    {
        if (!handshakeFinished.get()) {
            // If finished handshake, we need wakeup outbound pipeline,
            // it will remove BLOCKED status, and continue to handle messages.
            wakeupOutbound();
            handshakeFinished.compareAndSet(false, true);
        }
    }

    private void runAllDelegatedTasks()
    {
        for (; ; ) {
            Runnable task = engine.getDelegatedTask();
            if (task == null) {
                return;
            }
            SslContext.executors.execute(task);
        }
    }

    private ByteBuffer enlargeApplicationBuffer(ByteBuffer buffer)
    {
        ChannelOptions config = channel.options();
        int appBufferSize = engine.getSession().getApplicationBufferSize();
        if (logger.isFineEnabled()) {
            logger.fine(format("begin to enlargeApplicationBuffer, channel=%s, buffer=[%s, %s, %s]", channel, buffer.position(), buffer.limit(), buffer.capacity()));
        }
        ByteBuffer tmpBuffer = appBufferSize > buffer.capacity() ? newByteBuffer(appBufferSize, config.getOption(DIRECT_BUF)) : ByteBuffer.allocate(buffer.capacity() * 2);
        if (logger.isFineEnabled()) {
            logger.fine(format("end to enlargeApplicationBuffer, channel=%s, buffer=[%s, %s, %s]", channel, tmpBuffer.position(), tmpBuffer.limit(), tmpBuffer.capacity()));
        }
        return tmpBuffer;
    }

    private ByteBuffer enlargePacketBuffer(ByteBuffer buffer)
    {
        ChannelOptions config = channel.options();
        int appBufferSize = engine.getSession().getPacketBufferSize();
        if (logger.isFineEnabled()) {
            logger.fine(format("begin to enlargePacketBuffer, channel=%s, buffer=[%s, %s, %s]", channel, buffer.position(), buffer.limit(), buffer.capacity()));
        }
        ByteBuffer tmpBuffer = appBufferSize > buffer.capacity() ? newByteBuffer(appBufferSize, config.getOption(DIRECT_BUF)) : ByteBuffer.allocate(buffer.capacity() * 2);
        if (logger.isFineEnabled()) {
            logger.fine(format("end to enlargePacketBuffer, channel=%s, buffer=[%s, %s, %s]", channel, tmpBuffer.position(), tmpBuffer.limit(), tmpBuffer.capacity()));
        }
        return tmpBuffer;
    }

    private ByteBuffer adapterByteBufferIfNotEnough(ByteBuffer buffer, int otherBufferLimit)
    {
        ChannelOptions config = channel.options();
        ByteBuffer tmpBuffer = buffer;
        if (buffer.capacity() < otherBufferLimit) {
            if (logger.isFineEnabled()) {
                logger.fine(format("adapterByteBufferIfNotEnough, begin to enlarge, channel=%s, buffer=[%s, %s, %s]", channel, tmpBuffer.position(), tmpBuffer.limit(), tmpBuffer.capacity()));
            }
            tmpBuffer = newByteBuffer(otherBufferLimit, config.getOption(DIRECT_BUF));
            updateInboundPipeline();
            if (logger.isFineEnabled()) {
                logger.fine(format("adapterByteBufferIfNotEnough, end to enlarge, channel=%s, buffer=[%s, %s, %s]", channel, tmpBuffer.position(), tmpBuffer.limit(), tmpBuffer.capacity()));
            }
        }

        return tmpBuffer;
    }
}
