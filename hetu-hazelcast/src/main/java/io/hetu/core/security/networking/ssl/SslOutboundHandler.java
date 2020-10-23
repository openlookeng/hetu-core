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
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.IOUtil.newByteBuffer;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.String.format;

public class SslOutboundHandler
        extends OutboundHandler<ByteBuffer, ByteBuffer>
{
    private final long handshakeTimeoutMillis;
    private final boolean startTls;
    private final ILogger logger;
    private boolean sentFirstMessage;
    private boolean handshakeStarted;
    private AtomicBoolean handshakeFinished;
    private final SSLEngine engine;
    private boolean isTimeout;
    private ScheduledFuture timeoutFuture;
    private boolean ignoreHandlerAdded;

    public SslOutboundHandler(SSLEngine engine, boolean startTls, long handshakeTimeoutMillis, AtomicBoolean handshakeFinished)
    {
        this.engine = checkNotNull(engine, "engine");
        this.startTls = startTls;
        this.handshakeTimeoutMillis = handshakeTimeoutMillis;
        this.logger = Logger.getLogger(getClass());
        this.handshakeFinished = handshakeFinished;
    }

    @Override
    public void handlerAdded()
    {
        if (!ignoreHandlerAdded) {
            initDstBuffer();
        }
        else {
            ignoreHandlerAdded = false;
        }
    }

    @Override
    public HandlerStatus onWrite()
            throws Exception
    {
        compactOrClear(dst);

        try {
            // Do not encrypt the first write request if this handler is
            // created with startTLS flag turned on.
            if (startTls && !sentFirstMessage) {
                if (logger.isFineEnabled()) {
                    logger.fine("begin to handshake...");
                }
                sentFirstMessage = true;
                // Explicit start handshake processing once we send the first message. This will also ensure
                // we will schedule the timeout if needed.
                startHandshakeProcessing();
                return HandlerStatus.BLOCKED;
            }

            // If finish handshake, do wrap.
            SSLEngineResult.HandshakeStatus handshakeStatus = engine.getHandshakeStatus();
            if (handshakeStatus == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
                    || handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                // Wrap the buffer.
                // src will be flip() at the end of previous handler, so we do not need flip() again.
                while (src.hasRemaining()) {
                    if (logger.isFineEnabled()) {
                        logger.fine(format("channel=%s, before wrap, src=[%s, %s, %s]", channel, src.position(), src.limit(), src.capacity()));
                    }
                    SSLEngineResult result = engine.wrap(src, dst);
                    if (logger.isFineEnabled()) {
                        logger.fine(format("channel=%s, after wrap, src=[%s, %s, %s], dst=[%s, %s, %s], status=[%s, %s]",
                                channel, src.position(), src.limit(), src.capacity(), dst.position(), dst.limit(), dst.capacity(),
                                result.getStatus(), result.getHandshakeStatus()));
                    }
                    while (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                        dst = enlargeByteBuffer(dst);
                        updateOutboundPipeline();
                        if (logger.isFineEnabled()) {
                            logger.fine(format("BUFFER_OVERFLOW, channel=%s, before wrap, src=[%s, %s, %s], dst=[%s, %s, %s]",
                                    channel, src.position(), src.limit(), src.capacity(), dst.position(), dst.limit(), dst.capacity()));
                        }
                        result = engine.wrap(src, dst);
                        if (logger.isFineEnabled()) {
                            logger.fine(format("BUFFER_OVERFLOW, channel=%s, after wrap, src=[%s, %s, %s], dst=[%s, %s, %s], status=[%s, %s]",
                                    channel, src.position(), src.limit(), src.capacity(), dst.position(), dst.limit(), dst.capacity(),
                                    result.getStatus(), result.getHandshakeStatus()));
                        }
                    }
                    if (result.getStatus() == SSLEngineResult.Status.CLOSED) {
                        if (!engine.isOutboundDone()) {
                            engine.closeOutbound();
                        }
                    }
                }
                if (isTimeout) {
                    if (timeoutFuture != null) {
                        timeoutFuture.cancel(false);
                    }
                    throw new SSLException("SSL handshake time out.");
                }
                // src will be compact() at the begin previous handler, so we do not need compact() again.
                return HandlerStatus.CLEAN;
            }
            else {
                return HandlerStatus.BLOCKED;
            }
        }
        catch (SSLException sslException) {
            engine.closeOutbound();
            throw new SSLException("A problem was encountered while processing data caused the SSLEngine to abort. try to close connection..." + sslException.getMessage());
        }
        finally {
            dst.flip();
        }
    }

    private void startHandshakeProcessing()
    {
        if (!handshakeStarted) {
            handshakeStarted = true;
            handshake();
            timeoutFuture = applyHandshakeTimeout();
        }
    }

    private ScheduledFuture applyHandshakeTimeout()
    {
        // check timeout valid
        if (handshakeTimeoutMillis <= 0 || handshakeFinished.get()) {
            return null;
        }

        ScheduledFuture<?> timeoutFuture = SslContext.scheduleExecutor.schedule(() -> {
            if (!handshakeFinished.get()) {
                isTimeout = true;
            }
        }, handshakeTimeoutMillis, TimeUnit.MILLISECONDS);

        return timeoutFuture;
    }

    private void updateOutboundPipeline()
    {
        ignoreHandlerAdded = true;
        channel.outboundPipeline().replace(this, this);
    }

    private void handshake()
    {
        if (engine.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            return;
        }

        // Begin handshake.
        try {
            engine.beginHandshake();
            ChannelOptions config = channel.options();
            ByteBuffer empty = newByteBuffer(0, config.getOption(DIRECT_BUF));
            engine.wrap(empty, dst);
        }
        catch (Throwable e) {
            engine.closeOutbound();
        }
    }

    private ByteBuffer enlargeByteBuffer(ByteBuffer buffer)
    {
        int appByteBufferSize = engine.getSession().getApplicationBufferSize();
        if (logger.isFineEnabled()) {
            logger.fine(format("begin to enlargeByteBuffer, channel=%s, oldBuffer=[%s, %s, %s]", channel, buffer.position(), buffer.limit(), buffer.capacity()));
        }
        ByteBuffer newBuffer;
        if (appByteBufferSize > buffer.capacity()) {
            newBuffer = ByteBuffer.allocate(appByteBufferSize);
        }
        else {
            newBuffer = ByteBuffer.allocate(buffer.capacity() * 2);
        }
        buffer.flip();
        newBuffer.put(buffer);
        if (logger.isFineEnabled()) {
            logger.fine(format("end to enlargeByteBuffer and rewrite, channel=%s, oldBuffer=[%s, %s, %s], newBuffer=[%s, %s, %s]",
                    channel, buffer.position(), buffer.limit(), buffer.capacity(), newBuffer.position(), newBuffer.limit(), newBuffer.capacity()));
        }
        return newBuffer;
    }
}
