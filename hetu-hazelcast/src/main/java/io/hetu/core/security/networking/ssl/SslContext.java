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

import javax.net.ssl.SSLEngine;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class SslContext
{
    public static ScheduledExecutorService scheduleExecutor = Executors.newScheduledThreadPool(20);
    public static ExecutorService executors = new ThreadPoolExecutor(20, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
    private SSLEngine engine;
    private boolean startTls;
    private long sessionTimeout;
    private AtomicBoolean handshakeFinished = new AtomicBoolean(false);

    public SslContext(SSLEngine engine, boolean startTls, long sessionTimeout)
    {
        this.engine = checkNotNull(engine, "engine");
        this.startTls = startTls;
        this.sessionTimeout = sessionTimeout;
    }

    public SslInboundHandler newSslInboundHandler()
    {
        return new SslInboundHandler(engine, handshakeFinished);
    }

    public SslOutboundHandler newSslOutboundHandler()
    {
        return new SslOutboundHandler(engine, startTls, sessionTimeout, handshakeFinished);
    }
}
