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

package io.prestosql.plugin.memory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public class MemoryThreadManager
{
    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("MemoryConnector-pool-%d").setDaemon(true).build();
    private static ScheduledExecutorService executor;

    private MemoryThreadManager()
    {
    }

    /**
     * Should only be called once in MemoryConnectorFactory when connector is starting.
     * Callers should then use getSharedThreadPool()
     * @param size
     * @return
     */
    public static ScheduledExecutorService initSharedThreadPool(int size)
    {
        if (executor == null) {
            executor = Executors.newScheduledThreadPool(Math.max(size, 1), threadFactory);
        }
        else {
            throw new RuntimeException("Thread pool was already initialized");
        }

        return executor;
    }

    public static boolean isSharedThreadPoolInitilized()
    {
        return executor != null;
    }

    public static ScheduledExecutorService getSharedThreadPool()
    {
        if (executor == null) {
            throw new RuntimeException("Thread pool not initialized");
        }

        return executor;
    }

    public static ScheduledExecutorService getNewSingleThreadExecutor()
    {
        return Executors.newSingleThreadScheduledExecutor();
    }
}
