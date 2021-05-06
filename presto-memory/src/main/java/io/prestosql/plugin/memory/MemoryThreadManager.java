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

package io.prestosql.plugin.memory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public class MemoryThreadManager
{
    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("MemoryConnector-pool-%d").setDaemon(true).build();
    private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(Math.max((Runtime.getRuntime().availableProcessors() / 2), 2), threadFactory);

    private MemoryThreadManager()
    {
    }

    public static ScheduledExecutorService getSharedThreadPool()
    {
        return executor;
    }

    public static ScheduledExecutorService getNewSingleThreadExecutor()
    {
        return Executors.newSingleThreadScheduledExecutor();
    }
}
