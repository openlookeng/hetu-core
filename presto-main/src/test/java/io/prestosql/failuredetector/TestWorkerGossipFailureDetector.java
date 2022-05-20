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
package io.prestosql.failuredetector;

import io.airlift.discovery.client.ServiceDescriptor;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.testng.Assert.assertTrue;

public class TestWorkerGossipFailureDetector
{
    @Test
    public void testForEmptyWaitingTasks()
    {
        boolean pass = false;
        try {
            testEmptyServiceStateRefresh();
        }
        catch (NoSuchElementException e) {
            pass = true;
        }
        finally {
            assertTrue(pass);
        }
    }

    private void testEmptyServiceStateRefresh()
    {
        Map<ServiceDescriptor, Long> waitingTasks = new HashMap<>();
        long currentTime = waitingTasks.values().stream().mapToLong(t -> t).max().getAsLong();
    }
}
