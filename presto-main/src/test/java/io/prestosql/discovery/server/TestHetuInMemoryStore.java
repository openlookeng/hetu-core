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
package io.prestosql.discovery.server;

import io.airlift.discovery.server.DiscoveryConfig;
import io.airlift.discovery.store.ConflictResolver;
import io.airlift.discovery.store.Entry;
import io.airlift.discovery.store.Version;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHetuInMemoryStore
{
    @Test
    public void testPutNullMaxAgeEntity()
    {
        DiscoveryConfig discoveryConfig = new DiscoveryConfig()
                .setMaxAge(Duration.valueOf("10s"))
                .setStoreCacheTtl(Duration.valueOf("5s"));

        HetuInMemoryStore inMemoryStore = new HetuInMemoryStore(new ConflictResolver(), discoveryConfig);
        Entry entry = new Entry("key".getBytes(), "value".getBytes(), new Version(1L), 0L, null);
        inMemoryStore.put(entry);
        Entry check = inMemoryStore.get("key".getBytes());
        assertFalse(check.equals(entry));
        assertEquals((long) check.getMaxAgeInMs(), 10000L);
    }

    @Test
    public void testPutValidMaxAgeEntity()
    {
        DiscoveryConfig discoveryConfig = new DiscoveryConfig()
                .setMaxAge(Duration.valueOf("10s"))
                .setStoreCacheTtl(Duration.valueOf("5s"));

        HetuInMemoryStore inMemoryStore = new HetuInMemoryStore(new ConflictResolver(), discoveryConfig);
        Entry entry = new Entry("key".getBytes(), "value".getBytes(), new Version(1L), 0L, 5000L);
        inMemoryStore.put(entry);
        Entry check = inMemoryStore.get("key".getBytes());
        assertTrue(check.equals(entry));
        assertEquals((long) check.getMaxAgeInMs(), 5000L);
    }
}
