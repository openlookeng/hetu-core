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
import io.airlift.discovery.store.InMemoryStore;

import javax.inject.Inject;

public class HetuInMemoryStore
        extends InMemoryStore
{
    private final Long maxAgeInMs;

    @Inject
    public HetuInMemoryStore(ConflictResolver resolver, DiscoveryConfig discoveryConfig)
    {
        super(resolver);
        maxAgeInMs = discoveryConfig.getMaxAge().toMillis();
    }

    @Override
    public void put(Entry entry)
    {
        Long maxAgeInMs = entry.getMaxAgeInMs();
        if (entry.getMaxAgeInMs() == null) {
            // when put entity, set the default max time.
            // the entity from remote, the max age time is null, so the entity can't be expired when the node of entity is disconnected.
            // then the max age time is reached, the entity will be expired.
            // default max age time is 30s.
            maxAgeInMs = this.maxAgeInMs;
        }

        Entry newEntry = new Entry(entry.getKey(), entry.getValue(), entry.getVersion(), entry.getTimestamp(), maxAgeInMs);
        super.put(newEntry);
    }
}
