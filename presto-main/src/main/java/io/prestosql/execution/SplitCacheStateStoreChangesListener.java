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
package io.prestosql.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.prestosql.spi.statestore.listener.EntryAddedListener;
import io.prestosql.spi.statestore.listener.EntryEvent;
import io.prestosql.spi.statestore.listener.EntryRemovedListener;
import io.prestosql.spi.statestore.listener.EntryUpdatedListener;

import java.io.IOException;

/**
 * SplitCacheStateStoreChangesListener listens for
 * StateStore changes events and updates the changes local SplitCacheMap.
 *
 * @see EntryAddedListener
 * @see EntryRemovedListener
 * @see EntryUpdatedListener
 */
public class SplitCacheStateStoreChangesListener
        implements EntryAddedListener<String, String>, EntryRemovedListener<String, String>, EntryUpdatedListener<String, String>

{
    private final Logger log = Logger.get(SplitCacheStateStoreChangesListener.class);

    private final SplitCacheMap splitCacheMap;
    private final ObjectMapper mapper;

    public SplitCacheStateStoreChangesListener(SplitCacheMap splitCacheMap, ObjectMapper mapper)
    {
        this.splitCacheMap = splitCacheMap;
        this.mapper = mapper;
    }

    @Override
    public void entryAdded(EntryEvent<String, String> event)
    {
        log.debug("Entry added event received for table %s", event.getKey());
        addOrUpdateEntry(event);
    }

    @Override
    public void entryUpdated(EntryEvent<String, String> event)
    {
        log.debug("Entry updated event received for table %s", event.getKey());
        addOrUpdateEntry(event);
    }

    private void addOrUpdateEntry(EntryEvent<String, String> event)
    {
        try {
            TableCacheInfo tableCacheInfo = mapper.readerFor(TableCacheInfo.class).readValue(event.getValue());
            splitCacheMap.setTableCacheInfo(event.getKey(), tableCacheInfo);
        }
        catch (IOException e) {
            log.error(e, "Unable to update local split cache map. Event = %s", event);
        }
    }

    @Override
    public void entryRemoved(EntryEvent<String, String> event)
    {
        log.debug("Entry removed event received for table %s", event.getKey());
        splitCacheMap.removeTableCacheInfo(event.getKey());
    }
}
