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
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.block.BlockJsonSerde;
import io.prestosql.execution.SplitCacheStateInitializer.InitializationStatus;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.spi.type.Type;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.type.TypeDeserializer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SplitCacheStateManager
{
    private static Logger log = Logger.get(SplitCacheStateManager.class);

    private final StateStoreProvider provider;
    private final Metadata metadata;
    private SplitCacheStateInitializer initializer;
    private SplitCacheStateUpdater updater;
    private SplitCacheMap splitCacheMap;

    @Inject
    public SplitCacheStateManager(StateStoreProvider provider, Metadata metadata)
    {
        this.provider = provider;
        this.metadata = metadata;
        this.splitCacheMap = SplitCacheMap.getInstance();
    }

    public SplitCacheStateManager(StateStoreProvider provider, Metadata metadata, SplitCacheMap splitCacheMap)
    {
        this.provider = provider;
        this.metadata = metadata;
        this.splitCacheMap = splitCacheMap;
    }

    @PostConstruct
    public void startStateServices()
    {
        if (!PropertyService.getBooleanProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED)) {
            log.info("Split cache map feature is disabled.");
            return;
        }

        if (!PropertyService.getBooleanProperty(HetuConstant.MULTI_COORDINATOR_ENABLED)) {
            return;
        }

        BlockEncodingSerde blockEncodingSerde = metadata.getBlockEncodingSerde();
        ObjectMapper mapper = new ObjectMapperProvider().get().registerModule(new SimpleModule()
                .addDeserializer(Type.class, new TypeDeserializer(metadata))
                .addSerializer(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde))
                .addDeserializer(Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde))
                .addKeyDeserializer(SplitKey.class, new SplitKey.KeyDeserializer()));

        AtomicReference<InitializationStatus> status = new AtomicReference<>(InitializationStatus.INITIALIZING);

        //Async: One-shot action. Fetch split cache map info from state store if available
        if (initializer == null) {
            final Duration delay = new Duration(2, TimeUnit.SECONDS);
            final Duration timeout = new Duration(60, TimeUnit.SECONDS);
            initializer = new SplitCacheStateInitializer(provider, splitCacheMap, delay, timeout, mapper, status);
            initializer.start();
        }

        if (updater == null) {
            Duration updateInterval = PropertyService.getDurationProperty(HetuConstant.SPLIT_CACHE_STATE_UPDATE_INTERVAL);
            //Async Task - Periodically update state store with local split cache map changes
            updater = new SplitCacheStateUpdater(provider, splitCacheMap, updateInterval, mapper, status);
            updater.start();
        }
        log.info("-- Initialized split cache map state store and started state services --");
    }

    @PreDestroy
    public void stopStateServices()
    {
        if (initializer != null) {
            initializer.stop();
        }

        if (updater != null) {
            updater.stop();
        }
    }
}
