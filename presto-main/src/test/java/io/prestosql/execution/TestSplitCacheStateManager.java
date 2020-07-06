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

import io.airlift.units.Duration;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.statestore.StateStoreConstants;
import io.prestosql.statestore.StateStoreProvider;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestSplitCacheStateManager
{
    private StateStore stateStore;
    private Metadata metadata = MetadataManager.createTestMetadataManager();

    @BeforeSuite
    public void setup()
    {
        stateStore = mock(StateStore.class);
        when(stateStore.getName()).thenReturn("mock");

        Map<String, String> testStateMap = new ConcurrentHashMap<>();
        when(stateStore.getStateCollection(StateStoreConstants.SPLIT_CACHE_METADATA_NAME))
                .thenReturn(new TestSplitCacheStateUpdater.MockStateMap<>(StateStoreConstants.SPLIT_CACHE_METADATA_NAME, testStateMap));
    }

    private SplitCacheMap createNew(boolean enabled)
    {
        //way to hack around singleton object - Only intended for tests
        try {
            return SplitCacheMapSingletonFactory.createInstance(enabled);
        }
        catch (Exception e) {
            throw new IllegalStateException("Singleton creation failed!");
        }
    }

    @Test
    public void testStartAndStopServicesWhenSplitCacheDisabled()
    {
        StateStoreProvider provider = mock(StateStoreProvider.class);
        when(provider.getStateStore()).thenReturn(null);

        PropertyService.setProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED, false);
        PropertyService.setProperty(HetuConstant.MULTI_COORDINATOR_ENABLED, true);

        SplitCacheMap splitCacheMap = createNew(false);
        SplitCacheStateManager manager = new SplitCacheStateManager(provider, metadata, splitCacheMap);
        manager.startStateServices();
        manager.stopStateServices();

        assertNull(SplitCacheMap.getInstance());
    }

    @Test
    public void testStartAndStopServicesWhenStateStoreDisabled()
    {
        StateStoreProvider provider = mock(StateStoreProvider.class);
        when(provider.getStateStore()).thenReturn(null);

        PropertyService.setProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED, true);
        PropertyService.setProperty(HetuConstant.MULTI_COORDINATOR_ENABLED, false);

        SplitCacheMap splitCacheMap = createNew(true);
        SplitCacheStateManager manager = new SplitCacheStateManager(provider, metadata, splitCacheMap);
        manager.startStateServices();
        manager.stopStateServices();

        assertNotNull(SplitCacheMap.getInstance());
    }

    @Test
    public void testStartAndStopServicesWhenStateStoreEnabled()
    {
        StateStoreProvider provider = mock(StateStoreProvider.class);
        when(provider.getStateStore()).thenReturn(stateStore);

        PropertyService.setProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED, true);
        PropertyService.setProperty(HetuConstant.MULTI_COORDINATOR_ENABLED, true);
        PropertyService.setProperty(HetuConstant.SPLIT_CACHE_STATE_UPDATE_INTERVAL, new Duration(2, TimeUnit.SECONDS));

        SplitCacheMap splitCacheMap = createNew(true);
        SplitCacheStateManager manager = new SplitCacheStateManager(provider, metadata, splitCacheMap);
        manager.startStateServices();
        manager.stopStateServices();

        assertNotNull(SplitCacheMap.getInstance());
    }
}
