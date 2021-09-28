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

package io.prestosql.discovery;

import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.discovery.client.ServiceInventoryConfig;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.prestosql.operator.MockExchangeRequestProcessor;
import io.prestosql.server.InternalCommunicationConfig;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.statestore.LocalStateStoreProvider;
import io.prestosql.statestore.StateStoreConstants;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.utils.HetuConfig;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.units.DataSize.Unit.BYTE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestHetuServiceInventory
{
    private static final String TESTING_HOST = "127.0.0.1";
    private static final String TESTING_PORT = "8090";

    @Test
    public void testGetDiscoveryUriSupplierHttp()
    {
        InternalCommunicationConfig internalCommunicationConfig = new InternalCommunicationConfig()
                .setHttpsRequired(false);

        HetuConfig hetuConfig = new HetuConfig();
        hetuConfig.setMultipleCoordinatorEnabled(true);

        HetuServiceInventory inventory = new HetuServiceInventory(hetuConfig,
                null,
                createMockStateStoreProvider(),
                internalCommunicationConfig,
                new ServiceInventoryConfig(),
                new NodeInfo(new NodeConfig().setEnvironment("hetutest")),
                JsonCodec.jsonCodec(ServiceDescriptorsRepresentation.class),
                new TestingHttpClient(new MockExchangeRequestProcessor(new DataSize(0, BYTE))));
        Iterable<ServiceDescriptor> serviceDescriptors = inventory.getServiceDescriptors("discovery");
        List<URI> uris = new ArrayList<>();
        serviceDescriptors.forEach(serviceDescriptor -> {
            uris.add(URI.create(serviceDescriptor.getProperties().get("http")));
        });

        URI checkUri = URI.create("http://127.0.0.1:8090");
        assertEquals(uris.size(), 1);
        uris.contains(checkUri);
    }

    @Test
    public void testGetDiscoveryUriSupplierHttps()
    {
        InternalCommunicationConfig internalCommunicationConfig = new InternalCommunicationConfig()
                .setHttpsRequired(true);

        HetuConfig hetuConfig = new HetuConfig();
        hetuConfig.setMultipleCoordinatorEnabled(true);

        HetuServiceInventory inventory = new HetuServiceInventory(hetuConfig,
                null,
                createMockStateStoreProvider(),
                internalCommunicationConfig,
                new ServiceInventoryConfig(),
                new NodeInfo(new NodeConfig().setEnvironment("hetutest")),
                JsonCodec.jsonCodec(ServiceDescriptorsRepresentation.class),
                new TestingHttpClient(new MockExchangeRequestProcessor(new DataSize(0, BYTE))));
        Iterable<ServiceDescriptor> serviceDescriptors = inventory.getServiceDescriptors("discovery");
        List<URI> uris = new ArrayList<>();
        serviceDescriptors.forEach(serviceDescriptor -> {
            uris.add(URI.create(serviceDescriptor.getProperties().get("https")));
        });

        URI checkUri = URI.create("https://127.0.0.1:8090");
        assertEquals(uris.size(), 1);
        uris.contains(checkUri);
    }

    private StateStoreProvider createMockStateStoreProvider()
    {
        StateStoreProvider mockStateStoreProvider = mock(LocalStateStoreProvider.class);
        StateStore mockStateStore = mock(StateStore.class);
        when(mockStateStoreProvider.getStateStore()).thenReturn(mockStateStore);
        StateMap<String, String> mockDiscoveryInfo = mock(StateMap.class);
        Map<String, String> mockDiscoveryMap = new HashMap<>();
        mockDiscoveryMap.put(TESTING_HOST, TESTING_PORT);
        when(mockDiscoveryInfo.getAll()).thenReturn(mockDiscoveryMap);
        when(mockStateStore.getStateCollection(StateStoreConstants.DISCOVERY_SERVICE_COLLECTION_NAME)).thenReturn(mockDiscoveryInfo);

        return mockStateStoreProvider;
    }
}
