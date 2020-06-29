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
package io.prestosql.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.discovery.client.ServiceInventoryConfig;
import io.airlift.discovery.client.ServiceState;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.prestosql.server.InternalCommunicationConfig;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.statestore.StateStoreConstants;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.utils.HetuConfig;

import javax.inject.Inject;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class HetuServiceInventory
        extends ServiceInventory
{
    private Logger log = Logger.get(HetuServiceInventory.class);
    private final HetuConfig hetuConfig;
    private final String nodeId;
    private final StateStoreProvider stateStoreProvider;
    private final InternalCommunicationConfig internalCommunicationConfig;
    private AtomicBoolean serverUp = new AtomicBoolean(true);

    @Inject
    public HetuServiceInventory(HetuConfig hetuConfig,
                                StateStoreProvider stateStoreProvider,
                                InternalCommunicationConfig internalCommunicationConfig,
                                ServiceInventoryConfig config,
                                NodeInfo nodeInfo,
                                JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec,
                                @ForDiscoveryClient HttpClient httpClient)
    {
        super(config, nodeInfo, serviceDescriptorsCodec, httpClient);
        this.nodeId = nodeInfo.getNodeId();
        this.hetuConfig = hetuConfig;
        this.stateStoreProvider = stateStoreProvider;
        this.internalCommunicationConfig = internalCommunicationConfig;
    }

    @Override
    public Iterable<ServiceDescriptor> getServiceDescriptors(String type)
    {
        if ("discovery".equals(type) && hetuConfig.isMultipleCoordinatorEnabled()) {
            try {
                StateStore stateStore = stateStoreProvider.getStateStore();

                if (stateStore == null) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "State store has not been loaded yet");
                }

                Map.Entry<String, String> entry = ((StateMap<String, String>) stateStore.getStateCollection(StateStoreConstants.DISCOVERY_SERVICE_COLLECTION_NAME))
                        .getAll().entrySet().stream().findFirst().get();

                ImmutableMap.Builder properties = new ImmutableMap.Builder();
                if (internalCommunicationConfig != null && internalCommunicationConfig.isHttpsRequired()) {
                    properties.put("https", "https://" + entry.getKey() + ":" + entry.getValue());
                }
                else {
                    properties.put("http", "http://" + entry.getKey() + ":" + entry.getValue());
                }
                return ImmutableList.of(new ServiceDescriptor(UUID.randomUUID(), nodeId, "discovery", null, null, ServiceState.RUNNING, properties.build()));
            }
            catch (Exception e) {
                logServerError("Select service from state store failed:" + e);
            }
        }
        return super.getServiceDescriptors(type);
    }

    private void logServerError(String message, Object... args)
    {
        if (serverUp.compareAndSet(true, false)) {
            log.error(message, args);
        }
    }
}
