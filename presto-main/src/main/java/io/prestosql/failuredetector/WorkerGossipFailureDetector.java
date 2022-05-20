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
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.prestosql.server.InternalCommunicationConfig;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URI;
import java.util.BitSet;
import java.util.Set;
import java.util.stream.Collectors;

public class WorkerGossipFailureDetector
        extends HeartbeatFailureDetector
        implements FailureDetector
{
    private static final Logger log = Logger.get(WorkerGossipFailureDetector.class);
    private WhomToGossipInfo whomToGossipInfo;
    private final JsonCodec<WhomToGossipInfo> whomToGossipInfoCodec;

    @Inject
    public WorkerGossipFailureDetector(@ServiceType("presto") ServiceSelector selector,
                                       @ForFailureDetector HttpClient httpClient,
                                       FailureDetectorConfig failureDetectorConfig,
                                       NodeInfo nodeInfo,
                                       InternalCommunicationConfig internalCommunicationConfig,
                                       GossipProtocolConfig config,
                                       JsonCodec<WhomToGossipInfo> whomToGossipInfoCodec)
    {
        this(selector, httpClient, failureDetectorConfig, nodeInfo, internalCommunicationConfig, config.getWkWkUpdateGossipMonitorServiceInterval(), whomToGossipInfoCodec);
    }

    private WorkerGossipFailureDetector(@ServiceType("presto") ServiceSelector selector,
                                             @ForFailureDetector HttpClient httpClient,
                                             FailureDetectorConfig failureDetectorConfig,
                                             NodeInfo nodeInfo,
                                             InternalCommunicationConfig internalCommunicationConfig,
                                             Duration wkWkPingInterval,
                                        JsonCodec<WhomToGossipInfo> whomToGossipInfoCodec)
    {
        super(selector, httpClient, failureDetectorConfig, nodeInfo, internalCommunicationConfig);
        this.monitoringServiceUpdateInterval = wkWkPingInterval;
        this.whomToGossipInfoCodec = whomToGossipInfoCodec;
    }

    @Override
    protected MonitoringTask createNewTask(ServiceDescriptor service, URI uri)
    {
        log.debug("creating new gossip monitoring task for uri " + uri);
        int id = 0;
        for (URI u : this.whomToGossipInfo.getUriList()) {
            if (u.equals(uri)) {
                break;
            }
            id++;
        }
        GossipMonitoringTask task = new GossipMonitoringTask(service, uri, id);
        return task;
    }

    @PostConstruct
    @Override
    public void start()
    {
        log.debug("Post construct start: do nothing.");
    }

    public void initWhomToGossipList(byte[] whomToGossipInfo)
    {
        this.whomToGossipInfo = this.whomToGossipInfoCodec.fromJson(whomToGossipInfo);
        log.debug("created object " + this.whomToGossipInfo.toString());
        super.start();
    }

    @Override
    protected Set<ServiceDescriptor> getOnlineServiceDescriptors()
    {
        Set<ServiceDescriptor> online = super.getOnlineServiceDescriptors().stream()
                .filter(s -> this.whomToGossipInfo.getUriList()
                        .contains(getHttpUri(s))).collect(Collectors.toSet());
        return online;
    }

    @Override
    protected long getCurrentTime()
    {
        return System.nanoTime();
    }

    public BitSet getAliveNodesBitmap()
    {
        if (this.whomToGossipInfo != null) {
            BitSet bitset = new BitSet(this.whomToGossipInfo.getUriList().size());
            getAliveNodes().stream().forEach(t -> bitset.set(((GossipMonitoringTask) t).getId()));
            log.debug("sending bitset: " + bitset);
            return bitset;
        }
        return new BitSet(); // no info
    }

    @ThreadSafe
    private class GossipMonitoringTask
            extends HeartbeatFailureDetector.MonitoringTask
    {
        private int id;

        public GossipMonitoringTask(ServiceDescriptor service, URI uri, int id)
        {
            super(service, uri);
            this.id = id;
        }

        public int getId()
        {
            return this.id;
        }
    }
}
