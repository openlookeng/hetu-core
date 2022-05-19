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

import com.google.common.io.ByteStreams;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.prestosql.server.GossipStatusResource;
import io.prestosql.server.InternalCommunicationConfig;
import io.prestosql.spi.HostAddress;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.prestosql.failuredetector.FailureDetector.State.ALIVE;
import static io.prestosql.protocol.RequestHelpers.setContentTypeHeaders;
import static io.prestosql.spi.HostAddress.fromUri;

public class CoordinatorGossipFailureDetector
        extends HeartbeatFailureDetector
        implements FailureDetector
{
    private static final Logger log = Logger.get(CoordinatorGossipFailureDetector.class);
    private static int uniq; // initialization to zero by default for int
    private final Map<String, Long> gossipTales = new HashMap<>();
    private final Set<GossipMonitoringTask> tasks = new HashSet<>();
    private final Duration gossipValidityPeriod;
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("gossip-failure-detector"));
    private final JsonCodec<WhomToGossipInfo> whomToGossipInfoCodec;
    private final int gossipGroupSize;
    private static final int START = 0;
    private static final int END = 1;

    @Inject
    public CoordinatorGossipFailureDetector(@ServiceType("presto") ServiceSelector selector,
                                            @ForFailureDetector HttpClient httpClient,
                                            FailureDetectorConfig failureDetectorConfig,
                                            NodeInfo nodeInfo,
                                            InternalCommunicationConfig internalCommunicationConfig,
                                            GossipProtocolConfig config,
                                            JsonCodec<WhomToGossipInfo> whomToGossipInfoCodec)
    {
        this(selector, httpClient, failureDetectorConfig, nodeInfo,
                internalCommunicationConfig, config.getCnWkUpdateGossipMonitorServiceInterval(),
                config.getCnGossipCollateInterval(), config.getGossipGroupSize(), whomToGossipInfoCodec);
    }

    private CoordinatorGossipFailureDetector(@ServiceType("presto") ServiceSelector selector,
                                             @ForFailureDetector HttpClient httpClient,
                                             FailureDetectorConfig failureDetectorConfig,
                                             NodeInfo nodeInfo,
                                             InternalCommunicationConfig internalCommunicationConfig,
                                             Duration monitoringServiceUpdateInterval,
                                             Duration cnGossipCollateInterval,
                                             int gossipGroupSize,
                                             JsonCodec<WhomToGossipInfo> whomToGossipInfoCodec)
    {
        super(selector, httpClient, failureDetectorConfig, nodeInfo, internalCommunicationConfig);
        this.monitoringServiceUpdateInterval = monitoringServiceUpdateInterval;
        this.gossipValidityPeriod = cnGossipCollateInterval;
        this.whomToGossipInfoCodec = whomToGossipInfoCodec;
        this.gossipGroupSize = gossipGroupSize;
    }

    public JsonCodec<WhomToGossipInfo> getWhomToGossipInfoCodec()
    {
        return whomToGossipInfoCodec;
    }

    @Override
    protected MonitoringTask createNewTask(ServiceDescriptor service, URI uri)
    {
        log.debug("creating new gossip monitoring task for uri " + uri);
        GossipMonitoringTask task = new GossipMonitoringTask(service, uri, uniq++);
        tasks.add(task);
        return task;
    }

    public List<URI> getSortedOnlineServiceDescriptors(Set<ServiceDescriptor> online)
    {
        List<URI> uris = new ArrayList<>();
        List<ServiceDescriptor> onlineServices = new ArrayList<>(online);
        Collections.sort(onlineServices, new Comparator<ServiceDescriptor>() {
            @Override
            public int compare(ServiceDescriptor t2, ServiceDescriptor t1)
            {
                return t1.getNodeId().compareTo(t1.getNodeId());
            }
        });
        onlineServices.stream().forEach(o -> uris.add(getHttpUri(o)));
        return uris;
    }

    private int[] assignLocalGossipGroups(int idx, int clusterSize)
    {
        int[] startAndEndIdx = new int[2];
        if (this.gossipGroupSize >= clusterSize) {
            startAndEndIdx[END] = clusterSize; // START is already 0 by initialization
        }
        else {
            int start = idx * this.gossipGroupSize % clusterSize;
            int end = (start + this.gossipGroupSize - 1) % clusterSize;
            if (start < end) {
                startAndEndIdx[START] = start;
                startAndEndIdx[END] = end + 1;
            }
            else {
                startAndEndIdx[START] = end;
                startAndEndIdx[END] = start + 1;
            }
        }
        log.debug("In " + idx + " rotation, start idx "
                + startAndEndIdx[START] + ", end idx "
                + startAndEndIdx[END] + " for total "
                + clusterSize + " workers");
        return startAndEndIdx;
    }

    private static BitSet readResponseBitSet(Response response)
    {
        try {
            byte[] bytes = ByteStreams.toByteArray(response.getInputStream());
            return BitSet.valueOf(new byte[] {bytes[0]});
        }
        catch (IOException e) {
            log.error("error reading response input stream");
        }
        return new BitSet();
    }

    @PostConstruct
    @Override
    public void start()
    {
        initGossipTales();
        super.start();

        executor.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    forgetGossip();
                }
                catch (Throwable e) {
                    log.warn(e, "Error removing stale gossip entries");
                }
            }
        }, 0, ((Double) gossipValidityPeriod.getValue()).longValue(), gossipValidityPeriod.getUnit());
    }

    private void initGossipTales()
    {
        Set<ServiceDescriptor> online = getOnlineServiceDescriptors();
        online.forEach(o -> gossipTales.put(o.getNodeId(), 0L));
    }

    @Override
    public State getState(HostAddress hostAddress)
    {
        State state = super.getState(hostAddress);
        MonitoringTask task = tasks.stream()
                .filter(t -> hostAddress.equals(fromUri(t.getUri()))).findFirst().orElse(null);
        if (state != ALIVE && task != null
                && gossipTales.containsKey(task.getService().getNodeId())) {
            log.debug("node cannot be connected, but gossip is, it is alive!");
            state = ALIVE;
        }
        return state;
    }

    /**
     * The following method removes stale gossip info.
     * Gossip validity period (default is 2 seconds).
     * Any entry older than 2 seconds are removed.
     * If no gossip has been received for a node for more than 2 seconds, should be considered GONE/UNRESPONSIVE
     */
    private synchronized void forgetGossip()
    {
        Set<Map.Entry<String, Long>> staleGossip = gossipTales.entrySet().stream()
                .filter(e -> (System.nanoTime() - e.getValue() > gossipValidityPeriod.convertTo(TimeUnit.NANOSECONDS).getValue()))
                .collect(Collectors.toSet());
        log.debug("Stale gossip count " + staleGossip.size());
        staleGossip.forEach(s -> gossipTales.remove(s.getKey()));
    }

    private StaticBodyGenerator createBodyGenerator(WhomToGossipInfo info)
    {
        return jsonBodyGenerator(whomToGossipInfoCodec, info);
    }

    @ThreadSafe
    private class GossipMonitoringTask
            extends HeartbeatFailureDetector.MonitoringTask
    {
        private BitSet gossip;
        private WhomToGossipInfo whomToGossipInfo;
        private final int idx;

        public GossipMonitoringTask(ServiceDescriptor service, URI uri, int idx)
        {
            super(service, uri);
            this.idx = idx;
        }

        private Set<String> getAliveNodes()
        {
            Set<String> alive = new HashSet<>();
            for (int i = 0; i < whomToGossipInfo.getUriList().size(); i++) {
                if (gossip.get(i)) {
                    URI uri = whomToGossipInfo.getUriList().get(i);
                    alive.add(tasks.stream().filter(t -> t.getUri().equals(uri)).findFirst().get().getService().getNodeId());
                }
            }
            log.debug(alive.size() + "other workers alive");
            return alive;
        }

        /**
         * this method updates the gossip table.
         * Each entry is identified by the node id, and its value is a timestamp, when CN got to know it alive.
         * Ideally, each node should send their local timestamp. Due to payload concerns, it was not approached.
         */
        private synchronized void doGossip()
        {
            Set<String> alive = getAliveNodes();
            long currentTime = System.nanoTime(); // this is CN system time. So, later enties always have larger timestamp
            alive.forEach(a -> gossipTales.put(a, currentTime));
            gossipTales.put(getService().getNodeId(), currentTime); // put itself
        }

        private void postInitGossipUriList()
                throws Exception
        {
            Set<ServiceDescriptor> online = getOnlineServiceDescriptors();
            int[] startAndEnd = assignLocalGossipGroups(this.idx, online.size());
            List<URI> uris = getSortedOnlineServiceDescriptors(online).subList(startAndEnd[START], startAndEnd[END]);

            this.whomToGossipInfo = new WhomToGossipInfo();
            for (URI uri : uris) {
                if (!uri.equals(this.getUri())) {
                    this.whomToGossipInfo.add(uri);
                    log.debug("add " + uri + " to gossip for worker " + getUri());
                }
            }

            log.debug("CN ping to Wk " + getUri() + "to start gossip ");
            URI reqUri = uriBuilderFrom(getUri()).appendPath(GossipStatusResource.GOSSIP_STATUS).build();
            Request request = setContentTypeHeaders(false, preparePost()).setUri(reqUri)
                    .setBodyGenerator(createBodyGenerator(this.whomToGossipInfo)).build();
            getHttpClient().execute(request, new ResponseHandler<Object, Exception>() {
                @Override
                public Object handleException(Request request, Exception exception)
                {
                    log.debug("initial ping exception");
                    return null;
                }

                @Override
                public Object handle(Request request, Response response)
                {
                    return null;
                }
            });
        }

        @Override
        public synchronized void enable()
        {
            try {
                postInitGossipUriList();
            }
            catch (Exception e) {
                log.error("error while init gossip");
            }
            super.enable();
        }

        @Override
        protected void ping()
        {
            try {
                getStats().recordStart();
                log.debug("ping ... " + getUri());
                URI wUri = uriBuilderFrom(getUri()).appendPath(GossipStatusResource.GOSSIP_STATUS).build();
                Request request = prepareGet().setUri(wUri).build();

                getHttpClient().executeAsync(request,
                        new ResponseHandler<Object, Exception>()
                        {
                            @Override
                            public Exception handleException(Request request, Exception exception)
                            {
                                getStats().recordFailure(exception);
                                log.warn("gossip ping got exception");
                                return null;
                            }

                            @Override
                            public Object handle(Request request, Response response)
                            {
                                getStats().recordSuccess();
                                gossip = readResponseBitSet(response);
                                doGossip();
                                return null;
                            }
                        });
            }
            catch (Exception e) {
                log.warn(e, "Error scheduling request for %s", getUri());
            }
        }
    }
}
