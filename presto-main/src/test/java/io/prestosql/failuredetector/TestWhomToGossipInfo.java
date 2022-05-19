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

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.server.InternalCommunicationConfig;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.discovery.client.ServiceTypes.serviceType;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestWhomToGossipInfo
{
    private void testCreateInfo(JsonCodec<WhomToGossipInfo> codec)
    {
        try {
            WhomToGossipInfo w = new WhomToGossipInfo("");
            String ws = "WhomToGossipInfo{uri=}";
            assertEquals(ws, w.toString());
            assertEquals(w.getUriList().size(), 0);

            WhomToGossipInfo info = new WhomToGossipInfo(new URI("http://10.18.18.225:8080"));
            info.add(new URI("http://10.18.18.125:8080"));

            String json1 = "WhomToGossipInfo{uri=http://10.18.18.225:8080,http://10.18.18.125:8080}";
            assertEquals(json1, info.toString());

            String json = codec.toJson(info);
            System.out.println(json);
            byte[] j = codec.toJsonBytes(info);
            WhomToGossipInfo info2 = codec.fromJson(j);
            assertEquals(info2.toString(), info.toString());

            info.add(new URI("http://10.0.0.1:8080"));

            String infoString = info.toString();
            assertEquals(infoString, "WhomToGossipInfo{uri=http://10.18.18.225:8080,http://10.18.18.125:8080,http://10.0.0.1:8080}");

            List<URI> uris = new ArrayList<>();
            uris.add(new URI("https://10.10.10.10:9090"));
            uris.add(new URI("https://10.10.11.10:9090"));
            uris.add(new URI("https://10.10.12.10:9090"));

            WhomToGossipInfo newInfo = new WhomToGossipInfo(uris);
            assertEquals(newInfo.toString(), "WhomToGossipInfo{uri=https://10.10.10.10:9090,https://10.10.11.10:9090,https://10.10.12.10:9090}");
            json = codec.toJson(newInfo);
            assertEquals(json, "{\"uri\":\"https://10.10.10.10:9090,https://10.10.11.10:9090,https://10.10.12.10:9090\"}");
        }
        catch (URISyntaxException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testInitWorkerInfoJson()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingJmxModule(),
                new TestingDiscoveryModule(),
                new TestingHttpServerModule(),
                new TraceTokenModule(),
                new JsonModule(),
                new JaxrsModule(),
                new CoordinatorGossipFailureDetectorModule(),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        configBinder(binder).bindConfig(InternalCommunicationConfig.class);
                        configBinder(binder).bindConfig(QueryManagerConfig.class);
                        discoveryBinder(binder).bindSelector("presto");
                        discoveryBinder(binder).bindHttpAnnouncement("presto");
                        jsonCodecBinder(binder).bindJsonCodec(WhomToGossipInfo.class);

                        // Jersey with jetty 9 requires at least one resource
                        // todo add a dummy resource to airlift jaxrs in this case
                        jaxrsBinder(binder).bind(TestHeartbeatFailureDetector.FooResource.class);
                    }
                });

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        ServiceSelector selector = injector.getInstance(Key.get(ServiceSelector.class, serviceType("presto")));
        assertEquals(selector.selectAllServices().size(), 1);

        CoordinatorGossipFailureDetector detector = injector.getInstance(CoordinatorGossipFailureDetector.class);
        detector.updateMonitoredServices();

        testCreateInfo(detector.getWhomToGossipInfoCodec());

        assertEquals(detector.getTotalCount(), 0);
        assertEquals(detector.getActiveCount(), 0);
        assertEquals(detector.getFailedCount(), 0);
        assertTrue(detector.getFailed().isEmpty());
    }

    @Test
    public void testEqualURI()
    {
        try {
            URI uri1 = new URI("http://10.1.1.10:8080");
            URI uri2 = new URI("http://10.1.1.10:8080");

            assertTrue(uri1.equals(uri2));
        }
        catch (URISyntaxException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void bitsetTest()
    {
        BitSet b = new BitSet(5);
        b.set(1);
        b.set(3);
        byte[] bytes = b.toByteArray();
        assertEquals(bytes.length, 1);
        assertEquals(bytes[0], 10);

        b.set(0);
        bytes = b.toByteArray();
        assertEquals(bytes.length, 1);
        assertEquals(bytes[0], 11);
    }

    @Test
    public void serializationTest()
    {
        String s = "https://10.10.10.10:9090,https://10.10.11.10:9090,https://10.10.12.10:9090";
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < bytes.length; i++) {
            System.out.print(bytes[i]);
        }
    }
}
