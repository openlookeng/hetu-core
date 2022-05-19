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

import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.testng.Assert.assertEquals;

public class TestCoordinatorGossipFailureDetector
{
    private static final int START = 0;
    private static final int END = 1;
    private int gossipGroupSize;

    private void setGossipGroupSize(int x)
    {
        this.gossipGroupSize = x;
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
        return startAndEndIdx;
    }

    @Test
    public void testAssignLocalGossipGroups()
    {
        int clusterSize = 10; // 10 workers 0-9
        setGossipGroupSize(5);
        for (int i = 0; i < clusterSize; i++) {
            int[] idx1 = assignLocalGossipGroups(i, clusterSize);
            if (i % 2 == 0) {
                assertEquals(idx1[START], 0);
                assertEquals(idx1[END], 5);
            }
            else {
                assertEquals(idx1[START], 5);
                assertEquals(idx1[END], 10);
            }
        }

        clusterSize = 20; // 10 workers 0-9
        setGossipGroupSize(5);

        int[] idx1 = assignLocalGossipGroups(101, clusterSize);
        assertEquals(idx1[START], 5);
        assertEquals(idx1[END], 10);

        setGossipGroupSize(1000);
        idx1 = assignLocalGossipGroups(101, clusterSize);
        assertEquals(idx1[START], 0);
        assertEquals(idx1[END], 20);
    }

    @Test
    public void testTimeDiff()
    {
        Duration duration = new Duration(11, TimeUnit.SECONDS);
        Duration nanos = duration.convertTo(TimeUnit.NANOSECONDS);
        long now = System.nanoTime();
        try {
            sleep(1);
        }
        catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
        long now1 = System.nanoTime();
        long nowdiff = now1 - now;
        System.out.println(nowdiff);
        if (nowdiff > nanos.getValue()) {
            System.out.println("yes");
        }
        else {
            System.out.println("no");
        }
    }

    public static class TestServiceDescriptor
    {
        private String nodeId;

        private URI uri;

        public TestServiceDescriptor(String nodeId, URI uri)
        {
            this.nodeId = nodeId;
            this.uri = uri;
        }

        public String getNodeId()
        {
            return this.nodeId;
        }

        public URI getUri()
        {
            return this.uri;
        }
    }

    @Test
    public void testSubList()
    {
        Set<TestServiceDescriptor> online = new HashSet<>();
        try {
            online.add(new TestServiceDescriptor("fff1", new URI("http://10.1.1.12:8081")));
            online.add(new TestServiceDescriptor("fff2", new URI("http://10.1.1.12:8082")));
            List<TestServiceDescriptor> onlineServices = new ArrayList<>(online);

            Collections.sort(onlineServices, new Comparator<TestServiceDescriptor>() {
                @Override
                public int compare(TestServiceDescriptor t2, TestServiceDescriptor t1)
                {
                    return t1.getNodeId().compareTo(t1.getNodeId());
                }
            });
            List<URI> uris = new ArrayList<>();
            onlineServices.stream().forEach(o -> uris.add(o.getUri()));

            List<URI> subUris = uris.subList(START, END + 1);
            assertEquals(subUris.size(), 2);
        }
        catch (URISyntaxException e) {
            System.out.println(e.getMessage());
        }
    }
}
