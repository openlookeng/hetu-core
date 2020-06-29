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
package io.prestosql.execution.resourcegroups;

import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.MockManagedQueryExecution;
import io.prestosql.server.QueryStateInfo;
import io.prestosql.server.ResourceGroupInfo;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.statestore.SharedQueryState;
import io.prestosql.statestore.SharedResourceGroupState;
import io.prestosql.statestore.StateCacheStore;
import io.prestosql.statestore.StateStoreConstants;
import io.prestosql.utils.DistributedResourceGroupUtils;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.Returns;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.execution.QueryState.QUEUED;
import static io.prestosql.execution.QueryState.RUNNING;
import static io.prestosql.spi.resourcegroups.ResourceGroupState.CAN_QUEUE;
import static io.prestosql.spi.resourcegroups.SchedulingPolicy.FAIR;
import static io.prestosql.spi.resourcegroups.SchedulingPolicy.QUERY_PRIORITY;
import static io.prestosql.spi.resourcegroups.SchedulingPolicy.WEIGHTED;
import static io.prestosql.spi.resourcegroups.SchedulingPolicy.WEIGHTED_FAIR;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test DistributedResourceGroup
 *
 * @since 2019-11-29
 */
public class TestDistributedResourceGroup
{
    // To ensure that test cases are run sequentially
    private final Object lock = new Object();
    private StateStore statestore;
    private static final DataSize ONE_BYTE = new DataSize(1, BYTE);
    private static final DataSize FIVE_BYTE = new DataSize(5, BYTE);
    private static final DataSize TEN_BYTE = new DataSize(10, BYTE);
    private static final DataSize ONE_MEGABYTE = new DataSize(1, MEGABYTE);
    private static final DataSize ONE_GIGABYTE = new DataSize(1, GIGABYTE);

    @BeforeClass
    public void setup()
    {
        statestore = Mockito.mock(StateStore.class);
        when(statestore.getLock(anyString())).then(new Returns(new ReentrantLock()));
    }

    @Test
    public void testQueueFull()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, ONE_MEGABYTE, 1, 1);
            // Remote queries query1, query2
            MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
            query1.setResourceGroupId(root.getId());
            MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
            query2.setResourceGroupId(root.getId());
            query1.startWaitingForResources(); // query1 running in remote
            updateQueryStateCache(query1);
            updateQueryStateCache(query2);
            assertEquals(query1.getState(), RUNNING);
            assertEquals(query2.getState(), QUEUED);
            // local query query3
            MockManagedQueryExecution query3 = new MockManagedQueryExecution(0);
            root.run(query3);
            assertEquals(query3.getState(), FAILED);
            assertEquals(query3.getThrowable().getMessage(), "Too many queued queries for \"root\"");
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test
    public void testLeastRecentlyExecutedSubgroup()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, ONE_MEGABYTE, 1, 4);
            DistributedResourceGroup group1 = root.getOrCreateSubGroup("1");
            resourceGroupBasicSetUp(group1, ONE_MEGABYTE, 1, 4);
            DistributedResourceGroup group2 = root.getOrCreateSubGroup("2");
            resourceGroupBasicSetUp(group2, ONE_MEGABYTE, 1, 4);
            DistributedResourceGroup group3 = root.getOrCreateSubGroup("3");
            resourceGroupBasicSetUp(group3, ONE_MEGABYTE, 1, 4);
            // remote query query1a
            MockManagedQueryExecution query1a = new MockManagedQueryExecution(0);
            query1a.startWaitingForResources();
            assertEquals(query1a.getState(), RUNNING);
            query1a.setResourceGroupId(group1.getId());
            updateQueryStateCache(query1a);
            // local queries
            MockManagedQueryExecution query1b = new MockManagedQueryExecution(0);
            group1.run(query1b);
            assertEquals(query1b.getState(), QUEUED);
            query1b.setResourceGroupId(group1.getId());
            updateQueryStateCache(query1b);
            MockManagedQueryExecution query2a = new MockManagedQueryExecution(0);
            group2.run(query2a);
            assertEquals(query2a.getState(), QUEUED);
            query2a.setResourceGroupId(group2.getId());
            updateQueryStateCache(query2a);
            MockManagedQueryExecution query2b = new MockManagedQueryExecution(0);
            group2.run(query2b);
            assertEquals(query2b.getState(), QUEUED);
            query2b.setResourceGroupId(group2.getId());
            updateQueryStateCache(query2b);
            MockManagedQueryExecution query3a = new MockManagedQueryExecution(0);
            group3.run(query3a);
            assertEquals(query3a.getState(), QUEUED);
            query3a.setResourceGroupId(group3.getId());
            updateQueryStateCache(query3a);

            query1a.complete();
            updateQueryStateCache(query1a);
            query2a.complete();
            updateQueryStateCache(query2a);
            root.processQueuedQueries();
            // group1 and group2 have executed queries, group3 has not.
            // group3 is the least recently executed subgroup
            assertEquals(query1b.getState(), QUEUED);
            assertEquals(query2b.getState(), QUEUED);
            assertEquals(query3a.getState(), RUNNING);

            query3a.complete();
            updateQueryStateCache(query3a);
            root.processQueuedQueries();
            // group1 is the least recently executed subgroup
            assertEquals(query1b.getState(), RUNNING);
            assertEquals(query2b.getState(), QUEUED);

            query1b.complete();
            updateQueryStateCache(query1b);
            root.processQueuedQueries();
            assertEquals(query2b.getState(), RUNNING);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testQueryPrioritySchedulingPolicy()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, ONE_MEGABYTE, 1, 4);
            DistributedResourceGroup group1 = root.getOrCreateSubGroup("1");
            resourceGroupBasicSetUp(group1, ONE_MEGABYTE, 2, 4);
            DistributedResourceGroup group2 = root.getOrCreateSubGroup("2");
            resourceGroupBasicSetUp(group2, ONE_MEGABYTE, 2, 4);
            assertEquals(root.getSchedulingPolicy(), FAIR);
            root.setSchedulingPolicy(QUERY_PRIORITY);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testWeightedSchedulingPolicy()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, ONE_MEGABYTE, 1, 4);
            DistributedResourceGroup group1 = root.getOrCreateSubGroup("1");
            resourceGroupBasicSetUp(group1, ONE_MEGABYTE, 2, 4);
            DistributedResourceGroup group2 = root.getOrCreateSubGroup("2");
            resourceGroupBasicSetUp(group2, ONE_MEGABYTE, 2, 4);
            assertEquals(root.getSchedulingPolicy(), FAIR);
            root.setSchedulingPolicy(WEIGHTED);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testWeightedFairSchedulingPolicy()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, ONE_MEGABYTE, 1, 4);
            DistributedResourceGroup group1 = root.getOrCreateSubGroup("1");
            resourceGroupBasicSetUp(group1, ONE_MEGABYTE, 2, 4);
            DistributedResourceGroup group2 = root.getOrCreateSubGroup("2");
            resourceGroupBasicSetUp(group2, ONE_MEGABYTE, 2, 4);
            assertEquals(root.getSchedulingPolicy(), FAIR);
            root.setSchedulingPolicy(WEIGHTED_FAIR);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test
    public void testSetSchedulingPolicy()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, ONE_MEGABYTE, 1, 4);
            DistributedResourceGroup group1 = root.getOrCreateSubGroup("1");
            resourceGroupBasicSetUp(group1, ONE_MEGABYTE, 2, 4);
            DistributedResourceGroup group2 = root.getOrCreateSubGroup("2");
            resourceGroupBasicSetUp(group2, ONE_MEGABYTE, 2, 4);
            MockManagedQueryExecution query1a = new MockManagedQueryExecution(0);
            group1.run(query1a);
            assertEquals(query1a.getState(), RUNNING);
            query1a.setResourceGroupId(group1.getId());
            updateQueryStateCache(query1a);
            MockManagedQueryExecution query1b = new MockManagedQueryExecution(0);
            group1.run(query1b);
            assertEquals(query1b.getState(), QUEUED);
            query1b.setResourceGroupId(group1.getId());
            updateQueryStateCache(query1b);
            MockManagedQueryExecution query1c = new MockManagedQueryExecution(0);
            group1.run(query1c);
            assertEquals(query1c.getState(), QUEUED);
            query1c.setResourceGroupId(group1.getId());
            updateQueryStateCache(query1c);
            MockManagedQueryExecution query2a = new MockManagedQueryExecution(0);
            group2.run(query2a);
            assertEquals(query2a.getState(), QUEUED);
            query2a.setResourceGroupId(group2.getId());
            updateQueryStateCache(query2a);

            root.setSchedulingPolicy(FAIR);
            assertEquals(root.getOrCreateSubGroup("1").getQueuedQueries(), 2);
            assertEquals(root.getOrCreateSubGroup("2").getQueuedQueries(), 1);

            assertEquals(root.getSchedulingPolicy(), FAIR);
            assertEquals(root.getOrCreateSubGroup("1").getSchedulingPolicy(), FAIR);
            assertEquals(root.getOrCreateSubGroup("2").getSchedulingPolicy(), FAIR);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test
    public void testMemoryLimit()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, ONE_BYTE, 3, 4);
            Set<MockManagedQueryExecution> queries = new HashSet<>();
            // query1 running in remote
            MockManagedQueryExecution query1 = new MockManagedQueryExecution(2);
            query1.startWaitingForResources();
            query1.setResourceGroupId(root.getId());
            updateQueryStateCache(query1);
            // Process the group to refresh stats
            root.processQueuedQueries();
            assertEquals(query1.getState(), RUNNING);
            MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
            query2.setResourceGroupId(root.getId());
            root.run(query2);
            updateQueryStateCache(query2);
            assertEquals(query2.getState(), QUEUED);
            query1.complete();
            updateQueryStateCache(query1);
            root.processQueuedQueries();
            assertEquals(query2.getState(), RUNNING);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test
    public void testSoftReservedMemory()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, TEN_BYTE, 10, 10);
            // three subgroup with setSoftReservedMemory = 1 byte
            DistributedResourceGroup group1 = root.getOrCreateSubGroup("1");
            resourceGroupBasicSetUp(group1, FIVE_BYTE, 3, 4);
            group1.setSoftReservedMemory(ONE_BYTE);

            DistributedResourceGroup group2 = root.getOrCreateSubGroup("2");
            resourceGroupBasicSetUp(group2, FIVE_BYTE, 3, 4);
            group2.setSoftReservedMemory(ONE_BYTE);

            DistributedResourceGroup group3 = root.getOrCreateSubGroup("3");
            resourceGroupBasicSetUp(group3, FIVE_BYTE, 3, 4);
            group3.setSoftReservedMemory(ONE_BYTE);

            MockManagedQueryExecution query1 = new MockManagedQueryExecution(5);
            query1.setResourceGroupId(group1.getId());
            query1.startWaitingForResources();
            updateQueryStateCache(query1);
            MockManagedQueryExecution query2 = new MockManagedQueryExecution(4);
            query2.setResourceGroupId(group2.getId());
            query2.startWaitingForResources();
            updateQueryStateCache(query2);
            // group1 memory usage = 5M, group2 memory usage = 4M, group3 memory usage = 0
            assertEquals(query1.getState(), RUNNING);
            assertEquals(query2.getState(), RUNNING);
            root.processQueuedQueries();
            MockManagedQueryExecution extraQuery = new MockManagedQueryExecution(1);
            group2.run(extraQuery);
            assertEquals(extraQuery.getState(), QUEUED);
            MockManagedQueryExecution query3 = new MockManagedQueryExecution(1);
            group3.run(query3);
            assertEquals(query3.getState(), RUNNING);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test
    public void testSubgroupMemoryLimit()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, TEN_BYTE, 3, 4);
            DistributedResourceGroup subgroup = root.getOrCreateSubGroup("subgroup");
            resourceGroupBasicSetUp(subgroup, ONE_BYTE, 3, 4);

            MockManagedQueryExecution query1 = new MockManagedQueryExecution(2);
            subgroup.run(query1);
            // Process the group to refresh stats
            root.processQueuedQueries();
            assertEquals(query1.getState(), RUNNING);
            query1.setResourceGroupId(subgroup.getId());
            updateQueryStateCache(query1);
            MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
            subgroup.run(query2);
            assertEquals(query2.getState(), QUEUED);
            query2.setResourceGroupId(subgroup.getId());
            updateQueryStateCache(query2);
            MockManagedQueryExecution query3 = new MockManagedQueryExecution(0);
            subgroup.run(query3);
            assertEquals(query3.getState(), QUEUED);
            query3.setResourceGroupId(subgroup.getId());
            updateQueryStateCache(query3);

            query1.complete();
            updateQueryStateCache(query1);
            root.processQueuedQueries();
            assertEquals(query2.getState(), RUNNING);
            assertEquals(query3.getState(), RUNNING);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test
    public void testHardCpuLimit()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, ONE_BYTE, 1, 1);
            root.setHardCpuLimit(new Duration(1, SECONDS));
            root.setCpuQuotaGenerationMillisPerSecond(2000);

            Set<MockManagedQueryExecution> queries = new HashSet<>();
            MockManagedQueryExecution query1 = new MockManagedQueryExecution(1, "query_id", 1, new Duration(2, SECONDS));
            root.run(query1);
            assertEquals(query1.getState(), RUNNING);
            query1.setResourceGroupId(root.getId());
            queries.add(query1);
            updateStateCacheStore(queries);

            MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
            root.run(query2);
            assertEquals(query2.getState(), QUEUED);
            query2.setResourceGroupId(root.getId());
            queries.add(query2);
            updateStateCacheStore(queries);

            query1.complete();
            updateStateCacheStore(queries);
            // update query1's cpu usage(2000ms) to root
            setResourceGroupCpuUsage(root, 2000);
            root.processQueuedQueries();
            assertEquals(query2.getState(), QUEUED);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test
    public void testSoftCpuLimit()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, ONE_BYTE, 2, 1);
            root.setSoftCpuLimit(new Duration(1, SECONDS));
            root.setHardCpuLimit(new Duration(2, SECONDS));
            root.setCpuQuotaGenerationMillisPerSecond(2000);

            Set<MockManagedQueryExecution> queries = new HashSet<>();
            MockManagedQueryExecution query1 = new MockManagedQueryExecution(1, "query_id", 1, new Duration(1, SECONDS));
            root.run(query1);
            assertEquals(query1.getState(), RUNNING);
            query1.setResourceGroupId(root.getId());
            queries.add(query1);
            query1.complete();
            updateQueryStateCache(query1);

            MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
            root.run(query2);
            assertEquals(query2.getState(), RUNNING);
            query2.setResourceGroupId(root.getId());
            updateQueryStateCache(query2);

            // update query1's cpu usage(1000ms) to root
            setResourceGroupCpuUsage(root, 2000);
            MockManagedQueryExecution query3 = new MockManagedQueryExecution(0);
            root.run(query3);
            assertEquals(query3.getState(), QUEUED);
            query3.setResourceGroupId(root.getId());
            updateQueryStateCache(query3);

            // simulate behaviour of generating cpu quota by setting cpuUsageMillis to 0
            setResourceGroupCpuUsage(root, 0);
            root.processQueuedQueries();
            assertEquals(query2.getState(), RUNNING);
            root.processQueuedQueries();
            assertEquals(query3.getState(), RUNNING);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test
    public void testHardReservedConcurrency()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, TEN_BYTE, 10, 10);

            // three subgroups with setHardReservedConcurrency = 1
            DistributedResourceGroup group1 = root.getOrCreateSubGroup("group1");
            resourceGroupBasicSetUp(group1, ONE_BYTE, 5, 20);
            group1.setHardReservedConcurrency(1);

            DistributedResourceGroup group2 = root.getOrCreateSubGroup("group2");
            resourceGroupBasicSetUp(group2, ONE_BYTE, 5, 20);
            group2.setHardReservedConcurrency(1);

            DistributedResourceGroup group3 = root.getOrCreateSubGroup("group3");
            resourceGroupBasicSetUp(group3, ONE_BYTE, 5, 20);
            group3.setHardReservedConcurrency(1);

            Set<MockManagedQueryExecution> queries = new HashSet<>();
            queries.addAll(fillGroupTo(group1, ImmutableSet.of(), 4, true));
            queries.addAll(fillGroupTo(group2, ImmutableSet.of(), 5, true));
            updateStateCacheStore(queries);
            assertEquals(group1.getQueuedQueries(), 0);
            assertEquals(group1.getRunningQueries(), 4);
            assertEquals(group2.getQueuedQueries(), 0);
            assertEquals(group2.getRunningQueries(), 5);
            MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
            group1.run(query1);
            assertEquals(query1.getState(), QUEUED);
            MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
            group3.run(query2);
            assertEquals(query2.getState(), RUNNING);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test
    public void testGetInfo()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            // Start with zero capacity, so that nothing starts running until we've added all the queries
            resourceGroupBasicSetUp(root, ONE_MEGABYTE, 0, 40);

            DistributedResourceGroup rootA = root.getOrCreateSubGroup("a");
            resourceGroupBasicSetUp(rootA, ONE_MEGABYTE, 10, 20);

            DistributedResourceGroup rootB = root.getOrCreateSubGroup("b");
            resourceGroupBasicSetUp(rootB, ONE_MEGABYTE, 10, 20);

            DistributedResourceGroup rootAX = rootA.getOrCreateSubGroup("x");
            resourceGroupBasicSetUp(rootAX, ONE_MEGABYTE, 10, 10);

            DistributedResourceGroup rootAY = rootA.getOrCreateSubGroup("y");
            resourceGroupBasicSetUp(rootAY, ONE_MEGABYTE, 10, 10);

            DistributedResourceGroup rootBX = rootB.getOrCreateSubGroup("x");
            resourceGroupBasicSetUp(rootBX, ONE_MEGABYTE, 10, 10);

            DistributedResourceGroup rootBY = rootB.getOrCreateSubGroup("y");
            resourceGroupBasicSetUp(rootBY, ONE_MEGABYTE, 10, 10);

            // Queue 40 queries (= maxQueuedQueries (40) + maxRunningQueries (0))
            Set<MockManagedQueryExecution> queries = fillGroupTo(rootAX, ImmutableSet.of(), 10, false);
            queries.addAll(fillGroupTo(rootAY, ImmutableSet.of(), 10, false));
            queries.addAll(fillGroupTo(rootBX, ImmutableSet.of(), 10, true));
            queries.addAll(fillGroupTo(rootBY, ImmutableSet.of(), 10, true));

            updateStateCacheStore(queries);
            root.processQueuedQueries();
            ResourceGroupInfo info = root.getInfo();
            assertEquals(info.getNumRunningQueries(), 0);
            assertEquals(info.getNumQueuedQueries(), 40);

            root.setHardConcurrencyLimit(10);
            root.processQueuedQueries();
            updateStateCacheStore(queries);
            root.processQueuedQueries();
            info = root.getInfo();
            assertEquals(info.getNumRunningQueries(), 10);
            assertEquals(info.getNumQueuedQueries(), 30);

            // Complete running queries
            Iterator<MockManagedQueryExecution> iterator = queries.iterator();
            while (iterator.hasNext()) {
                MockManagedQueryExecution query = iterator.next();
                if (query.getState() == RUNNING) {
                    query.complete();
                    iterator.remove();
                    updateQueryStateCache(query);
                }
            }

            // 10 more queries start running, 20 left queued.
            root.processQueuedQueries();
            updateStateCacheStore(queries);
            root.processQueuedQueries();
            info = root.getInfo();
            assertEquals(info.getNumRunningQueries(), 10);
            assertEquals(info.getNumQueuedQueries(), 20);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test
    public void testGetResourceGroupStateInfo()
    {
        synchronized (lock) {
            DistributedResourceGroup root = new DistributedResourceGroup(Optional.empty(), "root", (group, export) -> {}, directExecutor(), statestore);
            resourceGroupBasicSetUp(root, ONE_GIGABYTE, 20, 40);
            root.setSoftConcurrencyLimit(20);

            DistributedResourceGroup rootA = root.getOrCreateSubGroup("a");
            resourceGroupBasicSetUp(rootA, ONE_MEGABYTE, 10, 20);

            DistributedResourceGroup rootB = root.getOrCreateSubGroup("b");
            resourceGroupBasicSetUp(rootB, ONE_MEGABYTE, 10, 20);

            DistributedResourceGroup rootAX = rootA.getOrCreateSubGroup("x");
            resourceGroupBasicSetUp(rootAX, ONE_MEGABYTE, 10, 10);

            DistributedResourceGroup rootAY = rootA.getOrCreateSubGroup("y");
            resourceGroupBasicSetUp(rootAY, ONE_MEGABYTE, 10, 10);

            Set<MockManagedQueryExecution> queries = fillGroupTo(rootAX, ImmutableSet.of(), 5, false);
            queries.addAll(fillGroupTo(rootAY, ImmutableSet.of(), 5, false));
            queries.addAll(fillGroupTo(rootB, ImmutableSet.of(), 10, true));
            updateStateCacheStore(queries);
            root.processQueuedQueries();

            ResourceGroupInfo rootInfo = root.getFullInfo();
            assertEquals(rootInfo.getId(), root.getId());
            assertEquals(rootInfo.getState(), CAN_QUEUE);
            assertEquals(rootInfo.getSoftMemoryLimit(), root.getSoftMemoryLimit());
            assertEquals(rootInfo.getMemoryUsage(), new DataSize(0, BYTE));
            assertEquals(rootInfo.getSubGroups().size(), 2);
            assertGroupInfoEquals(rootInfo.getSubGroups().get(0), rootA.getInfo());
            assertEquals(rootInfo.getSubGroups().get(0).getId(), rootA.getId());
            assertEquals(rootInfo.getSubGroups().get(0).getState(), CAN_QUEUE);
            assertEquals(rootInfo.getSubGroups().get(0).getSoftMemoryLimit(), rootA.getSoftMemoryLimit());
            assertEquals(rootInfo.getSubGroups().get(0).getSoftReservedMemory(), rootA.getSoftReservedMemory());
            assertEquals(rootInfo.getSubGroups().get(0).getHardReservedConcurrency(), rootA.getHardReservedConcurrency());
            assertEquals(rootInfo.getSubGroups().get(0).getSchedulingWeight(), rootA.getSchedulingWeight());
            assertEquals(rootInfo.getSubGroups().get(0).getSchedulingPolicy(), rootA.getSchedulingPolicy());
            assertEquals(rootInfo.getSubGroups().get(0).getHardConcurrencyLimit(), rootA.getHardConcurrencyLimit());
            assertEquals(rootInfo.getSubGroups().get(0).getMaxQueuedQueries(), rootA.getMaxQueuedQueries());
            assertEquals(rootInfo.getSubGroups().get(0).getNumRunningQueries(), 10);
            assertEquals(rootInfo.getSubGroups().get(0).getNumQueuedQueries(), 0);
            assertGroupInfoEquals(rootInfo.getSubGroups().get(1), rootB.getInfo());
            assertEquals(rootInfo.getSubGroups().get(1).getId(), rootB.getId());
            assertEquals(rootInfo.getSubGroups().get(1).getState(), CAN_QUEUE);
            assertEquals(rootInfo.getSubGroups().get(1).getSoftMemoryLimit(), rootB.getSoftMemoryLimit());
            assertEquals(rootInfo.getSubGroups().get(1).getSoftReservedMemory(), rootA.getSoftReservedMemory());
            assertEquals(rootInfo.getSubGroups().get(1).getHardReservedConcurrency(), rootA.getHardReservedConcurrency());
            assertEquals(rootInfo.getSubGroups().get(1).getSchedulingWeight(), rootA.getSchedulingWeight());
            assertEquals(rootInfo.getSubGroups().get(1).getSchedulingPolicy(), rootA.getSchedulingPolicy());
            assertEquals(rootInfo.getSubGroups().get(1).getHardConcurrencyLimit(), rootB.getHardConcurrencyLimit());
            assertEquals(rootInfo.getSubGroups().get(1).getMaxQueuedQueries(), rootB.getMaxQueuedQueries());
            assertEquals(rootInfo.getSubGroups().get(1).getNumEligibleSubGroups(), 0);
            assertEquals(rootInfo.getSubGroups().get(1).getNumRunningQueries(), 10);
            assertEquals(rootInfo.getSubGroups().get(1).getNumQueuedQueries(), 0);
            assertEquals(rootInfo.getSoftConcurrencyLimit(), root.getSoftConcurrencyLimit());
            assertEquals(rootInfo.getHardConcurrencyLimit(), root.getHardConcurrencyLimit());
            assertEquals(rootInfo.getMaxQueuedQueries(), root.getMaxQueuedQueries());
            assertEquals(rootInfo.getNumQueuedQueries(), 0);
            assertEquals(rootInfo.getRunningQueries().size(), 20);
            QueryStateInfo queryInfo = rootInfo.getRunningQueries().get(0);
            assertEquals(queryInfo.getResourceGroupId(), Optional.of(rootAX.getId()));
            StateCacheStore.get().resetCachedStates();
        }
    }

    private void setResourceGroupCpuUsage(DistributedResourceGroup group, long cpuUsageMillis)
    {
        // manually add cpu usage of query1(1000ms) to root
        Map<ResourceGroupId, SharedResourceGroupState> resourceGroupStates = StateCacheStore.get().getCachedStates(StateStoreConstants.RESOURCE_GROUP_STATE_COLLECTION_NAME);
        resourceGroupStates.get(group.getId()).setCpuUsageMillis(cpuUsageMillis);
        StateCacheStore.get().setCachedStates(StateStoreConstants.RESOURCE_GROUP_STATE_COLLECTION_NAME, resourceGroupStates);
    }

    // Set up mandatory fields for DistributedResourceGroup
    private void resourceGroupBasicSetUp(DistributedResourceGroup group, DataSize softMemoryLimit, int hardConcurrencyLimit, int maxQueued)
    {
        group.setSoftMemoryLimit(softMemoryLimit);
        group.setMaxQueuedQueries(maxQueued);
        group.setHardConcurrencyLimit(hardConcurrencyLimit);
    }

    // update cached queryStates for a set of queries
    private static void updateStateCacheStore(Set<MockManagedQueryExecution> queries)
    {
        Map<String, SharedQueryState> queryStates = new HashMap<>();
        for (Iterator<MockManagedQueryExecution> iterator = queries.iterator(); iterator.hasNext(); ) {
            MockManagedQueryExecution query = iterator.next();
            queryStates.put(query.toString(), getSharedQueryState(query));
        }
        StateCacheStore.get().setCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME, queryStates);
        DistributedResourceGroupUtils.mapCachedStates();
    }

    // update cached queryStates for a single query
    private static void updateQueryStateCache(MockManagedQueryExecution query)
    {
        Map<String, SharedQueryState> queryStates = StateCacheStore.get().getCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME);
        if (queryStates == null) {
            queryStates = new HashMap<>();
        }
        String queryKey = query.toString();
        queryStates.put(queryKey, getSharedQueryState(query));
        StateCacheStore.get().setCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME, queryStates);
        DistributedResourceGroupUtils.mapCachedStates();
    }

    // Create SharedQueryState for queries
    private static SharedQueryState getSharedQueryState(MockManagedQueryExecution query)
    {
        return new SharedQueryState(
                query.getBasicQueryInfo(),
                query.getSession().toSessionRepresentation(),
                query.getErrorCode(),
                query.getUserMemoryReservation(),
                query.getTotalMemoryReservation(),
                query.getTotalCpuTime(),
                new DateTime(),
                Optional.of(new DateTime()));
    }

    private static Set<MockManagedQueryExecution> fillGroupTo(DistributedResourceGroup group, Set<MockManagedQueryExecution> existingQueries, int count, boolean queryPriority)
    {
        int existingCount = existingQueries.size();
        Set<MockManagedQueryExecution> queries = new HashSet<>(existingQueries);
        for (int i = 0; i < count - existingCount; i++) {
            MockManagedQueryExecution query = new MockManagedQueryExecution(0, group.getId().toString().replace(".", "") + Integer.toString(i), queryPriority ? i + 1 : 1);
            query.setResourceGroupId(group.getId());
            queries.add(query);
            group.run(query);
        }
        return queries;
    }

    private static void assertGroupInfoEquals(ResourceGroupInfo actual, ResourceGroupInfo expected)
    {
        assertTrue(actual.getSchedulingWeight() == expected.getSchedulingWeight() &&
                actual.getSoftConcurrencyLimit() == expected.getSoftConcurrencyLimit() &&
                actual.getHardConcurrencyLimit() == expected.getHardConcurrencyLimit() &&
                actual.getHardReservedConcurrency() == expected.getHardReservedConcurrency() &&
                actual.getMaxQueuedQueries() == expected.getMaxQueuedQueries() &&
                actual.getNumQueuedQueries() == expected.getNumQueuedQueries() &&
                actual.getNumRunningQueries() == expected.getNumRunningQueries() &&
                actual.getNumEligibleSubGroups() == expected.getNumEligibleSubGroups() &&
                Objects.equals(actual.getId(), expected.getId()) &&
                actual.getState() == expected.getState() &&
                actual.getSchedulingPolicy() == expected.getSchedulingPolicy() &&
                Objects.equals(actual.getSoftMemoryLimit(), expected.getSoftMemoryLimit()) &&
                Objects.equals(actual.getSoftReservedMemory(), expected.getSoftReservedMemory()) &&
                Objects.equals(actual.getMemoryUsage(), expected.getMemoryUsage()));
    }
}
