/*
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
package io.prestosql.execution.scheduler;

import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.client.NodeVersion;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.InternalNode;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.util.FinalizerService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

// uses mutable state
@Test(singleThreaded = true)
public class TestFixedCountNodeAllocator
{
    private static final Session SESSION = testSessionBuilder().build();

    private static final HostAddress NODE_1_ADDRESS = HostAddress.fromParts("127.0.0.1", 8080);
    private static final HostAddress NODE_2_ADDRESS = HostAddress.fromParts("127.0.0.1", 8081);
    private static final HostAddress NODE_3_ADDRESS = HostAddress.fromParts("127.0.0.1", 8082);

    private static final InternalNode NODE_1 = new InternalNode("node-1", URI.create("local://" + NODE_1_ADDRESS), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_2 = new InternalNode("node-2", URI.create("local://" + NODE_2_ADDRESS), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_3 = new InternalNode("node-3", URI.create("local://" + NODE_3_ADDRESS), NodeVersion.UNKNOWN, false);

    private static final CatalogName CATALOG_1 = new CatalogName("catalog1");
    private static final CatalogName CATALOG_2 = new CatalogName("catalog2");

    private FinalizerService finalizerService;
    private NodeTaskMap nodeTaskMap;
    private InMemoryNodeManager nodeManager;

    private FixedCountNodeAllocatorService nodeAllocatorService;

    @BeforeMethod
    public void setUp()
    {
        PropertyService.setProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED, false);

        finalizerService = new FinalizerService();
        nodeTaskMap = new NodeTaskMap(finalizerService);
        nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(CATALOG_1, NODE_1);
        nodeManager.addNode(CATALOG_2, NODE_2, NODE_3);
        finalizerService.start();
    }

    @AfterMethod(alwaysRun = true)
    public void shutdownNodeAllocatorService()
    {
        if (nodeAllocatorService != null) {
            nodeAllocatorService.stop();
        }
        nodeAllocatorService = null;
    }

    private void setupNodeAllocatorService()
    {
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setIncludeCoordinator(true);
        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, nodeSchedulerConfig, nodeTaskMap);
        shutdownNodeAllocatorService(); // just in case
        nodeAllocatorService = new FixedCountNodeAllocatorService(nodeScheduler);
    }

    @Test
    public void testSingleNode()
            throws Exception
    {
        setupNodeAllocatorService();

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 1)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of(), new DataSize(1, GIGABYTE)));
            assertTrue(acquire1.getNode().isDone());
            assertEquals(acquire1.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of(), new DataSize(1, GIGABYTE)));
            assertFalse(acquire2.getNode().isDone());

            acquire1.release();

            assertTrue(acquire2.getNode().isDone());
            assertEquals(acquire2.getNode().get(), NODE_1);
        }

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(SESSION, 2)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of(), new DataSize(1, GIGABYTE)));
            assertTrue(acquire1.getNode().isDone());
            assertEquals(acquire1.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of(), new DataSize(1, GIGABYTE)));
            assertTrue(acquire2.getNode().isDone());
            assertEquals(acquire2.getNode().get(), NODE_1);

            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of(), new DataSize(1, GIGABYTE)));
            assertFalse(acquire3.getNode().isDone());

            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(new NodeRequirements(Optional.of(CATALOG_1), ImmutableSet.of(), new DataSize(1, GIGABYTE)));
            assertFalse(acquire4.getNode().isDone());

            acquire2.release(); // NODE_1
            assertTrue(acquire3.getNode().isDone());
            assertEquals(acquire3.getNode().get(), NODE_1);

            acquire3.release(); // NODE_1
            assertTrue(acquire4.getNode().isDone());
            assertEquals(acquire4.getNode().get(), NODE_1);
        }
    }
}
