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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.hetu.core.common.util.DataSizeOfUtil;
import io.prestosql.exchange.ExchangeSourceHandle;
import io.prestosql.execution.StageId;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.plan.PlanNodeId;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestTaskDescriptorStorage
{
    private static final QueryId QUERY_1 = new QueryId("query1");
    private static final QueryId QUERY_2 = new QueryId("query2");

    private static final StageId QUERY_1_STAGE_1 = new StageId(QUERY_1, 1);
    private static final StageId QUERY_1_STAGE_2 = new StageId(QUERY_1, 2);
    private static final StageId QUERY_2_STAGE_1 = new StageId(QUERY_2, 1);
    private static final StageId QUERY_2_STAGE_2 = new StageId(QUERY_2, 2);

    @Test
    public void testHappyPath()
    {
        TaskDescriptorStorage manager = new TaskDescriptorStorage(new DataSize(15, KILOBYTE));
        manager.initialize(QUERY_1);
        manager.initialize(QUERY_2);

        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, new DataSize(1, KILOBYTE), "catalog1"));
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(0, new DataSize(1, KILOBYTE), "catalog2"));
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(1, new DataSize(2, KILOBYTE), "catalog3"));
        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(0, new DataSize(1, KILOBYTE), "catalog4"));
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(0, new DataSize(1, KILOBYTE), "catalog5"));
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(1, new DataSize(2, KILOBYTE), "catalog6"));

        assertTrue(manager.getReservedBytes() >= new DataSize(8, KILOBYTE).toBytes());
        assertTrue(manager.getReservedBytes() <= new DataSize(10, KILOBYTE).toBytes());

        assertTrue(manager.get(QUERY_1_STAGE_1, 0).flatMap(TestTaskDescriptorStorage::getCatalogName).get().contains("catalog1"));
        assertTrue(manager.get(QUERY_1_STAGE_2, 0).flatMap(TestTaskDescriptorStorage::getCatalogName).get().contains("catalog2"));
        assertTrue(manager.get(QUERY_1_STAGE_2, 1).flatMap(TestTaskDescriptorStorage::getCatalogName).get().contains("catalog3"));
        assertTrue(manager.get(QUERY_2_STAGE_1, 0).flatMap(TestTaskDescriptorStorage::getCatalogName).get().contains("catalog4"));
        assertTrue(manager.get(QUERY_2_STAGE_2, 0).flatMap(TestTaskDescriptorStorage::getCatalogName).get().contains("catalog5"));
        assertTrue(manager.get(QUERY_2_STAGE_2, 1).flatMap(TestTaskDescriptorStorage::getCatalogName).get().contains("catalog6"));

        manager.remove(QUERY_1_STAGE_1, 0);
        manager.remove(QUERY_2_STAGE_2, 1);

        assertThrows(() -> manager.get(QUERY_1_STAGE_1, 0));
        assertThrows(() -> manager.get(QUERY_2_STAGE_2, 1));

        assertTrue(manager.getReservedBytes() >= new DataSize(5, KILOBYTE).toBytes());
        assertTrue(manager.getReservedBytes() <= new DataSize(7, KILOBYTE).toBytes());
    }

    @Test
    public void testDestroy()
    {
        TaskDescriptorStorage manager = new TaskDescriptorStorage(new DataSize(5, KILOBYTE));
        manager.initialize(QUERY_1);
        manager.initialize(QUERY_2);

        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, new DataSize(1, KILOBYTE), Optional.empty()));
        assertTrue(manager.get(QUERY_1_STAGE_1, 0).isPresent());
        assertTrue(manager.getReservedBytes() >= new DataSizeOfUtil(1, KILOBYTE).toBytes());
        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(0, new DataSize(1, KILOBYTE), Optional.empty()));
        assertTrue(manager.get(QUERY_2_STAGE_1, 0).isPresent());
        assertTrue(manager.getReservedBytes() >= new DataSizeOfUtil(2, KILOBYTE).toBytes());

        manager.destroy(QUERY_1);
        assertTrue(!manager.get(QUERY_1_STAGE_1, 0).isPresent());
        assertTrue(manager.get(QUERY_2_STAGE_1, 0).isPresent());
        assertTrue(manager.getReservedBytes() >= new DataSize(1, KILOBYTE).toBytes());
        assertTrue(manager.getReservedBytes() <= new DataSize(2, KILOBYTE).toBytes());

        manager.destroy(QUERY_2);

        assertTrue(!manager.get(QUERY_1_STAGE_1, 0).isPresent());
        assertTrue(!manager.get(QUERY_2_STAGE_1, 0).isPresent());
        assertEquals(manager.getReservedBytes(), 0);
    }

    @Test
    public void testCapacityExceeded()
    {
        TaskDescriptorStorage manager = new TaskDescriptorStorage(new DataSize(5, KILOBYTE));
        manager.initialize(QUERY_1);
        manager.initialize(QUERY_2);

        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, new DataSize(1, KILOBYTE), "catalog1"));
        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(1, new DataSize(1, KILOBYTE), "catalog2"));
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(0, new DataSize(1, KILOBYTE), "catalog3"));
        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(0, new DataSize(1, KILOBYTE), "catalog4"));
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(0, new DataSize(2, KILOBYTE), "catalog5"));

        assertTrue(manager.getReservedBytes() >= new DataSize(3, KILOBYTE).toBytes());
        assertTrue(manager.getReservedBytes() <= new DataSize(4, KILOBYTE).toBytes());

        assertThrows(() -> manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, new DataSize(1, KILOBYTE), Optional.empty())));
        assertThrows(() -> manager.put(QUERY_1_STAGE_1, createTaskDescriptor(1, new DataSize(1, KILOBYTE), Optional.empty())));
        assertThrows(() -> manager.get(QUERY_1_STAGE_1, 0));
        assertThrows(() -> manager.get(QUERY_1_STAGE_1, 1));
        assertThrows(() -> manager.get(QUERY_1_STAGE_2, 0));
        assertThrows(() -> manager.remove(QUERY_1_STAGE_1, 0));
        assertThrows(() -> manager.remove(QUERY_1_STAGE_1, 1));
        assertThrows(() -> manager.remove(QUERY_1_STAGE_2, 0));

        assertTrue(manager.get(QUERY_2_STAGE_1, 0).flatMap(TestTaskDescriptorStorage::getCatalogName).get().contains("catalog4"));
        assertTrue(manager.get(QUERY_2_STAGE_2, 0).flatMap(TestTaskDescriptorStorage::getCatalogName).get().contains("catalog5"));

        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(1, new DataSize(3, KILOBYTE), "catalog6"));

        assertEquals(manager.getReservedBytes(), 0);

        assertThrows(() -> manager.put(QUERY_2_STAGE_2, createTaskDescriptor(2, new DataSize(3, KILOBYTE), Optional.empty())));
        assertThrows(() -> manager.get(QUERY_2_STAGE_1, 0));
        assertThrows(() -> manager.remove(QUERY_2_STAGE_1, 0));
    }

    private static Optional<String> getCatalogName(TaskDescriptor descriptor)
    {
        return descriptor.getNodeRequirements().getCatalogName().map(CatalogName::getCatalogName);
    }

    private static TaskDescriptor createTaskDescriptor(int partitionId, DataSize retainedSize, String catalogName)
    {
        return createTaskDescriptor(partitionId, retainedSize, Optional.of(new CatalogName(catalogName)));
    }

    private static TaskDescriptor createTaskDescriptor(int partitionId, DataSize retainedSize, Optional<CatalogName> catalogName)
    {
        return new TaskDescriptor(partitionId,
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(new PlanNodeId("1"),
                        new TestingExchangeSourceHandle(retainedSize.toBytes())),
                new NodeRequirements(catalogName, ImmutableSet.of(), new DataSize(5, KILOBYTE)));
    }

    private static class TestingExchangeSourceHandle
            implements ExchangeSourceHandle
    {
        private final long retainedSizeInBytes;

        private TestingExchangeSourceHandle(long retainedSizeInBytes)
        {
            this.retainedSizeInBytes = retainedSizeInBytes;
        }

        @Override
        public int getPartitionId()
        {
            return 0;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return retainedSizeInBytes;
        }
    }
}
