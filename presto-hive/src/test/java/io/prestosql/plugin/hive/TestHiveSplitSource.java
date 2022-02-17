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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.Assertions.assertContains;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.plugin.hive.HiveTestUtils.createTestDynamicFilterSupplier;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHiveSplitSource
{
    @Test
    public void testOutstandingSplitCount()
    {
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, new HiveConfig(),
                HiveStorageFormat.ORC);

        // add 10 splits
        for (int i = 0; i < 10; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }

        // remove 1 split
        assertEquals(getSplits(hiveSplitSource, 1).size(), 1);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 9);

        // remove 4 splits
        assertEquals(getSplits(hiveSplitSource, 4).size(), 4);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 5);

        // try to remove 20 splits, and verify we only got 5
        assertEquals(getSplits(hiveSplitSource, 20).size(), 5);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 0);
    }

    @Test
    public void testFail()
    {
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, new HiveConfig(),
                HiveStorageFormat.ORC);

        // add some splits
        for (int i = 0; i < 5; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }

        // remove a split and verify
        assertEquals(getSplits(hiveSplitSource, 1).size(), 1);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 4);

        // fail source
        hiveSplitSource.fail(new RuntimeException("test"));
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 4);

        // try to remove a split and verify we got the expected exception
        try {
            getSplits(hiveSplitSource, 1);
            fail("expected RuntimeException");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "test");
        }
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 4); // 3 splits + poison

        // attempt to add another split and verify it does not work
        hiveSplitSource.addToQueue(new TestSplit(99));
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 4); // 3 splits + poison

        // fail source again
        hiveSplitSource.fail(new RuntimeException("another failure"));
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 4); // 3 splits + poison

        // try to remove a split and verify we got the first exception
        try {
            getSplits(hiveSplitSource, 1);
            fail("expected RuntimeException");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "test");
        }
    }

    @Test
    public void testReaderWaitsForSplits()
            throws Exception
    {
        final HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, new HiveConfig(),
                HiveStorageFormat.ORC);

        final SettableFuture<ConnectorSplit> splits = SettableFuture.create();

        // create a thread that will get a split
        final CountDownLatch started = new CountDownLatch(1);
        Thread getterThread = new Thread(() -> {
            try {
                started.countDown();
                List<ConnectorSplit> batch = getSplits(hiveSplitSource, 1);
                assertEquals(batch.size(), 1);
                splits.set(batch.get(0));
            }
            catch (Throwable e) {
                splits.setException(e);
            }
        });
        getterThread.setName("testReaderWaitsForSplits");
        getterThread.setUncaughtExceptionHandler((thread, throwable) -> System.out.println(thread.getName() + " : " + throwable.getMessage()));
        getterThread.start();

        try {
            // wait for the thread to be started
            assertTrue(started.await(1, TimeUnit.SECONDS));

            // sleep for a bit, and assure the thread is blocked
            TimeUnit.MILLISECONDS.sleep(200);
            assertTrue(!splits.isDone());

            // add a split
            hiveSplitSource.addToQueue(new TestSplit(33));

            // wait for thread to get the split
            ConnectorSplit split = splits.get(800, TimeUnit.MILLISECONDS);
            assertEquals(HiveSplitWrapper.getOnlyHiveSplit(split).getSchema().getProperty("id"), "33");
        }
        finally {
            // make sure the thread exits
            getterThread.interrupt();
        }
    }

    @Test
    public void testOutstandingSplitSize()
    {
        DataSize maxOutstandingSplitsSize = new DataSize(1, MEGABYTE);
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10000,
                maxOutstandingSplitsSize,
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, new HiveConfig(),
                HiveStorageFormat.ORC);
        int testSplitSizeInBytes = new TestSplit(0).getEstimatedSizeInBytes();

        int maxSplitCount = toIntExact(maxOutstandingSplitsSize.toBytes()) / testSplitSizeInBytes;
        for (int i = 0; i < maxSplitCount; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }

        assertEquals(getSplits(hiveSplitSource, maxSplitCount).size(), maxSplitCount);

        for (int i = 0; i < maxSplitCount; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }
        try {
            hiveSplitSource.addToQueue(new TestSplit(0));
            fail("expect failure");
        }
        catch (PrestoException e) {
            assertContains(e.getMessage(), "Split buffering for database.table exceeded memory limit");
        }
    }

    @Test
    public void testEmptyBucket()
    {
        final HiveSplitSource hiveSplitSource = HiveSplitSource.bucketed(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, new HiveConfig(),
                HiveStorageFormat.ORC);
        hiveSplitSource.addToQueue(new TestSplit(0, OptionalInt.of(2)));
        hiveSplitSource.noMoreSplits();
        assertEquals(getSplits(hiveSplitSource, OptionalInt.of(0), 10).size(), 0);
        assertEquals(getSplits(hiveSplitSource, OptionalInt.of(1), 10).size(), 0);
        assertEquals(getSplits(hiveSplitSource, OptionalInt.of(2), 10).size(), 1);
        assertEquals(getSplits(hiveSplitSource, OptionalInt.of(3), 10).size(), 0);
    }

    @Test
    public void testHiveSplitSourceWithDynamicFilter()
    {
        TypeManager typeManager = new TestingTypeManager();
        ConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(true), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                createTestDynamicFilterSupplier("pt_d", ImmutableList.of(1L)),
                null,
                typeManager,
                new HiveConfig(),
                HiveStorageFormat.ORC);

        for (int i = 0; i < 5; i++) {
            hiveSplitSource.addToQueue(new TestPartitionSplit(2 * i, ImmutableList.of(new HivePartitionKey("pt_d", "0")), "pt_d=0"));
            hiveSplitSource.addToQueue(new TestPartitionSplit(2 * i + 1, ImmutableList.of(new HivePartitionKey("pt_d", "1")), "pt_d=1"));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 2 * i + 2);
        }

        assertEquals(getSplits(hiveSplitSource, 10).size(), 5);
    }

    private static List<ConnectorSplit> getSplits(ConnectorSplitSource source, int maxSize)
    {
        return getSplits(source, OptionalInt.empty(), maxSize);
    }

    private static List<ConnectorSplit> getSplits(ConnectorSplitSource source, OptionalInt bucketNumber, int maxSize)
    {
        if (bucketNumber.isPresent()) {
            return getFutureValue(source.getNextBatch(new HivePartitionHandle(bucketNumber.getAsInt()), maxSize)).getSplits();
        }
        else {
            return getFutureValue(source.getNextBatch(NOT_PARTITIONED, maxSize)).getSplits();
        }
    }

    private static class TestingHiveSplitLoader
            implements HiveSplitLoader
    {
        @Override
        public void start(HiveSplitSource splitSource)
        {
        }

        @Override
        public void stop()
        {
        }
    }

    private static class TestSplit
            extends InternalHiveSplit
    {
        private TestSplit(int id, List<HostAddress> hostAddress)
        {
            this(id, OptionalInt.empty(), 100, hostAddress);
        }

        private TestSplit(int id)
        {
            this(id, OptionalInt.empty(), 100, ImmutableList.of());
        }

        private TestSplit(int id, OptionalInt bucketNumber)
        {
            this(id, bucketNumber, 100, ImmutableList.of());
        }

        private TestSplit(int id, OptionalInt bucketNumber, long fileSize, List<HostAddress> hostAddress)
        {
            super(
                    "partition-name",
                    "path",
                    0,
                    100,
                    fileSize,
                    0,
                    properties("id", String.valueOf(id)),
                    ImmutableList.of(),
                    ImmutableList.of(new InternalHiveBlock(0, 100, hostAddress)),
                    bucketNumber,
                    true,
                    false,
                    ImmutableMap.of(),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of());
        }

        private static Properties properties(String key, String value)
        {
            Properties properties = new Properties();
            properties.put(key, value);
            return properties;
        }
    }

    private static class TestPartitionSplit
            extends InternalHiveSplit
    {
        private TestPartitionSplit(int id, List<HivePartitionKey> partitionKeys, String partitionName)
        {
            this(id, partitionKeys, partitionName, OptionalInt.empty());
        }

        private TestPartitionSplit(int id, List<HivePartitionKey> partitionKeys, String partitionName, OptionalInt bucketNumber)
        {
            super(
                    partitionName,
                    "path",
                    0,
                    100,
                    100,
                    0,
                    properties("id", String.valueOf(id)),
                    partitionKeys,
                    ImmutableList.of(new InternalHiveBlock(0, 100, ImmutableList.of())),
                    bucketNumber,
                    true,
                    false,
                    ImmutableMap.of(),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of());
        }

        private static Properties properties(String key, String value)
        {
            Properties properties = new Properties();
            properties.put(key, value);
            return properties;
        }
    }

    @Test
    public void testGroupSmallSplit()
    {
        HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setMaxSplitsToGroup(10);
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, hiveConfig,
                HiveStorageFormat.ORC);

        List<HostAddress> hostAddress = new ArrayList<>();
        hostAddress.add(new HostAddress("vm1", 1));
        hostAddress.add(new HostAddress("vm3", 1));
        hostAddress.add(new HostAddress("vm2", 1));

        for (int i = 0; i < 12; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i, hostAddress));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }

        List<ConnectorSplit> connectorSplits = getSplits(hiveSplitSource, 100);
        List<ConnectorSplit> groupedConnectorSplits = hiveSplitSource.groupSmallSplits(connectorSplits, 1);
        assertEquals(groupedConnectorSplits.size(), 3);
        List<HiveSplitWrapper> hiveSplitWrappers = new ArrayList<>();
        groupedConnectorSplits.forEach(pendingSplit -> hiveSplitWrappers.add((HiveSplitWrapper) pendingSplit));
        assertEquals(hiveSplitWrappers.get(0).getSplits().size(), 4);
        assertEquals(hiveSplitWrappers.get(1).getSplits().size(), 4);
        assertEquals(hiveSplitWrappers.get(2).getSplits().size(), 4);
    }

    @Test
    public void testGroupSmallSplitReplicationFactor1()
    {
        HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setMaxSplitsToGroup(10);
        // ReplicationFactor 1 & all splits have same location
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, hiveConfig,
                HiveStorageFormat.ORC);

        List<HostAddress> hostAddress = new ArrayList<>();
        hostAddress.add(new HostAddress("vm1", 1));

        for (int i = 0; i < 30; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i, hostAddress));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }

        List<ConnectorSplit> connectorSplits = getSplits(hiveSplitSource, 100);
        List<ConnectorSplit> groupedConnectorSplits = hiveSplitSource.groupSmallSplits(connectorSplits, 1);
        assertEquals(groupedConnectorSplits.size(), 3);
        List<HiveSplitWrapper> hiveSplitWrappers = new ArrayList<>();
        groupedConnectorSplits.forEach(pendingSplit -> hiveSplitWrappers.add((HiveSplitWrapper) pendingSplit));
        assertEquals(hiveSplitWrappers.get(0).getSplits().size(), 10);
        assertEquals(hiveSplitWrappers.get(1).getSplits().size(), 10);
        assertEquals(hiveSplitWrappers.get(2).getSplits().size(), 10);
    }

    @Test
    public void testGroupSmallSplitReplicationFactor2()
    {
        HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setMaxSplitsToGroup(10);
        // ReplicationFactor 2 & Number of nodes 3 , split should be distributed equally among 3 nodes
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, hiveConfig,
                HiveStorageFormat.ORC);

        for (int i = 0; i < 24; i++) {
            List<HostAddress> hostAddress = new ArrayList<>();
            hostAddress.add(new HostAddress("vm" + (i % 3), 1));
            hostAddress.add(new HostAddress("vm" + ((i + 1) % 3), 1));
            hiveSplitSource.addToQueue(new TestSplit(i, hostAddress));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }

        List<ConnectorSplit> connectorSplits = getSplits(hiveSplitSource, 100);
        List<ConnectorSplit> groupedConnectorSplits = hiveSplitSource.groupSmallSplits(connectorSplits, 1);
        assertEquals(groupedConnectorSplits.size(), 6);
        List<HiveSplitWrapper> hiveSplitWrappers = new ArrayList<>();
        groupedConnectorSplits.forEach(pendingSplit -> hiveSplitWrappers.add((HiveSplitWrapper) pendingSplit));
        for (int i = 0; i < 6; i++) {
            assertEquals(hiveSplitWrappers.get(i).getSplits().size(), 4);
        }
    }

    @Test
    public void testGroupSmallSplitReplicationFactor2MoreThan10SplitsPerNode()
    {
        HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setMaxSplitsToGroup(10);
        // ReplicationFactor 2 & Number of nodes 3, 10 splits need to form one group
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, hiveConfig,
                HiveStorageFormat.ORC);

        for (int i = 0; i < 90; i++) {
            List<HostAddress> hostAddress = new ArrayList<>();
            hostAddress.add(new HostAddress("vm" + (i % 3), 1));
            hostAddress.add(new HostAddress("vm" + ((i + 1) % 3), 1));
            hiveSplitSource.addToQueue(new TestSplit(i, hostAddress));
        }

        // remove 1 split
        List<ConnectorSplit> connectorSplits = getSplits(hiveSplitSource, 100);
        List<ConnectorSplit> groupedConnectorSplits = hiveSplitSource.groupSmallSplits(connectorSplits, 1);
        assertEquals(groupedConnectorSplits.size(), 9);
        List<HiveSplitWrapper> hiveSplitWrappers = new ArrayList<>();
        groupedConnectorSplits.forEach(pendingSplit -> hiveSplitWrappers.add((HiveSplitWrapper) pendingSplit));
        for (int i = 0; i < 9; i++) {
            assertEquals(hiveSplitWrappers.get(i).getSplits().size(), 10);
        }
    }

    @Test
    public void testGroupSmallSplitConfigSetMaxSmallSplitsGrouped()
    {
        // testing setMaxSmallSplitsGrouped, need to 30 splits
        HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setMaxSplitsToGroup(30);
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, hiveConfig,
                HiveStorageFormat.ORC);

        for (int i = 0; i < 90; i++) {
            List<HostAddress> hostAddress = new ArrayList<>();
            hostAddress.add(new HostAddress("vm1", 1));
            hiveSplitSource.addToQueue(new TestSplit(i, hostAddress));
        }

        List<ConnectorSplit> connectorSplits = getSplits(hiveSplitSource, 100);
        List<ConnectorSplit> groupedConnectorSplits = hiveSplitSource.groupSmallSplits(connectorSplits, 1);
        assertEquals(groupedConnectorSplits.size(), 3);
        List<HiveSplitWrapper> hiveSplitWrappers = new ArrayList<>();
        groupedConnectorSplits.forEach(pendingSplit -> hiveSplitWrappers.add((HiveSplitWrapper) pendingSplit));
        for (int i = 0; i < 3; i++) {
            assertEquals(hiveSplitWrappers.get(i).getSplits().size(), 30);
        }
    }

    @Test
    public void testGroupSmallSplitBucket()
    {
        // test with 4 different bucket values
        HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setMaxSplitsToGroup(100);
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, hiveConfig,
                HiveStorageFormat.ORC);

        for (int i = 0; i < 100; i++) {
            List<HostAddress> hostAddress = new ArrayList<>();
            hostAddress.add(new HostAddress("vm1", 1));
            hiveSplitSource.addToQueue(new TestSplit(i, OptionalInt.of(i % 4), 100, hostAddress));
        }

        List<ConnectorSplit> connectorSplits = getSplits(hiveSplitSource, 100);
        List<ConnectorSplit> groupedConnectorSplits = hiveSplitSource.groupSmallSplits(connectorSplits, 1);
        assertEquals(groupedConnectorSplits.size(), 4);
        List<HiveSplitWrapper> hiveSplitWrappers = new ArrayList<>();
        groupedConnectorSplits.forEach(pendingSplit -> hiveSplitWrappers.add((HiveSplitWrapper) pendingSplit));
        for (int i = 0; i < 4; i++) {
            assertEquals(hiveSplitWrappers.get(i).getSplits().size(), 25);
        }
    }

    @Test
    public void testGroupSmallSplitAlternativeFileSize()
    {
        // alternative big and small size total 100 files
        HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setMaxSplitsToGroup(100);
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, hiveConfig,
                HiveStorageFormat.ORC);

        for (int i = 0; i < 100; i++) {
            List<HostAddress> hostAddress = new ArrayList<>();
            hostAddress.add(new HostAddress("vm1", 1));
            hiveSplitSource.addToQueue(new TestSplit(i, OptionalInt.empty(), 67108864 / (((i + 1) % 2) + 1), hostAddress));
        }

        List<ConnectorSplit> connectorSplits = getSplits(hiveSplitSource, 100);
        List<ConnectorSplit> groupedConnectorSplits = hiveSplitSource.groupSmallSplits(connectorSplits, 1);
        List<HiveSplitWrapper> hiveSplitWrappers = new ArrayList<>();
        groupedConnectorSplits.forEach(pendingSplit -> hiveSplitWrappers.add((HiveSplitWrapper) pendingSplit));
        System.out.println("hiveSplitWrappers.get(i).getSplits().size() " + groupedConnectorSplits.size());
        for (int i = 0; i < 50; i++) {
            assertEquals(hiveSplitWrappers.get(i).getSplits().size(), 1);
        }
        for (int i = 50; i < groupedConnectorSplits.size(); i++) {
            System.out.println(hiveSplitWrappers.get(i).getSplits().size());
            assertEquals(hiveSplitWrappers.get(i).getSplits().size(), 2);
        }
    }

    @Test
    public void testGroupSmallSplitAllBigSizeFiles()
    {
        // alternative big and small size total 100 files
        HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setMaxSplitsToGroup(100);
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, hiveConfig,
                HiveStorageFormat.ORC);

        for (int i = 0; i < 100; i++) {
            List<HostAddress> hostAddress = new ArrayList<>();
            hostAddress.add(new HostAddress("vm1", 1));
            hiveSplitSource.addToQueue(new TestSplit(i, OptionalInt.empty(), 67108864, hostAddress));
        }

        List<ConnectorSplit> connectorSplits = getSplits(hiveSplitSource, 100);
        List<ConnectorSplit> groupedConnectorSplits = hiveSplitSource.groupSmallSplits(connectorSplits, 1);
        assertEquals(groupedConnectorSplits.size(), 100);
        List<HiveSplitWrapper> hiveSplitWrappers = new ArrayList<>();
        groupedConnectorSplits.forEach(pendingSplit -> hiveSplitWrappers.add((HiveSplitWrapper) pendingSplit));
        System.out.println("hiveSplitWrappers.get(i).getSplits().size() " + groupedConnectorSplits.size());
        for (int i = 0; i < groupedConnectorSplits.size(); i++) {
            assertEquals(hiveSplitWrappers.get(i).getSplits().size(), 1);
        }
    }

    @Test
    public void testGroupSmallSplitDifferentFileSize()
    {
        // alternative big and small size total 100 files
        HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setMaxSplitsToGroup(100);
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, hiveConfig,
                HiveStorageFormat.ORC);

        List<HostAddress> hostAddress = new ArrayList<>();
        hostAddress.add(new HostAddress("vm1", 1));
        hiveSplitSource.addToQueue(new TestSplit(1, OptionalInt.empty(), 67108864 / 2, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(2, OptionalInt.empty(), 67108864 / 100, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(3, OptionalInt.empty(), 67108864 / 10, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(4, OptionalInt.empty(), 67108864 / 2, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(5, OptionalInt.empty(), 67108864 / 4, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(6, OptionalInt.empty(), 67108864 / 100, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(7, OptionalInt.empty(), 67108864 / 20, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(8, OptionalInt.empty(), 67108864 / 100, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(9, OptionalInt.empty(), 67108864 / 2, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(10, OptionalInt.empty(), 67108864 / 4, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(11, OptionalInt.empty(), 67108864 / 4, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(12, OptionalInt.empty(), 67108864 / 4, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(13, OptionalInt.empty(), 67108864 / 5, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(14, OptionalInt.empty(), 67108864 * 2, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(15, OptionalInt.empty(), 7000, hostAddress));
        hiveSplitSource.addToQueue(new TestSplit(16, OptionalInt.empty(), 20000, hostAddress));

        List<ConnectorSplit> connectorSplits = getSplits(hiveSplitSource, 100);
        List<ConnectorSplit> groupedConnectorSplits = hiveSplitSource.groupSmallSplits(connectorSplits, 1);
        List<HiveSplitWrapper> hiveSplitWrappers = new ArrayList<>();
        groupedConnectorSplits.forEach(pendingSplit -> hiveSplitWrappers.add((HiveSplitWrapper) pendingSplit));
        assertEquals(groupedConnectorSplits.size(), 6);
    }

    @Test
    public void testBucketedGroupSmallSplit()
    {
        HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setMaxSplitsToGroup(10);
        final HiveSplitSource hiveSplitSource = HiveSplitSource.bucketed(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, hiveConfig,
                HiveStorageFormat.ORC);
        for (int i = 0; i < 10; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i, OptionalInt.of(2)));
        }
        hiveSplitSource.noMoreSplits();
        List<ConnectorSplit> connectorSplits = getSplits(hiveSplitSource, OptionalInt.of(2), 100);
        List<ConnectorSplit> groupedConnectorSplits = hiveSplitSource.groupSmallSplits(connectorSplits, 1);
        assertEquals(groupedConnectorSplits.size(), 1);
        List<HiveSplitWrapper> hiveSplitWrappers = new ArrayList<>();
        groupedConnectorSplits.forEach(pendingSplit -> hiveSplitWrappers.add((HiveSplitWrapper) pendingSplit));
        assertEquals(hiveSplitWrappers.get(0).getSplits().size(), 10);
    }

    @Test
    public void testBucketedGroupSmallSplitDifferentBucket()
    {
        HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setMaxSplitsToGroup(10);
        final HiveSplitSource hiveSplitSource = HiveSplitSource.bucketed(
                HiveTestUtils.SESSION,
                "database",
                "table",
                10,
                10,
                new DataSize(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                null, null, hiveConfig,
                HiveStorageFormat.ORC);
        for (int i = 0; i < 100; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i, OptionalInt.of(i % 4)));
        }
        hiveSplitSource.noMoreSplits();

        for (int i = 0; i < 4; i++) {
            List<ConnectorSplit> connectorSplits = getSplits(hiveSplitSource, OptionalInt.of(i), 100);
            List<ConnectorSplit> groupedConnectorSplits = hiveSplitSource.groupSmallSplits(connectorSplits, 1);
            List<HiveSplitWrapper> hiveSplitWrappers = new ArrayList<>();
            groupedConnectorSplits.forEach(pendingSplit -> hiveSplitWrappers.add((HiveSplitWrapper) pendingSplit));
            assertEquals(hiveSplitWrappers.get(0).getSplits().size(), 10);
            assertEquals(hiveSplitWrappers.get(1).getSplits().size(), 10);
            assertEquals(hiveSplitWrappers.get(2).getSplits().size(), 5);
        }
    }
}
