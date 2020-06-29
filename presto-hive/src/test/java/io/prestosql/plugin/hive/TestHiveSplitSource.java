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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.Assertions.assertContains;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.plugin.hive.HiveTestUtils.createTestDynamicFilterSupplier;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.spi.type.BigintType.BIGINT;
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
                null);

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
                null);

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
                null);

        final SettableFuture<ConnectorSplit> splits = SettableFuture.create();

        // create a thread that will get a split
        final CountDownLatch started = new CountDownLatch(1);
        Thread getterThread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    started.countDown();
                    List<ConnectorSplit> batch = getSplits(hiveSplitSource, 1);
                    assertEquals(batch.size(), 1);
                    splits.set(batch.get(0));
                }
                catch (Throwable e) {
                    splits.setException(e);
                }
            }
        });
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
                null);
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
                null);
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
                createTestDynamicFilterSupplier("pt_d", ImmutableList.of("0")),
                null);

        for (int i = 0; i < 5; i++) {
            hiveSplitSource.addToQueue(new TestPartitionSplit(2 * i, ImmutableList.of(new HivePartitionKey("pt_d", "0")), "pt_d=0"));
            hiveSplitSource.addToQueue(new TestPartitionSplit(2 * i + 1, ImmutableList.of(new HivePartitionKey("pt_d", "1")), "pt_d=1"));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 2 * i + 2);
        }

        assertEquals(getSplits(hiveSplitSource, 10).size(), 5);
    }

    @Test
    public void testSplitCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", BIGINT);
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(BIGINT, 20200522L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(BIGINT, 20200521L))));
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
                null,
                cachePredicates);

        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "20200520", 2, "20200521", 3, "20200522", 2)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });

        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 8);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 5);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "20200521"))).count(), 3);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "20200522"))).count(), 2);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "20200520"))).count(), 0);
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
        private TestSplit(int id)
        {
            this(id, OptionalInt.empty());
        }

        private TestSplit(int id, OptionalInt bucketNumber)
        {
            super(
                    "partition-name",
                    "path",
                    0,
                    100,
                    100,
                    0,
                    properties("id", String.valueOf(id)),
                    ImmutableList.of(),
                    ImmutableList.of(new InternalHiveBlock(0, 100, ImmutableList.of())),
                    bucketNumber,
                    true,
                    false,
                    ImmutableMap.of(),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    Optional.empty());
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
                    Optional.empty());
        }

        private static Properties properties(String key, String value)
        {
            Properties properties = new Properties();
            properties.put(key, value);
            return properties;
        }
    }
}
