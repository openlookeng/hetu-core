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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import io.airlift.node.NodeInfo;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.hetu.core.statestore.hazelcast.HazelcastStateStoreBootstrapper;
import io.hetu.core.statestore.hazelcast.HazelcastStateStoreFactory;
import io.prestosql.execution.TaskId;
import io.prestosql.operator.DynamicFilterSourceOperator.Channel;
import io.prestosql.operator.DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory;
import io.prestosql.seedstore.SeedStoreManager;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.HashSetDynamicFilter;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.seedstore.Seed;
import io.prestosql.spi.seedstore.SeedStore;
import io.prestosql.spi.seedstore.SeedStoreSubType;
import io.prestosql.spi.statestore.StateSet;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.statestore.StateStoreBootstrapper;
import io.prestosql.spi.statestore.StateStoreFactory;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.planner.LocalDynamicFilter;
import io.prestosql.statestore.LocalStateStoreProvider;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.utils.DynamicFilterUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Strings.repeat;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.hetu.core.statestore.hazelcast.HazelcastConstants.DISCOVERY_PORT_CONFIG_NAME;
import static io.prestosql.SequencePageBuilder.createSequencePage;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverSize;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverValueCount;
import static io.prestosql.block.BlockAssertions.createBooleansBlock;
import static io.prestosql.block.BlockAssertions.createDoublesBlock;
import static io.prestosql.block.BlockAssertions.createLongRepeatBlock;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.block.BlockAssertions.createSlicesBlock;
import static io.prestosql.block.BlockAssertions.createStringsBlock;
import static io.prestosql.operator.OperatorAssertion.toMaterializedResult;
import static io.prestosql.operator.OperatorAssertion.toPages;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.GLOBAL;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.LOCAL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.FeaturesConfig.DynamicFilterDataType.BLOOM_FILTER;
import static io.prestosql.sql.analyzer.FeaturesConfig.DynamicFilterDataType.HASHSET;
import static io.prestosql.statestore.StateStoreConstants.STATE_STORE_CONFIGURATION_PATH;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.utils.DynamicFilterUtils.PARTIALPREFIX;
import static io.prestosql.utils.DynamicFilterUtils.TASKSPREFIX;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDynamicFilterSourceOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private PipelineContext pipelineContext;
    private StateStoreProvider stateStoreProvider;

    private static Channel channel(int index, Type type, String inputFilterId)
    {
        String filterId = inputFilterId;
        String queryId = TEST_SESSION.getQueryId().toString();
        if (filterId.equals("0")) {
            filterId = String.valueOf(index);
        }
        return new Channel(filterId, type, index, queryId);
    }

    @BeforeTest
    private void prepareConfigFiles()
            throws Exception
    {
        File launcherConfigFile = new File(STATE_STORE_CONFIGURATION_PATH);
        if (launcherConfigFile.exists()) {
            launcherConfigFile.delete();
        }
        launcherConfigFile.createNewFile();
        FileWriter configWriter = new FileWriter(STATE_STORE_CONFIGURATION_PATH);
        configWriter.write("state-store.type=hazelcast\n" +
                "state-store.name=test\n" +
                "state-store.cluster=test-cluster\n" +
                "hazelcast.discovery.mode=tcp-ip\n" +
                "hazelcast.discovery.port=7980\n");
        configWriter.close();
        Set<Seed> seeds = new HashSet<>();
        SeedStore mockSeedStore = mock(SeedStore.class);
        Seed mockSeed = mock(Seed.class);
        seeds.add(mockSeed);

        SeedStoreManager mockSeedStoreManager = mock(SeedStoreManager.class);
        when(mockSeedStoreManager.getSeedStore(SeedStoreSubType.HAZELCAST)).thenReturn(mockSeedStore);

        when(mockSeed.getLocation()).thenReturn("127.0.0.1:6991");
        when(mockSeedStore.get()).thenReturn(seeds);

        StateStoreFactory factory = new HazelcastStateStoreFactory();
        stateStoreProvider = new LocalStateStoreProvider(mockSeedStoreManager);
        stateStoreProvider.addStateStoreFactory(factory);
        createStateStoreCluster("6991");
        stateStoreProvider.loadStateStore();
    }

    @AfterTest
    private void cleanUp()
    {
        File launcherConfigFile = new File(STATE_STORE_CONFIGURATION_PATH);
        if (launcherConfigFile.exists()) {
            launcherConfigFile.delete();
        }
    }

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        pipelineContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false);
    }

    private StateStore createStateStoreCluster(String port)
    {
        Map<String, String> config = new HashMap<>();
        config.put("hazelcast.discovery.mode", "tcp-ip");
        config.put("state-store.cluster", "test-cluster");
        config.put(DISCOVERY_PORT_CONFIG_NAME, port);

        StateStoreBootstrapper bootstrapper = new HazelcastStateStoreBootstrapper();
        return bootstrapper.bootstrap(ImmutableSet.of("127.0.0.1:" + port), config);
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    private void verifyPassthrough(Operator operator, List<Type> types, Page... pages)
    {
        List<Page> inputPages = ImmutableList.copyOf(pages);
        List<Page> outputPages = toPages(operator, inputPages.iterator());
        MaterializedResult actual = toMaterializedResult(pipelineContext.getSession(), types, outputPages);
        MaterializedResult expected = toMaterializedResult(pipelineContext.getSession(), types, inputPages);
        assertEquals(actual, expected);
    }

    private DynamicFilterSourceOperatorFactory createOperatorFactory(DynamicFilter.Type dfType, FeaturesConfig.DynamicFilterDataType dataType, int partitionCount, Channel... buildChannels)
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        Multimap<String, Symbol> probeSymbols = MultimapBuilder.treeKeys().arrayListValues().build();
        Map<String, Integer> buildChannelMap = new HashMap<>();
        Arrays.stream(buildChannels).map(channel -> buildChannelMap.put(channel.getFilterId(), channel.getIndex()));
        Arrays.stream(buildChannels).map(channel -> probeSymbols.put(channel.getFilterId(), new Symbol(String.valueOf(channel.getIndex()))));

        TaskId taskId = new TaskId("test0.0");
        LocalDynamicFilter localDynamicFilter = new LocalDynamicFilter(probeSymbols,
                buildChannelMap, partitionCount, dfType, dataType, 0.1D, taskId, stateStoreProvider);

        return new DynamicFilterSourceOperatorFactory(
                0,
                new PlanNodeId("PLAN_NODE_ID"),
                localDynamicFilter.getValueConsumer(),
                Arrays.stream(buildChannels).collect(toList()),
                getDynamicFilteringMaxPerDriverValueCount(TEST_SESSION),
                getDynamicFilteringMaxPerDriverSize(TEST_SESSION));
    }

    private DynamicFilterSourceOperator createOperator(DynamicFilterSourceOperatorFactory operatorFactory)
    {
        return operatorFactory.createOperator(pipelineContext.addDriverContext());
    }

    @Test
    public void testCollectMultipleOperators()
    {
        String filterId = "0";
        DynamicFilterSourceOperatorFactory operatorFactory = createOperatorFactory(LOCAL, HASHSET, 2, channel(0, BIGINT, filterId));

        DynamicFilterSourceOperator op1 = createOperator(operatorFactory); // will finish before noMoreOperators()
        verifyPassthrough(op1,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2)),
                new Page(createLongsBlock(3, 5)));

        Operator op2 = createOperator(operatorFactory); // will finish after noMoreOperators()
        operatorFactory.noMoreOperators();

        verifyPassthrough(op2,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(2, 3)),
                new Page(createLongsBlock(1, 4)));

        String key = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        Set<Long> set = new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L, 5L));
        StateSet states = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key));
        for (Object bfSerialized : states.getAll()) {
            assertEquals(set, (Set) bfSerialized);
        }
    }

    @Test
    public void testGlobalDynamicFilterSourceOperatorBloomFilter()
    {
        String filterId = "99";
        DynamicFilterSourceOperatorFactory operatorFactory = createOperatorFactory
                (GLOBAL, BLOOM_FILTER, 1, channel(0, BIGINT, filterId));

        DynamicFilterSourceOperator op1 = createOperator(operatorFactory); // will finish before noMoreOperators()

        verifyPassthrough(op1,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2)),
                new Page(createLongsBlock(99, 101)),
                new Page(createLongsBlock(3, 5)));

        String key = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        StateSet states = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key));
        for (Object bfSerialized : states.getAll()) {
            BloomFilterDynamicFilter bfdf = new BloomFilterDynamicFilter(filterId, null, (byte[]) bfSerialized, GLOBAL);
            assertTrue(bfdf.contains(101L));
            assertEquals(bfdf.getSize(), 6);
        }
        assertEquals((stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils
                .createKey(TASKSPREFIX, filterId, TEST_SESSION.getQueryId().toString()))).size(), 1);
    }

    @Test
    public void testGlobalDynamicFilterSourceOperatorBloomFilterSlice()
    {
        String filterId = "909";
        DynamicFilterSourceOperatorFactory operatorFactory = createOperatorFactory
                (GLOBAL, BLOOM_FILTER, 1, channel(0, VARCHAR, filterId));

        DynamicFilterSourceOperator op1 = createOperator(operatorFactory); // will finish before noMoreOperators()

        verifyPassthrough(op1,
                ImmutableList.of(VARCHAR),
                new Page(createSlicesBlock(utf8Slice("test1"))),
                new Page(createSlicesBlock(utf8Slice("test2"))),
                new Page(createSlicesBlock(utf8Slice("test3"))));

        String key = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        StateSet states = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key));
        for (Object bfSerialized : states.getAll()) {
            BloomFilterDynamicFilter bfdf = new BloomFilterDynamicFilter(filterId, null, (byte[]) bfSerialized, GLOBAL);
            Slice slice = Slices.utf8Slice("test1");
            assertEquals(bfdf.getSize(), 3);
            assertTrue(bfdf.contains(slice));
        }
        assertEquals(stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils
                .createKey(TASKSPREFIX, filterId, TEST_SESSION.getQueryId().toString())).size(), 1);
    }

    @Test
    public void testGlobalDynamicFilterSourceOperatorHashSet()
    {
        String filterId = "22";
        DynamicFilterSourceOperatorFactory operatorFactory = createOperatorFactory
                (GLOBAL, HASHSET, 1, channel(0, BIGINT, filterId));

        DynamicFilterSourceOperator op1 = createOperator(operatorFactory); // will finish before noMoreOperators()

        verifyPassthrough(op1,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2)),
                new Page(createLongsBlock(12, 21)),
                new Page(createLongsBlock(13, 22)),
                new Page(createLongsBlock(3, 5)));

        String key = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());

        StateSet states = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key));
        for (Object bfSerialized : states.getAll()) {
            HashSetDynamicFilter bfdf = new HashSetDynamicFilter(filterId, null, (Set) bfSerialized, GLOBAL);
            assertTrue(bfdf.contains(22L));
            assertEquals(bfdf.getSize(), 8);
        }
        assertEquals(stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils
                .createKey(TASKSPREFIX, filterId, TEST_SESSION.getQueryId().toString())).size(), 1);
    }

    @Test
    public void testCollectMultipleColumns()
    {
        String filterId1 = "multiple_columns_1";
        String filterId2 = "multiple_columns_2";
        OperatorFactory operatorFactory = createOperatorFactory(LOCAL, HASHSET, 1, channel(0, BOOLEAN, filterId1), channel(1, DOUBLE, filterId2));
        verifyPassthrough(createOperator((DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        String key1 = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId1, TEST_SESSION.getQueryId().toString());
        Set<Boolean> set1 = new HashSet<>(Arrays.asList(true, false));
        StateSet states1 = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key1));
        for (Object bfSerialized : states1.getAll()) {
            assertEquals((Set) bfSerialized, set1);
        }

        String key2 = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId2, TEST_SESSION.getQueryId().toString());
        Set<Double> set2 = new HashSet<>(Arrays.asList(1.5, 3.0, 4.5));
        StateSet states2 = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key2));
        for (Object bfSerialized : states2.getAll()) {
            assertEquals((Set) bfSerialized, set2);
        }
    }

    @Test
    public void testCollectOnlyFirstColumn()
    {
        String filterId = "first_column";
        OperatorFactory operatorFactory = createOperatorFactory(LOCAL, HASHSET, 1, channel(0, BOOLEAN, filterId));
        verifyPassthrough(createOperator((DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        String key = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        Set<Boolean> set = new HashSet<>(Arrays.asList(true, false));
        StateSet states = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key));
        for (Object bfSerialized : states.getAll()) {
            assertEquals((Set) bfSerialized, set);
        }
    }

    @Test
    public void testCollectOnlyLastColumn()
    {
        String filterId = "last_column";
        OperatorFactory operatorFactory = createOperatorFactory(LOCAL, HASHSET, 1, channel(1, DOUBLE, filterId));
        verifyPassthrough(createOperator((DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        String key = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        Set<Double> set = new HashSet<>(Arrays.asList(1.5, 3.0, 4.5));
        StateSet states = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key));
        for (Object bfSerialized : states.getAll()) {
            assertEquals((Set) bfSerialized, set);
        }
    }

    @Test
    public void testCollectWithNulls()
    {
        Block blockWithNulls = INTEGER
                .createFixedSizeBlockBuilder(0)
                .writeInt(3)
                .appendNull()
                .writeInt(4)
                .build();
        String filterId = "with_nulls";
        OperatorFactory operatorFactory = createOperatorFactory(LOCAL, HASHSET, 1, channel(0, INTEGER, filterId));
        verifyPassthrough(createOperator((DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(INTEGER),
                new Page(createLongsBlock(1, 2, 3)),
                new Page(blockWithNulls),
                new Page(createLongsBlock(4, 5)));
        operatorFactory.noMoreOperators();

        String key = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        Set<Long> set = new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L, 5L));
        StateSet states = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key));
        for (Object bfSerialized : states.getAll()) {
            assertEquals((Set) bfSerialized, set);
        }
    }

    @Test
    private void testCollectNoFilters()
    {
        OperatorFactory operatorFactory = createOperatorFactory(LOCAL, HASHSET, 1);
        verifyPassthrough(createOperator((DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2, 3)));
        operatorFactory.noMoreOperators();
    }

    @Test
    public void testCollectEmptyBuildSide()
    {
        String filterId = "empty_build_side";
        OperatorFactory operatorFactory = createOperatorFactory(LOCAL, HASHSET, 1, channel(0, BIGINT, filterId));
        verifyPassthrough(createOperator((DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BIGINT));
        operatorFactory.noMoreOperators();

        String key = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        StateSet states = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key));
        for (Object bfSerialized : states.getAll()) {
            assertEquals((Set) bfSerialized, new HashSet()); // should be empty
        }
    }

    @Test
    public void testCollectTooMuchRows()
    {
        String filterId = "too_much_rows";
        final int maxRowCount = getDynamicFilteringMaxPerDriverValueCount(pipelineContext.getSession());
        Page largePage = createSequencePage(ImmutableList.of(BIGINT), maxRowCount + 1);

        OperatorFactory operatorFactory = createOperatorFactory(LOCAL, HASHSET, 1, channel(0, BIGINT, filterId));
        verifyPassthrough(createOperator((DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BIGINT),
                largePage);
        operatorFactory.noMoreOperators();

        String key = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        assertNull(stateStoreProvider.getStateStore().getStateCollection(key));
    }

    @Test
    public void testCollectTooMuchBytesSingleColumn()
    {
        String filterId = "too_much_bytes_single_column";
        final long maxByteSize = getDynamicFilteringMaxPerDriverSize(pipelineContext.getSession()).toBytes();
        Page largePage = new Page(createStringsBlock(repeat("A", (int) maxByteSize + 1)));

        OperatorFactory operatorFactory = createOperatorFactory(LOCAL, HASHSET, 1, channel(0, VARCHAR, filterId));
        verifyPassthrough(createOperator((DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(VARCHAR),
                largePage);
        operatorFactory.noMoreOperators();
        String key = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        assertNull(stateStoreProvider.getStateStore().getStateCollection(key));
    }

    @Test
    public void testCollectTooMuchBytesMultipleColumns()
    {
        String filterId1 = "too_much_bytes_multiple_columns_1";
        String filterId2 = "too_much_bytes_multiple_columns_2";
        final long maxByteSize = getDynamicFilteringMaxPerDriverSize(pipelineContext.getSession()).toBytes();
        Page largePage = new Page(createStringsBlock(repeat("A", (int) (maxByteSize / 2) + 1)),
                createStringsBlock(repeat("B", (int) (maxByteSize / 2) + 1)));

        OperatorFactory operatorFactory = createOperatorFactory(LOCAL, HASHSET, 1, channel(0, VARCHAR, filterId1),
                channel(1, VARCHAR, filterId2));
        verifyPassthrough(createOperator((DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(VARCHAR, VARCHAR),
                largePage);
        operatorFactory.noMoreOperators();

        String key1 = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId1, TEST_SESSION.getQueryId().toString());
        assertNull(stateStoreProvider.getStateStore().getStateCollection(key1));

        String key2 = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId2, TEST_SESSION.getQueryId().toString());
        assertNull(stateStoreProvider.getStateStore().getStateCollection(key2));
    }

    @Test
    public void testCollectDeduplication()
    {
        String filterId = "deduplication";
        final int maxRowCount = getDynamicFilteringMaxPerDriverValueCount(pipelineContext.getSession());
        Page largePage = new Page(createLongRepeatBlock(7, maxRowCount * 10)); // lots of zeros
        Page nullsPage = new Page(createLongsBlock(Arrays.asList(new Long[maxRowCount * 10]))); // lots of nulls

        OperatorFactory operatorFactory = createOperatorFactory(LOCAL, BLOOM_FILTER, 1, channel(0, BIGINT, filterId));
        verifyPassthrough(createOperator((DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BIGINT),
                largePage, nullsPage);
        operatorFactory.noMoreOperators();

        String key = DynamicFilterUtils.createKey(PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        Set<Long> set = new HashSet<>();
        set.add(7L);
        StateSet states = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key));
        for (Object bfSerialized : states.getAll()) {
            assertEquals((Set) bfSerialized, set);
        }
    }
}
