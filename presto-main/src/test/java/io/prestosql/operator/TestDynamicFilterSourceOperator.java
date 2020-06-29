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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.node.NodeInfo;
import io.hetu.core.statestore.hazelcast.HazelcastStateStoreBootstrapper;
import io.hetu.core.statestore.hazelcast.HazelcastStateStoreFactory;
import io.prestosql.seedstore.SeedStoreManager;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.HashSetDynamicFilter;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.seedstore.Seed;
import io.prestosql.spi.seedstore.SeedStore;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateSet;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.statestore.StateStoreBootstrapper;
import io.prestosql.spi.statestore.StateStoreFactory;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.statestore.LocalStateStoreProvider;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.utils.DynamicFilterUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
import static io.prestosql.SequencePageBuilder.createSequencePage;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverSize;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverValueCount;
import static io.prestosql.block.BlockAssertions.createBooleansBlock;
import static io.prestosql.block.BlockAssertions.createDoublesBlock;
import static io.prestosql.block.BlockAssertions.createLongRepeatBlock;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.block.BlockAssertions.createStringsBlock;
import static io.prestosql.operator.OperatorAssertion.toMaterializedResult;
import static io.prestosql.operator.OperatorAssertion.toPages;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.statestore.StateStoreConstants.STATE_STORE_CONFIGURATION_PATH;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test(singleThreaded = true)
public class TestDynamicFilterSourceOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private PipelineContext pipelineContext;
    private StateStoreProvider stateStoreProvider;
    private StateStoreFactory factory;

    private ImmutableList.Builder<TupleDomain<String>> partitions;

    @BeforeTest
    private void prepareConfigFiles()
            throws Exception
    {
        File launcherConfigFile = new File(STATE_STORE_CONFIGURATION_PATH);
        if (!launcherConfigFile.exists()) {
            launcherConfigFile.createNewFile();
        }
        else {
            launcherConfigFile.delete();
            launcherConfigFile.createNewFile();
        }
        FileWriter configWriter = new FileWriter(STATE_STORE_CONFIGURATION_PATH);
        configWriter.write("state-store.type=hazelcast\n" +
                "state-store.name=test\n" +
                "state-store.cluster=test-cluster\n" +
                "hazelcast.discovery.mode=tcp-ip\n" +
                "hazelcast.tcp-ip.port=7980\n");
        configWriter.close();
        Set<Seed> seeds = new HashSet<>();
        SeedStore mockSeedStore = mock(SeedStore.class);
        Seed mockSeed = mock(Seed.class);
        seeds.add(mockSeed);

        SeedStoreManager mockSeedStoreManager = mock(SeedStoreManager.class);
        when(mockSeedStoreManager.getSeedStore()).thenReturn(mockSeedStore);

        when(mockSeed.getLocation()).thenReturn("127.0.0.1:6991");
        when(mockSeedStore.get()).thenReturn(seeds);

        factory = new HazelcastStateStoreFactory();
        stateStoreProvider = new LocalStateStoreProvider(mockSeedStoreManager);
        stateStoreProvider.addStateStoreFactory(factory);
        createStateStoreCluster("6991");
        stateStoreProvider.loadStateStore();
    }

    @BeforeMethod
    public void setUp() throws Exception
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        pipelineContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false);

        partitions = ImmutableList.builder();
    }

    private StateStore createStateStoreCluster(String port)
    {
        Map<String, String> config = new HashMap<>();
        config.put("hazelcast.discovery.mode", "tcp-ip");
        config.put("state-store.cluster", "test-cluster");
        config.put("hazelcast.tcp-ip.port", port);

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

    private DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory createOperatorFactory(DynamicFilter.Type dfType, int dataStructure, DynamicFilterSourceOperator.Channel... buildChannels)
    {
        NodeInfo nodeInfo = new NodeInfo("test");

        return new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                0,
                new PlanNodeId("PLAN_NODE_ID"),
                this::consumePredicate,
                Arrays.stream(buildChannels).collect(toList()),
                getDynamicFilteringMaxPerDriverValueCount(TEST_SESSION),
                getDynamicFilteringMaxPerDriverSize(TEST_SESSION),
                dataStructure,
                dfType,
                nodeInfo, stateStoreProvider);
    }

    private void consumePredicate(TupleDomain<String> partitionPredicate)
    {
        partitions.add(partitionPredicate);
    }

    private DynamicFilterSourceOperator createOperator(DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory operatorFactory)
    {
        return (DynamicFilterSourceOperator) operatorFactory.createOperator(pipelineContext.addDriverContext());
    }

    private static DynamicFilterSourceOperator.Channel channel(int index, Type type, String filterId)
    {
        String queryId = TEST_SESSION.getQueryId().toString();
        if (filterId.equals("0")) {
            filterId = String.valueOf(index);
        }
        return new DynamicFilterSourceOperator.Channel(filterId, type, index, queryId);
    }

    @Test
    public void testCollectMultipleOperators()
    {
        DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory operatorFactory = createOperatorFactory(DynamicFilter.Type.LOCAL, 0, channel(0, BIGINT, "0"));

        DynamicFilterSourceOperator op1 = createOperator(operatorFactory); // will finish before noMoreOperators()
        verifyPassthrough(op1,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2)),
                new Page(createLongsBlock(3, 5)));

        Operator op2 = createOperator(operatorFactory); // will finish after noMoreOperators()
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 5L))))));

        verifyPassthrough(op2,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(2, 3)),
                new Page(createLongsBlock(1, 4)));

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 5L)))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 4L))))));
    }

    @Test
    public void testGlobalDynamicFilterSourceOperatorBloomFilter() throws IOException
    {
        String filterId = "99";
        DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory operatorFactory = createOperatorFactory
                (DynamicFilter.Type.GLOBAL, 0, channel(0, BIGINT, filterId));

        DynamicFilterSourceOperator op1 = createOperator(operatorFactory); // will finish before noMoreOperators()

        verifyPassthrough(op1,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2)),
                new Page(createLongsBlock(99, 101)),
                new Page(createLongsBlock(3, 5)));

        String key = DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        String typeKey = DynamicFilterUtils.createKey(DynamicFilterUtils.TYPEPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        String resultType = (String) ((StateMap) stateStoreProvider.getStateStore()
                .getStateCollection(DynamicFilterUtils.DFTYPEMAP)).get(typeKey);
        StateSet states = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key));
        for (Object bfSerialized : states.getAll()) {
            BloomFilterDynamicFilter bfdf = new BloomFilterDynamicFilter(filterId, null, (byte[]) bfSerialized, DynamicFilter.Type.GLOBAL);
            assertEquals(bfdf.contains("101"), true);
            assertEquals(bfdf.getSize(), 6);
        }
        assertEquals(resultType, DynamicFilterUtils.BLOOMFILTERTYPEGLOBAL);
        assertEquals(((StateSet) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils
                .createKey(DynamicFilterUtils.FINISHREFIX, filterId, TEST_SESSION.getQueryId().toString()))).size(), 1);
        assertEquals(((StateSet) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils
                .createKey(DynamicFilterUtils.WORKERSPREFIX, filterId, TEST_SESSION.getQueryId().toString()))).size(), 1);
    }

    @Test
    public void testGlobalDynamicFilterSourceOperatorHashSet() throws IOException
    {
        String filterId = "22";
        DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory operatorFactory = createOperatorFactory
                (DynamicFilter.Type.GLOBAL, 1, channel(0, BIGINT, filterId));

        DynamicFilterSourceOperator op1 = createOperator(operatorFactory); // will finish before noMoreOperators()

        verifyPassthrough(op1,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2)),
                new Page(createLongsBlock(12, 21)),
                new Page(createLongsBlock(13, 22)),
                new Page(createLongsBlock(3, 5)));

        String key = DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        String typeKey = DynamicFilterUtils.createKey(DynamicFilterUtils.TYPEPREFIX, filterId, TEST_SESSION.getQueryId().toString());
        String resultType = (String) ((StateMap) stateStoreProvider.getStateStore()
                .getStateCollection(DynamicFilterUtils.DFTYPEMAP)).get(typeKey);
        StateSet states = ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key));
        for (Object bfSerialized : states.getAll()) {
            HashSetDynamicFilter bfdf = new HashSetDynamicFilter(filterId, null, (Set) bfSerialized, DynamicFilter.Type.GLOBAL);
            assertEquals(bfdf.contains("22"), true);
            assertEquals(bfdf.getSize(), 8);
        }
        assertEquals(resultType, DynamicFilterUtils.HASHSETTYPEGLOBAL);
        assertEquals(((StateSet) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils
                .createKey(DynamicFilterUtils.FINISHREFIX, filterId, TEST_SESSION.getQueryId().toString()))).size(), 1);
        assertEquals(((StateSet) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils
                .createKey(DynamicFilterUtils.WORKERSPREFIX, filterId, TEST_SESSION.getQueryId().toString()))).size(), 1);
    }

    @Test
    public void testCollectMultipleColumns()
    {
        OperatorFactory operatorFactory = createOperatorFactory(DynamicFilter.Type.LOCAL, 0, channel(0, BOOLEAN, "0"), channel(1, DOUBLE, "0"));
        verifyPassthrough(createOperator((DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(BOOLEAN, ImmutableList.of(true, false)),
                        "1", Domain.multipleValues(DOUBLE, ImmutableList.of(1.5, 3.0, 4.5))))));
    }

    @Test
    public void testCollectOnlyFirstColumn()
    {
        OperatorFactory operatorFactory = createOperatorFactory(DynamicFilter.Type.LOCAL, 0, channel(0, BOOLEAN, "0"));
        verifyPassthrough(createOperator((DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(BOOLEAN, ImmutableList.of(true, false))))));
    }

    @Test
    public void testCollectOnlyLastColumn()
    {
        OperatorFactory operatorFactory = createOperatorFactory(DynamicFilter.Type.LOCAL, 0, channel(1, DOUBLE, "0"));
        verifyPassthrough(createOperator((DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "1", Domain.multipleValues(DOUBLE, ImmutableList.of(1.5, 3.0, 4.5))))));
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

        OperatorFactory operatorFactory = createOperatorFactory(DynamicFilter.Type.LOCAL, 0, channel(0, INTEGER, "0"));
        verifyPassthrough(createOperator((DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(INTEGER),
                new Page(createLongsBlock(1, 2, 3)),
                new Page(blockWithNulls),
                new Page(createLongsBlock(4, 5)));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.create(ValueSet.of(INTEGER, 1L, 2L, 3L, 4L, 5L), false)))));
    }

    @Test
    public void testCollectNoFilters()
    {
        OperatorFactory operatorFactory = createOperatorFactory(DynamicFilter.Type.LOCAL, 0);
        verifyPassthrough(createOperator((DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2, 3)));
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectEmptyBuildSide()
    {
        OperatorFactory operatorFactory = createOperatorFactory(DynamicFilter.Type.LOCAL, 0, channel(0, BIGINT, "0"));
        verifyPassthrough(createOperator((DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BIGINT));
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.none()));
    }

    @Test
    public void testCollectTooMuchRows()
    {
        final int maxRowCount = getDynamicFilteringMaxPerDriverValueCount(pipelineContext.getSession());
        Page largePage = createSequencePage(ImmutableList.of(BIGINT), maxRowCount + 1);

        OperatorFactory operatorFactory = createOperatorFactory(DynamicFilter.Type.LOCAL, 0, channel(0, BIGINT, "0"));
        verifyPassthrough(createOperator((DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BIGINT),
                largePage);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectTooMuchBytesSingleColumn()
    {
        final long maxByteSize = getDynamicFilteringMaxPerDriverSize(pipelineContext.getSession()).toBytes();
        Page largePage = new Page(createStringsBlock(repeat("A", (int) maxByteSize + 1)));

        OperatorFactory operatorFactory = createOperatorFactory(DynamicFilter.Type.LOCAL, 0, channel(0, VARCHAR, "0"));
        verifyPassthrough(createOperator((DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(VARCHAR),
                largePage);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectTooMuchBytesMultipleColumns()
    {
        final long maxByteSize = getDynamicFilteringMaxPerDriverSize(pipelineContext.getSession()).toBytes();
        Page largePage = new Page(createStringsBlock(repeat("A", (int) (maxByteSize / 2) + 1)),
                createStringsBlock(repeat("B", (int) (maxByteSize / 2) + 1)));

        OperatorFactory operatorFactory = createOperatorFactory(DynamicFilter.Type.LOCAL, 0, channel(0, VARCHAR, "0"),
                channel(1, VARCHAR, "0"));
        verifyPassthrough(createOperator((DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(VARCHAR, VARCHAR),
                largePage);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectDeduplication()
    {
        final int maxRowCount = getDynamicFilteringMaxPerDriverValueCount(pipelineContext.getSession());
        Page largePage = new Page(createLongRepeatBlock(7, maxRowCount * 10)); // lots of zeros
        Page nullsPage = new Page(createLongsBlock(Arrays.asList(new Long[maxRowCount * 10]))); // lots of nulls

        OperatorFactory operatorFactory = createOperatorFactory(DynamicFilter.Type.LOCAL, 0, channel(0, BIGINT, "0"));
        verifyPassthrough(createOperator((DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory) operatorFactory),
                ImmutableList.of(BIGINT),
                largePage, nullsPage);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.create(ValueSet.of(BIGINT, 7L), false)))));
    }
}
