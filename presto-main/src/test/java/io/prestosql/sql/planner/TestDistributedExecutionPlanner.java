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
package io.prestosql.sql.planner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.prestosql.Session;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.metadata.TableProperties;
import io.prestosql.security.AccessControl;
import io.prestosql.snapshot.MarkerSplitSource;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.exchange.RetryPolicy;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.split.SplitManager;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static io.prestosql.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.prestosql.spi.plan.AggregationNode.AggregationType.HASH;
import static io.prestosql.spi.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.DistributedExecutionPlanner.Mode.RESUME;
import static io.prestosql.sql.planner.DistributedExecutionPlanner.Mode.SNAPSHOT;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.prestosql.testing.TestingHandles.TEST_TABLE_HANDLE;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDistributedExecutionPlanner
{
    private static final Symbol symbol = new Symbol("symbol");

    private final Session session;
    private final DistributedExecutionPlanner planner;
    private final Map<MarkerSplitSource, String> sources = new HashMap<>();
    private final Multimap<String, String> dependencies = HashMultimap.create();
    private final Multimap<String, String> unions = HashMultimap.create();
    private int nodeId;

    public TestDistributedExecutionPlanner()
    {
        PropertyService.setProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED, false);
        session = TEST_SNAPSHOT_SESSION.beginTransactionId(TransactionId.create(), mock(TransactionManager.class), mock(AccessControl.class));
        Metadata metadata = mock(Metadata.class);
        doReturn(new TableMetadata(new CatalogName("catalog"), new ConnectorTableMetadata(new SchemaTableName("schemaName", "tableName"), ImmutableList.of()))).when(metadata).getTableMetadata(anyObject(), anyObject());
        doReturn(new TableProperties(new CatalogName("catalog"), mock(ConnectorTransactionHandle.class), new ConnectorTableProperties())).when(metadata).getTableProperties(anyObject(), anyObject());

        SplitManager splitManager = mock(SplitManager.class);
        doAnswer(invocation -> {
            PlanNodeId id = invocation.getArgumentAt(8, PlanNodeId.class);
            MarkerSplitSource source = mock(MarkerSplitSource.class);
            doAnswer(invocation1 -> {
                dependencies.put(id.toString(), sources.get(invocation1.getArgumentAt(0, MarkerSplitSource.class)));
                return null;
            }).when(source).addDependency(anyObject());
            doAnswer(invocation1 -> {
                List<String> list = (List<String>) invocation1.getArgumentAt(0, List.class).stream().map(sources::get).collect(Collectors.toList());
                unions.putAll(id.toString(), list);
                return null;
            }).when(source).addUnionSources(anyObject());
            sources.put(source, id.toString());
            return source;
        }).when(splitManager).getSplits(anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyBoolean(), anyObject());
        planner = new DistributedExecutionPlanner(splitManager, metadata);
    }

    @BeforeMethod
    public void setUp()
    {
        sources.clear();
        dependencies.clear();
        unions.clear();
        nodeId = 0;
    }

    @Test
    public void testSimple()
    {
        SubPlan root = makePlan(1,
                join(source("A"), source("B")),
                ImmutableList.of());
        test(root,
                ImmutableMultimap.of("A", "B"),
                ImmutableMultimap.of("B", "A"));
    }

    @Test
    public void testRemote()
    {
        SubPlan stage2 = makePlan(2,
                source("B"),
                ImmutableList.of());
        SubPlan stage3 = makePlan(3,
                source("C"),
                ImmutableList.of());

        SubPlan root = makePlan(1,
                join(source("A"), remote(2, 3)),
                ImmutableList.of(stage2, stage3));
        test(root,
                ImmutableMultimap.of(
                        "A", "B",
                        "A", "C"),
                ImmutableMultimap.of(
                        "B", "A",
                        "C", "A",
                        "B", "C",
                        "C", "B"));
    }

    @Test
    public void testJoins()
    {
        SubPlan stage3 = makePlan(3,
                source("D"),
                ImmutableList.of());
        SubPlan stage2 = makePlan(2,
                semiJoin(source("B"), spatialJoin(source("C"), remote(3))),
                ImmutableList.of(stage3));

        SubPlan root = makePlan(1,
                join(source("A"), remote(2)),
                ImmutableList.of(stage2));
        test(root,
                ImmutableMultimap.of(
                        "A", "B",
                        "B", "C",
                        "C", "D"),
                ImmutableMultimap.of(
                        "B", "A",
                        "C", "B",
                        "D", "C"));
    }

    @Test
    public void testLeftSameFragment()
    {
        SubPlan root = makePlan(1,
                join(join(source("A"), source("B")), join(source("C"), source("D"))),
                ImmutableList.of());
        test(root,
                ImmutableMultimap.of(
                        "A", "B",
                        "A", "C",
                        "C", "D"),
                ImmutableMultimap.of(
                        "B", "A",
                        "D", "C",
                        "B", "C",
                        "B", "D"));
    }

    @Test
    public void testLeftSameFragmentRemote()
    {
        SubPlan stage2 = makePlan(2,
                source("C"),
                ImmutableList.of());
        SubPlan root = makePlan(1,
                join(join(source("A"), source("B")), remote(2)),
                ImmutableList.of(stage2));
        test(root,
                ImmutableMultimap.of(
                        "A", "B",
                        "A", "C"),
                ImmutableMultimap.of(
                        "B", "A",
                        "B", "C"));
    }

    @Test
    public void testRemoteLeft()
    {
        SubPlan stage2 = makePlan(2,
                source("A"),
                ImmutableList.of());
        SubPlan stage3 = makePlan(3,
                source("B"),
                ImmutableList.of());
        SubPlan root = makePlan(1,
                join(join(remote(2, 3), source("C")), source("D")),
                ImmutableList.of(stage2, stage3));
        test(root,
                ImmutableMultimap.of(
                        "A", "C",
                        "A", "D",
                        "B", "C",
                        "B", "D"),
                ImmutableMultimap.of(
                        "C", "D",
                        "D", "C"));
    }

    @Test
    public void testRemoteBoth()
    {
        SubPlan stage2 = makePlan(2,
                source("A"),
                ImmutableList.of());
        SubPlan stage3 = makePlan(3,
                source("B"),
                ImmutableList.of());
        SubPlan stage4 = makePlan(4,
                source("C"),
                ImmutableList.of());
        SubPlan stage5 = makePlan(5,
                source("D"),
                ImmutableList.of());
        SubPlan root = makePlan(1,
                join(remote(2, 3), remote(4, 5)),
                ImmutableList.of(stage2, stage3, stage4, stage5));
        test(root,
                ImmutableMultimap.of(
                        "A", "C",
                        "A", "D",
                        "B", "C",
                        "B", "D"),
                ImmutableMultimap.of());
    }

    private void test(SubPlan root, Multimap<String, String> must, Multimap<String, String> mustNot)
    {
        planner.plan(root, session, SNAPSHOT, null, 0);
        Multimap<String, String> missing = Multimaps.filterEntries(must, e -> !dependencies.containsEntry(e.getKey(), e.getValue()));
        assertTrue(missing.isEmpty(), "Missing dependency: " + missing);
        Multimap<String, String> wrong = Multimaps.filterEntries(mustNot, e -> dependencies.containsEntry(e.getKey(), e.getValue()));
        assertTrue(wrong.isEmpty(), "Wrong dependency: " + wrong);
    }

    private void testExact(SubPlan root, Multimap<String, String> expected)
    {
        planner.plan(root, session, SNAPSHOT, null, 0);
        Multimap<String, String> missing = Multimaps.filterEntries(expected, e -> !dependencies.containsEntry(e.getKey(), e.getValue()));
        assertTrue(missing.isEmpty(), "Missing dependency: " + missing);
        Multimap<String, String> wrong = Multimaps.filterEntries(dependencies, e -> !expected.containsEntry(e.getKey(), e.getValue()));
        assertTrue(wrong.isEmpty(), "Wrong dependency: " + wrong);
    }

    @Test
    public void testExchangeUnion()
    {
        SubPlan stage2 = makePlan(2,
                source("A"),
                ImmutableList.of());
        SubPlan stage3 = makePlan(3,
                source("B"),
                ImmutableList.of());
        SubPlan stage4 = makePlan(4,
                source("C"),
                ImmutableList.of());
        SubPlan stage5 = makePlan(5,
                source("D"),
                ImmutableList.of());
        SubPlan root = makePlan(1,
                union(remote(2, 3), remote(4, 5)),
                ImmutableList.of(stage2, stage3, stage4, stage5));

        planner.plan(root, session, SNAPSHOT, null, 0);
        Collection<String> expected = ImmutableList.of("A", "B", "C", "D");
        assertEquals(unions.get("A"), expected);
        assertEquals(unions.get("B"), expected);
        assertEquals(unions.get("C"), expected);
        assertEquals(unions.get("D"), expected);
    }

    @Test
    public void testValuesSnapshot()
    {
        ValuesNode a = values("A");
        ValuesNode b = values("B");
        assertNull(a.getResumeSnapshotId());
        assertEquals(a.getNextSnapshotId(), 0);

        SubPlan root = makePlan(1,
                join(a, b),
                ImmutableList.of());
        planner.plan(root, session, SNAPSHOT, null, 7);

        assertNull(a.getResumeSnapshotId());
        assertEquals(a.getNextSnapshotId(), 7);
        assertNull(b.getResumeSnapshotId());
        assertEquals(b.getNextSnapshotId(), 7);
    }

    @Test
    public void testValuesRestart()
    {
        ValuesNode a = values("A");
        ValuesNode b = values("B");

        SubPlan root = makePlan(1,
                join(a, b),
                ImmutableList.of());
        planner.plan(root, session, RESUME, null, 7);

        assertNull(a.getResumeSnapshotId());
        assertEquals(a.getNextSnapshotId(), 7);
        assertNull(b.getResumeSnapshotId());
        assertEquals(b.getNextSnapshotId(), 7);
    }

    @Test
    public void testValuesResume()
    {
        ValuesNode a = values("A");
        ValuesNode b = values("B");

        SubPlan root = makePlan(1,
                join(a, b),
                ImmutableList.of());
        planner.plan(root, session, RESUME, 5L, 7);

        assertEquals(a.getResumeSnapshotId().longValue(), 5);
        assertEquals(a.getNextSnapshotId(), 7);
        assertEquals(b.getResumeSnapshotId().longValue(), 5);
        assertEquals(b.getNextSnapshotId(), 7);
    }

    @Test
    public void testSimpleExchangeAndJoin()
    {
        SubPlan root = makePlan(1,
                join(
                        union(source("A"), source("B")),
                        source("C")
                ), ImmutableList.of());
        test(root, ImmutableMultimap.of("A", "C"),
                ImmutableMultimap.of(
                        "A", "B",
                        "B", "C"));
    }

    @Test
    public void testComplexExchangeAndJoin()
    {
        SubPlan root = makePlan(1,
                join(
                        union(ImmutableList.of(
                                join(source("A"), source("B")),
                                join(
                                        union(source("C"), source("D")),
                                        source("E")),
                                source("F"))),
                        union(
                                join(source("G"), source("H")),
                                join(source("I"), source("J")))),
                ImmutableList.of());
        testExact(root, ImmutableMultimap.of(
                "A", "B",
                "A", "G",
                "C", "E",
                "G", "H",
                "I", "J"));
    }

    private SubPlan makePlan(int fragmentId, PlanNode node, List<SubPlan> children)
    {
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId(String.valueOf(fragmentId)),
                node,
                Collections.emptyMap(),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of()),
                ungroupedExecution(),
                StatsAndCosts.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new SubPlan(fragment, children);
    }

    private JoinNode join(PlanNode left, PlanNode right)
    {
        return new JoinNode(
                new PlanNodeId(String.valueOf(++nodeId)),
                INNER,
                left,
                right,
                ImmutableList.of(),
                ImmutableList.of(symbol),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    private SemiJoinNode semiJoin(PlanNode left, PlanNode right)
    {
        return new SemiJoinNode(
                new PlanNodeId(String.valueOf(++nodeId)),
                left,
                right,
                symbol,
                symbol,
                symbol,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private SpatialJoinNode spatialJoin(PlanNode left, PlanNode right)
    {
        return new SpatialJoinNode(
                new PlanNodeId(String.valueOf(++nodeId)),
                SpatialJoinNode.Type.INNER,
                left,
                right,
                ImmutableList.of(symbol),
                mock(RowExpression.class),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private ExchangeNode union(PlanNode left, PlanNode right)
    {
        return union(ImmutableList.of(left, right));
    }

    private ExchangeNode union(ImmutableList<PlanNode> subPlans)
    {
        return new ExchangeNode(
                new PlanNodeId(String.valueOf(++nodeId)),
                GATHER,
                LOCAL,
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                subPlans,
                subPlans.stream().map(p -> ImmutableList.of(symbol)).collect(ImmutableList.toImmutableList()),
                Optional.empty(),
                HASH);
    }

    private TableScanNode source(String name)
    {
        return TableScanNode.newInstance(
                new PlanNodeId(name),
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, mock(ColumnHandle.class)),
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                new UUID(0, 0),
                0,
                false);
    }

    private ValuesNode values(String name)
    {
        return new ValuesNode(
                new PlanNodeId(name),
                ImmutableList.of(symbol),
                ImmutableList.of());
    }

    private RemoteSourceNode remote(int... fragmentId)
    {
        return new RemoteSourceNode(
                new PlanNodeId(String.valueOf(++nodeId)),
                Arrays.stream(fragmentId).mapToObj(String::valueOf).map(PlanFragmentId::new).collect(Collectors.toList()),
                ImmutableList.of(symbol),
                Optional.empty(),
                GATHER,
                RetryPolicy.NONE);
    }
}
