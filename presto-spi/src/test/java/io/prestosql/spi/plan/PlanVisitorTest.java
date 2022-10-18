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
package io.prestosql.spi.plan;

import com.google.common.collect.ImmutableListMultimap;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.predicate.TupleDomain;
import org.checkerframework.checker.units.qual.C;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;

public class PlanVisitorTest
{
    private PlanVisitor<Object, C> planVisitorUnderTest;

    @Test
    public void testVisitAggregation() throws Exception
    {
        // Setup
        final AggregationNode node = new AggregationNode(new PlanNodeId("id"), null, new HashMap<>(),
                new AggregationNode.GroupingSetDescriptor(
                        Arrays.asList(new Symbol("name")), 0, new HashSet<>(Arrays.asList(0))),
                Arrays.asList(new Symbol("name")), AggregationNode.Step.PARTIAL, Optional.of(new Symbol("name")),
                Optional.of(new Symbol("name")), AggregationNode.AggregationType.HASH, Optional.of(new Symbol("name")));
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitAggregation(node, context);

        // Verify the results
    }

    @Test
    public void testVisitExcept() throws Exception
    {
        // Setup
        final ExceptNode node = new ExceptNode(new PlanNodeId("id"), Arrays.asList(),
                ImmutableListMultimap.of(new Symbol("name"), new Symbol("name")), Arrays.asList(new Symbol("name")));
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitExcept(node, context);

        // Verify the results
    }

    @Test
    public void testVisitFilter() throws Exception
    {
        // Setup
        final FilterNode node = new FilterNode(new PlanNodeId("id"), null, null);
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitFilter(node, context);

        // Verify the results
    }

    @Test
    public void testVisitIntersect() throws Exception
    {
        // Setup
        final IntersectNode node = new IntersectNode(new PlanNodeId("id"), Arrays.asList(),
                ImmutableListMultimap.of(new Symbol("name"), new Symbol("name")), Arrays.asList(new Symbol("name")));
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitIntersect(node, context);

        // Verify the results
    }

    @Test
    public void testVisitJoin() throws Exception
    {
        // Setup
        final JoinNode node = new JoinNode(new PlanNodeId("id"), JoinNode.Type.INNER, null, null,
                Arrays.asList(new JoinNode.EquiJoinClause(new Symbol("name"), new Symbol("name"))),
                Arrays.asList(new Symbol("name")), Optional.empty(), Optional.of(new Symbol("name")),
                Optional.of(new Symbol("name")), Optional.of(JoinNode.DistributionType.PARTITIONED), Optional.of(false),
                new HashMap<>());
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitJoin(node, context);

        // Verify the results
    }

    @Test
    public void testVisitLimit() throws Exception
    {
        // Setup
        final LimitNode node = new LimitNode(new PlanNodeId("id"), null, 0L, Optional.of(new OrderingScheme(
                Arrays.asList(new Symbol("name")), new HashMap<>())), false);
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitLimit(node, context);

        // Verify the results
    }

    @Test
    public void testVisitMarkDistinct() throws Exception
    {
        // Setup
        final MarkDistinctNode node = new MarkDistinctNode(new PlanNodeId("id"), null, new Symbol("name"),
                Arrays.asList(new Symbol("name")), Optional.of(new Symbol("name")));
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitMarkDistinct(node, context);

        // Verify the results
    }

    @Test
    public void testVisitProject() throws Exception
    {
        // Setup
        final ProjectNode node = new ProjectNode(new PlanNodeId("id"), null, new Assignments(new HashMap<>()));
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitProject(node, context);

        // Verify the results
    }

    @Test
    public void testVisitTableScan() throws Exception
    {
        // Setup
        final TableScanNode node = new TableScanNode(new PlanNodeId("id"),
                new TableHandle(new CatalogName("catalogName"), null, null, Optional.empty()),
                Arrays.asList(new Symbol("name")), new HashMap<>(), TupleDomain.withColumnDomains(new HashMap<>()),
                Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                UUID.fromString("d31dc43f-b323-43a0-9234-8cfd4492a24d"), 0, false);
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitTableScan(node, context);

        // Verify the results
    }

    @Test
    public void testVisitTopN() throws Exception
    {
        // Setup
        final TopNNode node = new TopNNode(new PlanNodeId("id"), null, 0L,
                new OrderingScheme(Arrays.asList(new Symbol("name")), new HashMap<>()), TopNNode.Step.SINGLE);
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitTopN(node, context);

        // Verify the results
    }

    @Test
    public void testVisitUnion() throws Exception
    {
        // Setup
        final UnionNode node = new UnionNode(new PlanNodeId("id"), Arrays.asList(),
                ImmutableListMultimap.of(new Symbol("name"), new Symbol("name")), Arrays.asList(new Symbol("name")));
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitUnion(node, context);

        // Verify the results
    }

    @Test
    public void testVisitValues() throws Exception
    {
        // Setup
        final ValuesNode node = new ValuesNode(new PlanNodeId("id"), Arrays.asList(new Symbol("name")), Arrays.asList(
                Arrays.asList()), 0L, 0L);
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitValues(node, context);

        // Verify the results
    }

    @Test
    public void testVisitGroupReference() throws Exception
    {
        // Setup
        final GroupReference node = new GroupReference(new PlanNodeId("id"), 0, Arrays.asList(new Symbol("name")));
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitGroupReference(node, context);

        // Verify the results
    }

    @Test
    public void testVisitWindow() throws Exception
    {
        // Setup
        final WindowNode node = new WindowNode(new PlanNodeId("id"), null,
                new WindowNode.Specification(Arrays.asList(new Symbol("name")), Optional.of(new OrderingScheme(
                        Arrays.asList(new Symbol("name")), new HashMap<>()))), new HashMap<>(),
                Optional.of(new Symbol("name")), new HashSet<>(
                Arrays.asList(new Symbol("name"))), 0);
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitWindow(node, context);

        // Verify the results
    }

    @Test
    public void testVisitGroupId() throws Exception
    {
        // Setup
        final GroupIdNode node = new GroupIdNode(new PlanNodeId("id"), null,
                Arrays.asList(Arrays.asList(new Symbol("name"))), new HashMap<>(), Arrays.asList(new Symbol("name")),
                new Symbol("name"));
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitGroupId(node, context);

        // Verify the results
    }

    @Test
    public void testVisitCTEScan() throws Exception
    {
        // Setup
        final CTEScanNode node = new CTEScanNode(new PlanNodeId("id"), null, Arrays.asList(new Symbol("name")),
                Optional.empty(), "cteRefName", new HashSet<>(
                Arrays.asList(new PlanNodeId("id"))), 0);
        final C context = null;

        // Run the test
        planVisitorUnderTest.visitCTEScan(node, context);

        // Verify the results
    }
}
