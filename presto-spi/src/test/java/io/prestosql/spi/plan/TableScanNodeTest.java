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

import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.predicate.TupleDomain;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TableScanNodeTest
{
    @Mock
    private PlanNodeId mockId;

    private TableScanNode tableScanNodeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        tableScanNodeUnderTest = new TableScanNode(mockId,
                new TableHandle(new CatalogName("catalogName"), null, null, Optional.empty()),
                Arrays.asList(new Symbol("name")), new HashMap<>(), TupleDomain.withColumnDomains(new HashMap<>()),
                Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                UUID.fromString("67c76a11-1cd1-46a5-bceb-2ed8eeb0b333"), 0, false);
    }

    @Test
    public void testGetSources() throws Exception
    {
        // Setup
        // Run the test
        final List<PlanNode> result = tableScanNodeUnderTest.getSources();

        // Verify the results
    }

//    @Test
//    public void testAccept() throws Exception
//    {
//        // Setup
//        final PlanVisitor<R, C> visitor = null;
//        final C context = null;
//
//        // Run the test
//        tableScanNodeUnderTest.accept(visitor, context);
//
//        // Verify the results
//    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", tableScanNodeUnderTest.toString());
    }

    @Test
    public void testReplaceChildren() throws Exception
    {
        // Setup
        final List<PlanNode> newChildren = Arrays.asList();

        // Run the test
        final PlanNode result = tableScanNodeUnderTest.replaceChildren(newChildren);

        // Verify the results
    }

    @Test
    public void testIsSourcesEqual() throws Exception
    {
        // Setup
        final List<PlanNode> n1 = Arrays.asList();
        final List<PlanNode> n2 = Arrays.asList();

        // Run the test
        final boolean result = tableScanNodeUnderTest.isSourcesEqual(n1, n2);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsSymbolsEqual() throws Exception
    {
        // Setup
        final List<Symbol> s1 = Arrays.asList(new Symbol("name"));
        final List<Symbol> s2 = Arrays.asList(new Symbol("name"));

        // Run the test
        final boolean result = tableScanNodeUnderTest.isSymbolsEqual(s1, s2);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsPredicateSame() throws Exception
    {
        // Setup
        final TableScanNode curr = new TableScanNode(new PlanNodeId("id"),
                new TableHandle(new CatalogName("catalogName"), null, null, Optional.empty()),
                Arrays.asList(new Symbol("name")), new HashMap<>(), TupleDomain.withColumnDomains(new HashMap<>()),
                Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                UUID.fromString("67c76a11-1cd1-46a5-bceb-2ed8eeb0b333"), 0, false);

        // Run the test
        final boolean result = tableScanNodeUnderTest.isPredicateSame(curr);

        // Verify the results
        assertTrue(result);
    }

//    @Test
//    public void testNewInstance() throws Exception
//    {
//        // Setup
//        final PlanNodeId id = new PlanNodeId("id");
//        final TableHandle table = new TableHandle(new CatalogName("catalogName"), null, null, Optional.empty());
//        final List<Symbol> outputs = Arrays.asList(new Symbol("name"));
//        final Map<Symbol, ColumnHandle> assignments = new HashMap<>();
//
//        // Run the test
//        final TableScanNode result = TableScanNode.newInstance(id, table, outputs, assignments,
//                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
//                UUID.fromString("a4fdc7af-2635-4dfb-a1b4-37c5a18bd87b"), 0, false);
//        assertEquals(null, result.getFilterExpr());
//        assertEquals(new TableHandle(new CatalogName("catalogName"), null, null, Optional.empty()), result.getTable());
//        assertTrue(result.isForDelete());
//        assertEquals(Arrays.asList(new Symbol("name")), result.getOutputSymbols());
//        assertEquals(new HashMap<>(), result.getAssignments());
//        assertEquals(ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, result.getStrategy());
//        assertEquals(UUID.fromString("67c76a11-1cd1-46a5-bceb-2ed8eeb0b333"), result.getReuseTableScanMappingId());
//        assertEquals(0, result.getConsumerTableScanNodeCount());
//        assertEquals(TupleDomain.withColumnDomains(new HashMap<>()), result.getEnforcedConstraint());
//        assertEquals(Optional.empty(), result.getPredicate());
//        assertEquals(Arrays.asList(), result.getSources());
//        final PlanVisitor<R, C> visitor = null;
//        final C context = null;
//        assertEquals(null, result.accept(visitor, context));
//        assertEquals("result", result.toString());
//        final List<PlanNode> newChildren = Arrays.asList();
//        assertEquals(null, result.replaceChildren(newChildren));
//        final List<PlanNode> n1 = Arrays.asList();
//        final List<PlanNode> n2 = Arrays.asList();
//        assertTrue(result.isSourcesEqual(n1, n2));
//        final List<Symbol> s1 = Arrays.asList(new Symbol("name"));
//        final List<Symbol> s2 = Arrays.asList(new Symbol("name"));
//        assertTrue(result.isSymbolsEqual(s1, s2));
//        final TableScanNode curr = new TableScanNode(new PlanNodeId("id"),
//                new TableHandle(new CatalogName("catalogName"), null, null, Optional.empty()),
//                Arrays.asList(new Symbol("name")), new HashMap<>(), TupleDomain.withColumnDomains(new HashMap<>()),
//                Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
//                UUID.fromString("67c76a11-1cd1-46a5-bceb-2ed8eeb0b333"), 0, false);
//        assertTrue(result.isPredicateSame(curr));
//        assertEquals(new PlanNodeId("id"), result.getId());
//        assertEquals(Arrays.asList(new Symbol("name")), result.getInputSymbols());
//        assertEquals(Arrays.asList(new Symbol("name")), result.getAllSymbols());
//        assertEquals(PlanNode.SkipOptRuleLevel.APPLY_ALL_RULES, result.getSkipOptRuleLevel());
//    }

    @Test
    public void testGetActualColName() throws Exception
    {
        assertEquals("var", TableScanNode.getActualColName("var"));
    }
}
