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

import org.checkerframework.checker.units.qual.C;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AggregationNodeTest
{
    @Mock
    private PlanNodeId mockId;
    @Mock
    private PlanNode mockSource;

    private AggregationNode aggregationNodeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        aggregationNodeUnderTest = new AggregationNode(mockId, mockSource, new HashMap<>(),
                new AggregationNode.GroupingSetDescriptor(
                        Arrays.asList(new Symbol("name")), 0, new HashSet<>(Arrays.asList(0))),
                Arrays.asList(new Symbol("name")), AggregationNode.Step.PARTIAL,
                Optional.of(new Symbol("name")), Optional.of(new Symbol("name")), AggregationNode.AggregationType.HASH,
                Optional.of(new Symbol("name")));
    }

    @Test
    public void testGetGroupingKeys() throws Exception
    {
        // Setup
        final List<Symbol> expectedResult = Arrays.asList(new Symbol("name"));

        // Run the test
        final List<Symbol> result = aggregationNodeUnderTest.getGroupingKeys();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testHasDefaultOutput() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = aggregationNodeUnderTest.hasDefaultOutput();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testHasEmptyGroupingSet() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = aggregationNodeUnderTest.hasEmptyGroupingSet();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testHasNonEmptyGroupingSet() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = aggregationNodeUnderTest.hasNonEmptyGroupingSet();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetSources() throws Exception
    {
        // Setup
        // Run the test
        final List<PlanNode> result = aggregationNodeUnderTest.getSources();

        // Verify the results
    }

    @Test
    public void testGetOutputSymbols() throws Exception
    {
        // Setup
        final List<Symbol> expectedResult = Arrays.asList(new Symbol("name"));

        // Run the test
        final List<Symbol> result = aggregationNodeUnderTest.getOutputSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetGroupingSetCount() throws Exception
    {
        // Setup
        // Run the test
        final int result = aggregationNodeUnderTest.getGroupingSetCount();

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testGetGlobalGroupingSets() throws Exception
    {
        // Setup
        // Run the test
        final Set<Integer> result = aggregationNodeUnderTest.getGlobalGroupingSets();

        // Verify the results
        assertEquals(new HashSet<>(Arrays.asList(0)), result);
    }

    @Test
    public void testHasOrderings() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = aggregationNodeUnderTest.hasOrderings();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testAccept() throws Exception
    {
        // Setup
        final PlanVisitor<Object, C> visitor = null;
        final C context = null;

        // Run the test
        aggregationNodeUnderTest.accept(visitor, context);

        // Verify the results
    }

    @Test
    public void testReplaceChildren() throws Exception
    {
        // Setup
        final List<PlanNode> newChildren = Arrays.asList();

        // Run the test
        final PlanNode result = aggregationNodeUnderTest.replaceChildren(newChildren);

        // Verify the results
    }

    @Test
    public void testProducesDistinctRows() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = aggregationNodeUnderTest.producesDistinctRows();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsStreamable() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = aggregationNodeUnderTest.isStreamable();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGlobalAggregation() throws Exception
    {
        // Setup
        // Run the test
        final AggregationNode.GroupingSetDescriptor result = AggregationNode.globalAggregation();

        // Verify the results
    }

    @Test
    public void testSingleGroupingSet() throws Exception
    {
        // Setup
        final List<Symbol> groupingKeys = Arrays.asList(new Symbol("name"));

        // Run the test
        final AggregationNode.GroupingSetDescriptor result = AggregationNode.singleGroupingSet(groupingKeys);

        // Verify the results
    }

    @Test
    public void testGroupingSets() throws Exception
    {
        // Setup
        final List<Symbol> groupingKeys = Arrays.asList(new Symbol("name"));

        // Run the test
        final AggregationNode.GroupingSetDescriptor result = AggregationNode.groupingSets(groupingKeys, 0,
                new HashSet<>(Arrays.asList(0)));

        // Verify the results
    }
}
