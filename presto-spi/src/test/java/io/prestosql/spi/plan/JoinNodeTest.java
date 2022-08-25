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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class JoinNodeTest
{
    @Mock
    private PlanNodeId mockId;
    @Mock
    private PlanNode mockLeft;
    @Mock
    private PlanNode mockRight;

    private JoinNode joinNodeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        joinNodeUnderTest = new JoinNode(mockId, JoinNode.Type.INNER, mockLeft, mockRight,
                Arrays.asList(new JoinNode.EquiJoinClause(new Symbol("name"), new Symbol("name"))),
                Arrays.asList(new Symbol("name")), Optional.empty(), Optional.of(new Symbol("name")),
                Optional.of(new Symbol("name")), Optional.of(JoinNode.DistributionType.PARTITIONED),
                Optional.of(false), new HashMap<>());
    }

    @Test
    public void testFlipChildren() throws Exception
    {
        // Setup
        // Run the test
        final JoinNode result = joinNodeUnderTest.flipChildren();

        // Verify the results
    }

    @Test
    public void testGetRightOutputSymbols() throws Exception
    {
        // Setup
        final Set<Symbol> expectedResult = new HashSet<>(Arrays.asList(new Symbol("name")));
        when(mockRight.getOutputSymbols()).thenReturn(Arrays.asList(new Symbol("name")));

        // Run the test
        final Set<Symbol> result = joinNodeUnderTest.getRightOutputSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetRightOutputSymbols_PlanNodeReturnsNoItems() throws Exception
    {
        // Setup
        when(mockRight.getOutputSymbols()).thenReturn(Collections.emptyList());

        // Run the test
        final Set<Symbol> result = joinNodeUnderTest.getRightOutputSymbols();

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testGetSources() throws Exception
    {
        // Setup
        // Run the test
        final List<PlanNode> result = joinNodeUnderTest.getSources();

        // Verify the results
    }

    @Test
    public void testAccept() throws Exception
    {
        // Setup
        final PlanVisitor<Object, C> visitor = null;
        final C context = null;

        // Run the test
        joinNodeUnderTest.accept(visitor, context);

        // Verify the results
    }

    @Test
    public void testReplaceChildren() throws Exception
    {
        // Setup
        final List<PlanNode> newChildren = Arrays.asList();

        // Run the test
        final PlanNode result = joinNodeUnderTest.replaceChildren(newChildren);

        // Verify the results
    }

    @Test
    public void testWithDistributionType() throws Exception
    {
        // Setup
        // Run the test
        final JoinNode result = joinNodeUnderTest.withDistributionType(JoinNode.DistributionType.PARTITIONED);

        // Verify the results
    }

    @Test
    public void testWithSpillable() throws Exception
    {
        // Setup
        // Run the test
        final JoinNode result = joinNodeUnderTest.withSpillable(false);

        // Verify the results
    }

    @Test
    public void testIsCrossJoin() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = joinNodeUnderTest.isCrossJoin();

        // Verify the results
        assertTrue(result);
    }
}
