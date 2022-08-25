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

import io.prestosql.spi.sql.expression.Types;
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

public class WindowNodeTest
{
    @Mock
    private PlanNodeId mockId;
    @Mock
    private PlanNode mockSource;

    private WindowNode windowNodeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        windowNodeUnderTest = new WindowNode(mockId, mockSource, new WindowNode.Specification(
                Arrays.asList(new Symbol("name")),
                Optional.of(new OrderingScheme(Arrays.asList(new Symbol("name")), new HashMap<>()))), new HashMap<>(),
                Optional.of(new Symbol("name")), new HashSet<>(Arrays.asList(new Symbol("name"))), 0);
    }

    @Test
    public void testGetSources() throws Exception
    {
        // Setup
        // Run the test
        final List<PlanNode> result = windowNodeUnderTest.getSources();

        // Verify the results
    }

    @Test
    public void testGetOutputSymbols() throws Exception
    {
        // Setup
        final List<Symbol> expectedResult = Arrays.asList(new Symbol("name"));
        when(mockSource.getOutputSymbols()).thenReturn(Arrays.asList(new Symbol("name")));

        // Run the test
        final List<Symbol> result = windowNodeUnderTest.getOutputSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetOutputSymbols_PlanNodeReturnsNoItems() throws Exception
    {
        // Setup
        when(mockSource.getOutputSymbols()).thenReturn(Collections.emptyList());

        // Run the test
        final List<Symbol> result = windowNodeUnderTest.getOutputSymbols();

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testGetCreatedSymbols() throws Exception
    {
        // Setup
        final Set<Symbol> expectedResult = new HashSet<>(Arrays.asList(new Symbol("name")));

        // Run the test
        final Set<Symbol> result = windowNodeUnderTest.getCreatedSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionBy() throws Exception
    {
        // Setup
        final List<Symbol> expectedResult = Arrays.asList(new Symbol("name"));

        // Run the test
        final List<Symbol> result = windowNodeUnderTest.getPartitionBy();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetOrderingScheme() throws Exception
    {
        // Setup
        final Optional<OrderingScheme> expectedResult = Optional.of(
                new OrderingScheme(Arrays.asList(new Symbol("name")), new HashMap<>()));

        // Run the test
        final Optional<OrderingScheme> result = windowNodeUnderTest.getOrderingScheme();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetFrames() throws Exception
    {
        // Setup
        final List<WindowNode.Frame> expectedResult = Arrays.asList(
                new WindowNode.Frame(Types.WindowFrameType.RANGE, Types.FrameBoundType.UNBOUNDED_PRECEDING,
                        Optional.of(new Symbol("name")), Types.FrameBoundType.UNBOUNDED_PRECEDING,
                        Optional.of(new Symbol("name")), Optional.of("value"), Optional.of("value")));

        // Run the test
        final List<WindowNode.Frame> result = windowNodeUnderTest.getFrames();

        // Verify the results
        assertEquals(expectedResult, result);
    }

//    @Test
//    public void testAccept() throws Exception
//    {
//        // Setup
//        final PlanVisitor<R, C> visitor = null;
//        final C context = null;
//
//        // Run the test
//        final R result = windowNodeUnderTest.accept(visitor, context);
//
//        // Verify the results
//    }

    @Test
    public void testReplaceChildren() throws Exception
    {
        // Setup
        final List<PlanNode> newChildren = Arrays.asList();

        // Run the test
        final PlanNode result = windowNodeUnderTest.replaceChildren(newChildren);

        // Verify the results
    }
}
