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
import java.util.Objects;
import java.util.Set;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class GroupIdNodeTest
{
    @Mock
    private PlanNodeId mockId;
    @Mock
    private PlanNode mockSource;
    @Mock
    private Symbol mockGroupIdSymbol;

    private GroupIdNode groupIdNodeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        groupIdNodeUnderTest = new GroupIdNode(mockId, mockSource, Arrays.asList(Arrays.asList(new Symbol("name"))),
                new HashMap<>(),
                Arrays.asList(new Symbol("name")), mockGroupIdSymbol);
    }

    @Test
    public void testGetOutputSymbols() throws Exception
    {
        // Setup
        final List<Symbol> expectedResult = Arrays.asList(new Symbol("name"));

        // Run the test
        final List<Symbol> result = groupIdNodeUnderTest.getOutputSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetSources() throws Exception
    {
        // Setup
        // Run the test
        final List<PlanNode> result = groupIdNodeUnderTest.getSources();

        // Verify the results
    }

    @Test
    public void testAccept() throws Exception
    {
        // Setup
        final PlanVisitor<Objects, C> visitor = null;
        final C context = null;

        // Run the test
        groupIdNodeUnderTest.accept(visitor, context);

        // Verify the results
    }

    @Test
    public void testGetInputSymbols() throws Exception
    {
        // Setup
        final Set<Symbol> expectedResult = new HashSet<>(Arrays.asList(new Symbol("name")));

        // Run the test
        final Set<Symbol> result = groupIdNodeUnderTest.getInputSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetCommonGroupingColumns() throws Exception
    {
        // Setup
        final Set<Symbol> expectedResult = new HashSet<>(Arrays.asList(new Symbol("name")));

        // Run the test
        final Set<Symbol> result = groupIdNodeUnderTest.getCommonGroupingColumns();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testReplaceChildren() throws Exception
    {
        // Setup
        final List<PlanNode> newChildren = Arrays.asList();

        // Run the test
        final PlanNode result = groupIdNodeUnderTest.replaceChildren(newChildren);

        // Verify the results
    }
}
