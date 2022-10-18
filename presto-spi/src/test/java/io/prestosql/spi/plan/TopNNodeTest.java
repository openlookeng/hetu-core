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

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class TopNNodeTest
{
    @Mock
    private PlanNodeId mockId;
    @Mock
    private PlanNode mockSource;
    @Mock
    private OrderingScheme mockOrderingScheme;

    private TopNNode topNNodeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        topNNodeUnderTest = new TopNNode(mockId, mockSource, 0L, mockOrderingScheme, TopNNode.Step.SINGLE);
    }

    @Test
    public void testGetSources() throws Exception
    {
        // Setup
        // Run the test
        final List<PlanNode> result = topNNodeUnderTest.getSources();

        // Verify the results
    }

    @Test
    public void testGetOutputSymbols() throws Exception
    {
        // Setup
        final List<Symbol> expectedResult = Arrays.asList(new Symbol("name"));
        when(mockSource.getOutputSymbols()).thenReturn(Arrays.asList(new Symbol("name")));

        // Run the test
        final List<Symbol> result = topNNodeUnderTest.getOutputSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetOutputSymbols_PlanNodeReturnsNoItems() throws Exception
    {
        // Setup
        when(mockSource.getOutputSymbols()).thenReturn(Collections.emptyList());

        // Run the test
        final List<Symbol> result = topNNodeUnderTest.getOutputSymbols();

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testReplaceChildren() throws Exception
    {
        // Setup
        final List<PlanNode> newChildren = Arrays.asList();

        // Run the test
        final PlanNode result = topNNodeUnderTest.replaceChildren(newChildren);

        // Verify the results
    }
}
