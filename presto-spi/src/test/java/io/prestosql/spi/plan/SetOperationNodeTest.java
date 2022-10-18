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
import com.google.common.collect.ListMultimap;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class SetOperationNodeTest
{
    @Mock
    private PlanNodeId mockId;

    private SetOperationNode setOperationNodeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        setOperationNodeUnderTest = new SetOperationNode(mockId, Arrays.asList(),
                ImmutableListMultimap.of(new Symbol("name"), new Symbol("name")), Arrays.asList(new Symbol("name"))) {
            @Override
            public PlanNode replaceChildren(List<PlanNode> newChildren)
            {
                return null;
            }
        };
    }

    @Test
    public void testGetOutputSymbols() throws Exception
    {
        // Setup
        final List<Symbol> expectedResult = Arrays.asList(new Symbol("name"));

        // Run the test
        final List<Symbol> result = setOperationNodeUnderTest.getOutputSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetSymbolMapping() throws Exception
    {
        // Setup
        final ListMultimap<Symbol, Symbol> expectedResult = ImmutableListMultimap.of(new Symbol("name"),
                new Symbol("name"));

        // Run the test
        final ListMultimap<Symbol, Symbol> result = setOperationNodeUnderTest.getSymbolMapping();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testSourceOutputLayout() throws Exception
    {
        // Setup
        final List<Symbol> expectedResult = Arrays.asList(new Symbol("name"));

        // Run the test
        final List<Symbol> result = setOperationNodeUnderTest.sourceOutputLayout(0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testSourceSymbolMap() throws Exception
    {
        // Setup
        final Map<Symbol, Symbol> expectedResult = new HashMap<>();

        // Run the test
        final Map<Symbol, Symbol> result = setOperationNodeUnderTest.sourceSymbolMap(0);

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
