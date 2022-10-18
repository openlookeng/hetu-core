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
import java.util.Collection;
import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class PlanNodeTest
{
    @Mock
    private PlanNodeId mockId;

    private PlanNode planNodeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        planNodeUnderTest = new PlanNode(mockId) {
            @Override
            public List<PlanNode> getSources()
            {
                return null;
            }

            @Override
            public List<Symbol> getOutputSymbols()
            {
                return null;
            }

            @Override
            public PlanNode replaceChildren(List<PlanNode> newChildren)
            {
                return null;
            }
        };
    }

    @Test
    public void testGetInputSymbols() throws Exception
    {
        // Setup
        final Collection<Symbol> expectedResult = Arrays.asList(new Symbol("name"));

        // Run the test
        final Collection<Symbol> result = planNodeUnderTest.getInputSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetAllSymbols() throws Exception
    {
        // Setup
        final List<Symbol> expectedResult = Arrays.asList(new Symbol("name"));

        // Run the test
        final List<Symbol> result = planNodeUnderTest.getAllSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testAccept() throws Exception
    {
        // Setup
        final PlanVisitor<Object, C> visitor = null;
        final C context = null;

        // Run the test
        planNodeUnderTest.accept(visitor, context);

        // Verify the results
    }
}
