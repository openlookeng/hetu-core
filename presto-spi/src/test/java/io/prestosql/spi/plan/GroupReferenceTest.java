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
import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class GroupReferenceTest
{
    @Mock
    private PlanNodeId mockId;

    private GroupReference groupReferenceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        groupReferenceUnderTest = new GroupReference(mockId, 0, Arrays.asList(new Symbol("name")));
    }

    @Test
    public void testGetSources() throws Exception
    {
        // Setup
        // Run the test
        final List<PlanNode> result = groupReferenceUnderTest.getSources();

        // Verify the results
    }

    @Test
    public void testAccept() throws Exception
    {
        // Setup
        final PlanVisitor<Object, C> visitor = null;
        final C context = null;

        // Run the test
        groupReferenceUnderTest.accept(visitor, context);

        // Verify the results
    }

    @Test
    public void testGetOutputSymbols() throws Exception
    {
        // Setup
        final List<Symbol> expectedResult = Arrays.asList(new Symbol("name"));

        // Run the test
        final List<Symbol> result = groupReferenceUnderTest.getOutputSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testReplaceChildren() throws Exception
    {
        // Setup
        final List<PlanNode> newChildren = Arrays.asList();

        // Run the test
        final PlanNode result = groupReferenceUnderTest.replaceChildren(newChildren);

        // Verify the results
    }
}
