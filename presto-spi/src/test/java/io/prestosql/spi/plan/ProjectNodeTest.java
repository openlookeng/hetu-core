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

public class ProjectNodeTest
{
    @Mock
    private PlanNodeId mockId;
    @Mock
    private PlanNode mockSource;
    @Mock
    private Assignments mockAssignments;

    private ProjectNode projectNodeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        projectNodeUnderTest = new ProjectNode(mockId, mockSource, mockAssignments);
    }

    @Test
    public void testGetOutputSymbols() throws Exception
    {
        // Setup
        final List<Symbol> expectedResult = Arrays.asList(new Symbol("name"));
        when(mockAssignments.getOutputs()).thenReturn(Arrays.asList(new Symbol("name")));

        // Run the test
        final List<Symbol> result = projectNodeUnderTest.getOutputSymbols();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetOutputSymbols_AssignmentsReturnsNoItems() throws Exception
    {
        // Setup
        when(mockAssignments.getOutputs()).thenReturn(Collections.emptyList());

        // Run the test
        final List<Symbol> result = projectNodeUnderTest.getOutputSymbols();

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testGetSources() throws Exception
    {
        // Setup
        // Run the test
        final List<PlanNode> result = projectNodeUnderTest.getSources();

        // Verify the results
    }

    @Test
    public void testReplaceChildren() throws Exception
    {
        // Setup
        final List<PlanNode> newChildren = Arrays.asList();

        // Run the test
        final PlanNode result = projectNodeUnderTest.replaceChildren(newChildren);

        // Verify the results
    }
}
