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
import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;

public class ValuesNodeTest
{
    @Mock
    private PlanNodeId mockId;

    private ValuesNode valuesNodeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        valuesNodeUnderTest = new ValuesNode(mockId, Arrays.asList(new Symbol("name")),
                Arrays.asList(Arrays.asList()), 0L, 0L);
    }

    @Test
    public void testGetSources() throws Exception
    {
        // Setup
        // Run the test
        final List<PlanNode> result = valuesNodeUnderTest.getSources();

        // Verify the results
    }

    @Test
    public void testReplaceChildren() throws Exception
    {
        // Setup
        final List<PlanNode> newChildren = Arrays.asList();

        // Run the test
        final PlanNode result = valuesNodeUnderTest.replaceChildren(newChildren);

        // Verify the results
    }

    @Test
    public void testSetupSnapshot() throws Exception
    {
        // Setup
        // Run the test
        valuesNodeUnderTest.setupSnapshot(0L, 0L);

        // Verify the results
    }
}
