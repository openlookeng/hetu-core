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
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;

public class UnionNodeTest
{
    @Mock
    private PlanNodeId mockId;

    private UnionNode unionNodeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        unionNodeUnderTest = new UnionNode(mockId, Arrays.asList(),
                ImmutableListMultimap.of(new Symbol("name"), new Symbol("name")), Arrays.asList(new Symbol("name")));
    }

    @Test
    public void testReplaceChildren() throws Exception
    {
        // Setup
        final List<PlanNode> newChildren = Arrays.asList();

        // Run the test
        final PlanNode result = unionNodeUnderTest.replaceChildren(newChildren);

        // Verify the results
    }
}
