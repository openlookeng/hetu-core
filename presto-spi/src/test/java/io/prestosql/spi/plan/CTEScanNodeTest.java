/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class CTEScanNodeTest
{
    @Mock
    private PlanNodeId mockId;
    @Mock
    private PlanNode mockSource;

    private CTEScanNode cteScanNodeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        cteScanNodeUnderTest = new CTEScanNode(mockId, mockSource, Arrays.asList(new Symbol("name")),
                Optional.empty(), "cteRefName", new HashSet<>(Arrays.asList(new PlanNodeId("id"))), 0);
    }

    @Test
    public void testGetSources() throws Exception
    {
        // Setup
        // Run the test
        final List<PlanNode> result = cteScanNodeUnderTest.getSources();

        // Verify the results
    }

    @Test
    public void testUpdateConsumerPlans() throws Exception
    {
        // Setup
        final Set<PlanNodeId> planNodeId = new HashSet<>(Arrays.asList(new PlanNodeId("id")));

        // Run the test
        cteScanNodeUnderTest.updateConsumerPlans(planNodeId);

        // Verify the results
    }

    @Test
    public void testUpdateConsumerPlan() throws Exception
    {
        // Setup
        final PlanNodeId planNodeId = new PlanNodeId("id");

        // Run the test
        cteScanNodeUnderTest.updateConsumerPlan(planNodeId);

        // Verify the results
    }

    @Test
    public void testReplaceChildren() throws Exception
    {
        // Setup
        final List<PlanNode> newChildren = Arrays.asList();

        // Run the test
        final PlanNode result = cteScanNodeUnderTest.replaceChildren(newChildren);

        // Verify the results
    }

    @Test
    public void testAccept() throws Exception
    {
        // Setup
        final PlanVisitor<Object, C> visitor = null;
        final C context = null;

        // Run the test
        cteScanNodeUnderTest.accept(visitor, context);

        // Verify the results
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", cteScanNodeUnderTest.toString());
    }

    @Test
    public void testIsNotCTEScanNode() throws Exception
    {
        // Setup
        final PlanNode node = null;

        // Run the test
        final boolean result = CTEScanNode.isNotCTEScanNode(node);

        // Verify the results
        assertTrue(result);
    }
}
