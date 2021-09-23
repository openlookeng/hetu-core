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
package io.prestosql.operator;

import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.SnapshotTestUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestAssignUniqueIdOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private TaskContext taskContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testAssignUniqueIdSnapshot()
    {
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        OperatorFactory factory = new AssignUniqueIdOperator.AssignUniqueIdOperatorFactory(0, new PlanNodeId("plan-node-0"));
        AssignUniqueIdOperator operator = (AssignUniqueIdOperator) factory.createOperator(driverContext);

        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(9, 0)
                .build();

        operator.addInput(input.get(0));
        Page output = operator.getOutput();
        Object snapshot = operator.capture(operator.getOperatorContext().getDriverContext().getSerde());
        assertEquals(SnapshotTestUtil.toSimpleSnapshotMapping(snapshot), createExpectedMapping());
        assertEquals(output.getPositionCount(), 9);
        assertEquals(output.getChannelCount(), 2);
    }

    private Map<String, Object> createExpectedMapping()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("operatorContext", 0);
        expectedMapping.put("finishing", false);
        expectedMapping.put("rowIdPool", 1048576L);
        expectedMapping.put("rowIdCounter", 9L);
        expectedMapping.put("maxRowIdCounterValue", 1048576L);
        return expectedMapping;
    }
}
