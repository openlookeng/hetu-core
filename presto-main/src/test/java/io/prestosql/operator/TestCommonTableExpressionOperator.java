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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.prestosql.SequencePageBuilder;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertTrue;

public class TestCommonTableExpressionOperator
        extends AbstractTestFunctions
{
    private final Metadata metadata = createTestMetadataManager();
    private final ExpressionCompiler expressionCompiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    public TestCommonTableExpressionOperator()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @Test
    public void testOperatorSource()
    {
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();

        CommonTableExecutionContext cteContext = new CommonTableExecutionContext("test_cte_prod_1",
                ImmutableSet.of(new PlanNodeId("consumer_1"), new PlanNodeId("consumer_2")), new PlanNodeId("consumer_1"),
                driverContext.getNotificationExecutor(), 0, 1024, 512);

        CommonTableExpressionOperator.CommonTableExpressionOperatorFactory parent1 = new CommonTableExpressionOperator.CommonTableExpressionOperatorFactory(
                0,
                new PlanNodeId("test"),
                cteContext,
                ImmutableList.of(VARCHAR),
                new DataSize(0, DataSize.Unit.BYTE),
                0,
                symbol -> symbol);
        parent1.addConsumer(new PlanNodeId("consumer_1"));

        CommonTableExpressionOperator.CommonTableExpressionOperatorFactory parent2 = new CommonTableExpressionOperator.CommonTableExpressionOperatorFactory(
                1,
                new PlanNodeId("test"),
                cteContext,
                ImmutableList.of(VARCHAR),
                new DataSize(0, DataSize.Unit.BYTE),
                0,
                symbol -> symbol);
        parent2.addConsumer(new PlanNodeId("consumer_2"));

        //Operator operator = factory.createOperator(driverContext);
        MaterializedResult result = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR)
                .page(input)
                .build();

        assertOperatorEquals(parent1, driverContext, ImmutableList.of(input), result);
        assertOperatorEquals(parent2, driverContext, ImmutableList.of(input), result);
    }

    private static List<Page> toPages(Operator operator)
    {
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        // read output until input is needed or operator is finished
        int nullPages = 0;
        while (!operator.isFinished()) {
            Page outputPage = operator.getOutput();
            if (outputPage == null) {
                // break infinite loop due to null pages
                assertTrue(nullPages < 1_000_000, "Too many null pages; infinite loop?");
                nullPages++;
            }
            else {
                outputPages.add(outputPage);
                nullPages = 0;
            }
        }

        return outputPages.build();
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
