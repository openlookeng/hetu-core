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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEqualsWithStateComparison;
import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.BETWEEN;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestFilterAndProjectOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void test()
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .addSequencePage(100, 0, 0)
                .build();

        RowExpression filter = call(BETWEEN.getFunctionName().toString(),
                new BuiltInFunctionHandle(Signature.internalOperator(BETWEEN, BOOLEAN.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature()))),
                BOOLEAN,
                field(1, BIGINT),
                constant(10L, BIGINT),
                constant(19L, BIGINT));

        RowExpression field0 = field(0, VARCHAR);
        RowExpression add5 = call(ADD.getFunctionName().toString(),
                new BuiltInFunctionHandle(Signature.internalOperator(ADD, BIGINT.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()))),
                BIGINT,
                field(1, BIGINT),
                constant(5L, BIGINT));

        Metadata metadata = createTestMetadataManager();
        ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
        Supplier<PageProcessor> processor = compiler.compilePageProcessor(Optional.of(filter), ImmutableList.of(field0, add5));

        OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                processor,
                ImmutableList.of(VARCHAR, BIGINT),
                new DataSize(0, BYTE),
                0);

        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("10", 15L)
                .row("11", 16L)
                .row("12", 17L)
                .row("13", 18L)
                .row("14", 19L)
                .row("15", 20L)
                .row("16", 21L)
                .row("17", 22L)
                .row("18", 23L)
                .row("19", 24L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testSnapshot()
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .addSequencePage(100, 0, 0)
                .addSequencePage(100, 0, 0)
                .addSequencePage(100, 0, 0)
                .build();

        RowExpression filter = call(
                BETWEEN.getFunctionName().toString(),
                new BuiltInFunctionHandle(Signature.internalOperator(BETWEEN, BOOLEAN.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature()))),
                BOOLEAN,
                field(1, BIGINT),
                constant(10L, BIGINT),
                constant(19L, BIGINT));

        RowExpression field0 = field(0, VARCHAR);
        RowExpression add5 = call(
                ADD.getFunctionName().toString(),
                new BuiltInFunctionHandle(Signature.internalOperator(ADD, BIGINT.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()))),
                BIGINT,
                field(1, BIGINT),
                constant(5L, BIGINT));

        Metadata metadata = createTestMetadataManager();
        ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
        Supplier<PageProcessor> processor = compiler.compilePageProcessor(Optional.of(filter), ImmutableList.of(field0, add5));

        OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                processor,
                ImmutableList.of(VARCHAR, BIGINT),
                new DataSize(0, BYTE),
                0);

        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("10", 15L)
                .row("11", 16L)
                .row("12", 17L)
                .row("13", 18L)
                .row("14", 19L)
                .row("15", 20L)
                .row("16", 21L)
                .row("17", 22L)
                .row("18", 23L)
                .row("19", 24L)
                .row("10", 15L)
                .row("11", 16L)
                .row("12", 17L)
                .row("13", 18L)
                .row("14", 19L)
                .row("15", 20L)
                .row("16", 21L)
                .row("17", 22L)
                .row("18", 23L)
                .row("19", 24L)
                .row("10", 15L)
                .row("11", 16L)
                .row("12", 17L)
                .row("13", 18L)
                .row("14", 19L)
                .row("15", 20L)
                .row("16", 21L)
                .row("17", 22L)
                .row("18", 23L)
                .row("19", 24L)
                .build();

        assertOperatorEqualsWithStateComparison(operatorFactory, driverContext, input, expected, false, ImmutableList.of(), true, createExpectedMapping());
    }

    private Map<String, Object> createExpectedMapping()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        Map<String, Object> mergingOutputMapping = new HashMap<>();
        Map<String, Object> pageBuilderMapping = new HashMap<>();
        List<Map<String, Object>> blockBuildersMapping = new ArrayList<>();
        Map<String, Object> longArrayBlockBuilderMapping = new HashMap<>();
        Map<String, Object> variableWidthBlockBuilderMapping = new HashMap<>();
        Map<String, Object> longArrayBlockBuilderStatusMapping = new HashMap<>();
        Map<String, Object> variableWidthBlockBuilderStatusMapping = new HashMap<>();
        Map<String, Object> longArrayBlockBuilderStatusPageBuilderStatusMapping = new HashMap<>();

        expectedMapping.put("operatorContext", 0);
        expectedMapping.put("pageProcessorMemoryContext", 0L);
        expectedMapping.put("outputMemoryContext", 388L);
        expectedMapping.put("mergingOutput", mergingOutputMapping);
        expectedMapping.put("finishing", false);

        mergingOutputMapping.put("pageBuilder", pageBuilderMapping);
        mergingOutputMapping.put("finishing", false);
        pageBuilderMapping.put("blockBuilders", blockBuildersMapping);
        pageBuilderMapping.put("pageBuilderStatus", 0L);
        pageBuilderMapping.put("declaredPositions", 0);

        blockBuildersMapping.add(variableWidthBlockBuilderMapping);
        blockBuildersMapping.add(longArrayBlockBuilderMapping);

        variableWidthBlockBuilderMapping.put("blockBuilderStatus", variableWidthBlockBuilderStatusMapping);
        variableWidthBlockBuilderMapping.put("initialized", false);
        variableWidthBlockBuilderMapping.put("initialEntryCount", 8);
        variableWidthBlockBuilderMapping.put("initialSliceOutputSize", 256);
        variableWidthBlockBuilderMapping.put("sliceOutput", Collections.emptyList());
        variableWidthBlockBuilderMapping.put("hasNullValue", false);
        variableWidthBlockBuilderMapping.put("valueIsNull", Collections.emptyList());
        variableWidthBlockBuilderMapping.put("offsets", Arrays.asList(0));
        variableWidthBlockBuilderMapping.put("positions", 0);
        variableWidthBlockBuilderMapping.put("currentEntrySize", 0);
        variableWidthBlockBuilderMapping.put("arraysRetainedSizeInBytes", 36L);

        longArrayBlockBuilderMapping.put("blockBuilderStatus", longArrayBlockBuilderStatusMapping);
        longArrayBlockBuilderMapping.put("initialized", false);
        longArrayBlockBuilderMapping.put("positionCount", 0);
        longArrayBlockBuilderMapping.put("hasNullValue", false);
        longArrayBlockBuilderMapping.put("hasNonNullValue", false);
        longArrayBlockBuilderMapping.put("valueIsNull", Collections.emptyList());
        longArrayBlockBuilderMapping.put("values", Collections.emptyList());
        longArrayBlockBuilderMapping.put("retainedSizeInBytes", 128L);

        variableWidthBlockBuilderStatusMapping.put("pageBuilderStatus", 0L);
        variableWidthBlockBuilderStatusMapping.put("currentSize", 0);

        longArrayBlockBuilderStatusMapping.put("pageBuilderStatus", 0L);
        longArrayBlockBuilderStatusMapping.put("currentSize", 0);

        return expectedMapping;
    }

    @Test
    public void testMergeOutput()
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .addSequencePage(100, 0, 0)
                .addSequencePage(100, 0, 0)
                .addSequencePage(100, 0, 0)
                .addSequencePage(100, 0, 0)
                .build();

        RowExpression filter = call(EQUAL.getFunctionName().toString(),
                new BuiltInFunctionHandle(Signature.internalOperator(EQUAL, BOOLEAN.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()))),
                BOOLEAN,
                field(1, BIGINT),
                constant(10L, BIGINT));

        Metadata metadata = createTestMetadataManager();
        ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
        Supplier<PageProcessor> processor = compiler.compilePageProcessor(Optional.of(filter), ImmutableList.of(field(1, BIGINT)));

        OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                processor,
                ImmutableList.of(BIGINT),
                new DataSize(64, KILOBYTE),
                2);

        List<Page> expected = rowPagesBuilder(BIGINT)
                .row(10L)
                .row(10L)
                .row(10L)
                .row(10L)
                .build();

        assertOperatorEquals(operatorFactory, ImmutableList.of(BIGINT), driverContext, input, expected);
    }
}
