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
package io.prestosql.sql.planner.sanity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.SqlFunctionHandle;
import io.prestosql.spi.function.SqlFunctionId;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.VariableReferenceSymbolConverter;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.StatisticAggregations;
import io.prestosql.sql.planner.plan.StatisticAggregationsDescriptor;
import io.prestosql.sql.planner.plan.TableDeleteNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.VacuumTableNode;
import io.prestosql.testing.TestingHandle;
import io.prestosql.testing.TestingMetadata;
import io.prestosql.testing.TestingTransactionHandle;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.plan.JoinNode.Type.INNER;
import static io.prestosql.spi.sql.expression.Types.FrameBoundType.CURRENT_ROW;
import static io.prestosql.spi.sql.expression.Types.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.relational.Expressions.call;

public class TestExternalFunctionPushDownChecker
        extends BasePlanTest
{
    private static final FunctionAndTypeManager FUNCTION_MANAGER = createTestMetadataManager().getFunctionAndTypeManager();
    private static final FunctionHandle SUM = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(DOUBLE));
    private static final FunctionHandle EXTERNAL_FOO = new SqlFunctionHandle(
            new SqlFunctionId(new QualifiedObjectName("jdbc", "v1", "foo"), fromTypes(DOUBLE).stream().map(TypeSignatureProvider::getTypeSignature).collect(Collectors.toList())),
            "v1");
    private static final TableHandle tableHandle = new TableHandle(
            new CatalogName("testConnector"),
            new TestingMetadata.TestingTableHandle(),
            TestingTransactionHandle.create(),
            Optional.of(TestingHandle.INSTANCE));
    private CallExpression sumCall;
    private CallExpression externalFooCall1;
    private CallExpression externalFooCall2;
    private PlanSymbolAllocator planSymbolAllocator;
    private Symbol columnA;
    private Symbol columnB;
    private Metadata metadata;
    private PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private PlanBuilder builder;

    @BeforeClass
    public void setup()
    {
        planSymbolAllocator = new PlanSymbolAllocator();
        columnA = planSymbolAllocator.newSymbol("a", DOUBLE);
        columnB = planSymbolAllocator.newSymbol("b", DOUBLE);
        sumCall = call("sum", SUM, DOUBLE, ImmutableList.of(VariableReferenceSymbolConverter.toVariableReference(columnA, DOUBLE)));
        externalFooCall1 = new CallExpression("jdbc.v1.foo", EXTERNAL_FOO, DOUBLE, ImmutableList.of());
        externalFooCall2 = new CallExpression("jdbc.v1.foo2", EXTERNAL_FOO, DOUBLE, ImmutableList.of());
        metadata = getQueryRunner().getMetadata();
        builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata);
    }

    @Test(expectedExceptions = ExternalFunctionPushDownChecker.IllegalExternalFunctionUsageException.class, expectedExceptionsMessageRegExp = "The external function jdbc.v1.foo does not support to push down to data source for this query.")
    public void testFilterNodeWithExternalCall()
    {
        PlanNode root = builder.filter(externalFooCall1, builder.values(columnA));
        validatePlan(root);
    }

    @Test(expectedExceptions = ExternalFunctionPushDownChecker.IllegalExternalFunctionUsageException.class, expectedExceptionsMessageRegExp = "The external function jdbc.v1.foo, jdbc.v1.foo2 does not support to push down to data source for this query.")
    public void testFilterNodeWithExternalCallMultiple()
    {
        PlanNode root = builder.filter(externalFooCall1, builder.filter(externalFooCall2, builder.values(columnA)));
        validatePlan(root);
    }

    @Test(expectedExceptions = ExternalFunctionPushDownChecker.IllegalExternalFunctionUsageException.class, expectedExceptionsMessageRegExp = "The external function jdbc.v1.foo does not support to push down to data source for this query.")
    public void testValueNodeWithExternalCall()
    {
        PlanNode root = builder.values(ImmutableList.of(columnA), ImmutableList.of(ImmutableList.of(externalFooCall1)));
        validatePlan(root);
    }

    @Test(expectedExceptions = ExternalFunctionPushDownChecker.IllegalExternalFunctionUsageException.class, expectedExceptionsMessageRegExp = "The external function jdbc.v1.foo does not support to push down to data source for this query.")
    public void testProjectNodeWithExternalCall()
    {
        PlanNode root = builder.project(Assignments.of(columnA, externalFooCall1), builder.values(columnA));
        validatePlan(root);
    }

    @Test(expectedExceptions = ExternalFunctionPushDownChecker.IllegalExternalFunctionUsageException.class, expectedExceptionsMessageRegExp = "The external function jdbc.v1.foo does not support to push down to data source for this query.")
    public void testWindowNodeWithExternalCall()
    {
        WindowNode.Frame frame = new WindowNode.Frame(
                Types.WindowFrameType.RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        PlanNode root = builder.window(new WindowNode.Specification(
                        ImmutableList.of(columnA),
                        Optional.empty()),
                ImmutableMap.of(columnB,
                        new WindowNode.Function(externalFooCall1, ImmutableList.of(), frame)),
                builder.values(columnA));
        validatePlan(root);
    }

    @Test(expectedExceptions = ExternalFunctionPushDownChecker.IllegalExternalFunctionUsageException.class, expectedExceptionsMessageRegExp = "The external function jdbc.v1.foo does not support to push down to data source for this query.")
    public void testJoinNodeWithExternalCall()
    {
        PlanNode root = builder.join(
                INNER,
                builder.values(columnA),
                builder.values(columnB),
                ImmutableList.of(new JoinNode.EquiJoinClause(columnB, columnA)),
                ImmutableList.of(columnB, columnA),
                Optional.of(externalFooCall1));
        validatePlan(root);
    }

    @Test(expectedExceptions = ExternalFunctionPushDownChecker.IllegalExternalFunctionUsageException.class, expectedExceptionsMessageRegExp = "The external function jdbc.v1.foo does not support to push down to data source for this query.")
    public void testSpatialJoinNodeWithExternalCall()
    {
        PlanNode node = new SpatialJoinNode(
                idAllocator.getNextId(),
                SpatialJoinNode.Type.INNER,
                builder.values(columnA),
                builder.values(columnB),
                ImmutableList.of(columnB, columnA),
                externalFooCall1,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        validatePlan(node);
    }

    @Test(expectedExceptions = ExternalFunctionPushDownChecker.IllegalExternalFunctionUsageException.class, expectedExceptionsMessageRegExp = "The external function jdbc.v1.foo does not support to push down to data source for this query.")
    public void testAggregationNodeWithExternalCall()
    {
        PlanNode root = builder.aggregation(a -> a
                .globalGrouping()
                .addAggregation(columnA, externalFooCall1)
                .source(
                        builder.values(columnB)));
        validatePlan(root);
    }

    @Test(expectedExceptions = ExternalFunctionPushDownChecker.IllegalExternalFunctionUsageException.class, expectedExceptionsMessageRegExp = "The external function jdbc.v1.foo does not support to push down to data source for this query.")
    public void testTableFinishNodeWithExternalCall()
    {
        TableWriterNode.DeleteTarget deleteTarget = new TableWriterNode.DeleteTarget(
                tableHandle,
                new SchemaTableName("sch", "tab"));
        PlanNode node = new TableFinishNode(
                idAllocator.getNextId(),
                builder.values(),
                deleteTarget,
                columnB,
                Optional.of(new StatisticAggregations(ImmutableMap.of(columnA, new AggregationNode.Aggregation(externalFooCall1, externalFooCall1.getArguments(), false, Optional.empty(), Optional.empty(), Optional.empty())), ImmutableList.of(columnA))),
                Optional.of(new StatisticAggregationsDescriptor<>(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of())));
        validatePlan(node);
    }

    @Test(expectedExceptions = ExternalFunctionPushDownChecker.IllegalExternalFunctionUsageException.class, expectedExceptionsMessageRegExp = "The external function jdbc.v1.foo does not support to push down to data source for this query.")
    public void testTableWriterNodeWithExternalCall()
    {
        TableWriterNode.DeleteTarget deleteTarget = new TableWriterNode.DeleteTarget(
                tableHandle,
                new SchemaTableName("sch", "tab"));
        PlanNode node = new TableWriterNode(
                idAllocator.getNextId(),
                builder.values(),
                deleteTarget,
                columnA,
                columnB,
                ImmutableList.of(columnA, columnB),
                ImmutableList.of("a", "b"),
                Optional.empty(),
                Optional.of(new StatisticAggregations(ImmutableMap.of(columnA, new AggregationNode.Aggregation(externalFooCall1, externalFooCall1.getArguments(), false, Optional.empty(), Optional.empty(), Optional.empty())), ImmutableList.of(columnA))),
                Optional.of(new StatisticAggregationsDescriptor<>(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of())));
        validatePlan(node);
    }

    @Test(expectedExceptions = ExternalFunctionPushDownChecker.IllegalExternalFunctionUsageException.class, expectedExceptionsMessageRegExp = "The external function jdbc.v1.foo does not support to push down to data source for this query.")
    public void testVacuumTableNodeWithExternalCall()
    {
        TableWriterNode.DeleteTarget deleteTarget = new TableWriterNode.DeleteTarget(
                tableHandle,
                new SchemaTableName("sch", "tab"));
        PlanNode node = new VacuumTableNode(
                idAllocator.getNextId(),
                tableHandle,
                deleteTarget,
                columnA,
                columnB,
                "p1",
                false,
                ImmutableList.of(),
                Optional.of(new StatisticAggregations(ImmutableMap.of(columnA, new AggregationNode.Aggregation(externalFooCall1, externalFooCall1.getArguments(), false, Optional.empty(), Optional.empty(), Optional.empty())), ImmutableList.of(columnA))),
                Optional.of(new StatisticAggregationsDescriptor<>(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of())));
        validatePlan(node);
    }

    @Test(expectedExceptions = ExternalFunctionPushDownChecker.IllegalExternalFunctionUsageException.class, expectedExceptionsMessageRegExp = "The external function jdbc.v1.foo does not support to push down to data source for this query.")
    public void testTableDeleteNodeWithExternalCall()
    {
        PlanNode node = new TableDeleteNode(
                idAllocator.getNextId(),
                builder.values(),
                Optional.of(externalFooCall1),
                tableHandle,
                ImmutableMap.of(),
                columnA);
        validatePlan(node);
    }

    @Test
    public void testFilterNode()
    {
        PlanNode root = builder.filter(sumCall, builder.values(columnA));
        validatePlan(root);
    }

    @Test
    public void testValueNode()
    {
        PlanNode root = builder.values(ImmutableList.of(columnA), ImmutableList.of(ImmutableList.of(sumCall)));
        validatePlan(root);
    }

    @Test
    public void testProjectNode()
    {
        PlanNode root = builder.project(Assignments.of(columnA, sumCall), builder.values(columnA));
        validatePlan(root);
    }

    @Test
    public void testWindowNode()
    {
        WindowNode.Frame frame = new WindowNode.Frame(
                Types.WindowFrameType.RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        PlanNode root = builder.window(new WindowNode.Specification(
                        ImmutableList.of(columnA),
                        Optional.empty()),
                ImmutableMap.of(columnB,
                        new WindowNode.Function(sumCall, ImmutableList.of(), frame)),
                builder.values(columnA));
        validatePlan(root);
    }

    @Test
    public void testJoinNode()
    {
        PlanNode root = builder.join(
                INNER,
                builder.values(columnA),
                builder.values(columnB),
                ImmutableList.of(new JoinNode.EquiJoinClause(columnB, columnA)),
                ImmutableList.of(columnB, columnA),
                Optional.of(sumCall));
        validatePlan(root);
    }

    @Test
    public void testSpatialJoinNode()
    {
        PlanNode node = new SpatialJoinNode(
                idAllocator.getNextId(),
                SpatialJoinNode.Type.INNER,
                builder.values(columnA),
                builder.values(columnB),
                ImmutableList.of(columnB, columnA),
                sumCall,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        validatePlan(node);
    }

    @Test
    public void testAggregationNode()
    {
        PlanNode root = builder.aggregation(a -> a
                .globalGrouping()
                .addAggregation(columnA, sumCall)
                .source(
                        builder.values(columnB)));
        validatePlan(root);
    }

    @Test
    public void testTableFinishNode()
    {
        TableWriterNode.DeleteTarget deleteTarget = new TableWriterNode.DeleteTarget(
                tableHandle,
                new SchemaTableName("sch", "tab"));
        PlanNode node = new TableFinishNode(
                idAllocator.getNextId(),
                builder.values(),
                deleteTarget,
                columnB,
                Optional.of(new StatisticAggregations(ImmutableMap.of(columnA, new AggregationNode.Aggregation(sumCall, sumCall.getArguments(), false, Optional.empty(), Optional.empty(), Optional.empty())), ImmutableList.of(columnA))),
                Optional.of(new StatisticAggregationsDescriptor<>(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of())));
        validatePlan(node);
    }

    @Test
    public void testTableWriterNode()
    {
        TableWriterNode.DeleteTarget deleteTarget = new TableWriterNode.DeleteTarget(
                tableHandle,
                new SchemaTableName("sch", "tab"));
        PlanNode node = new TableWriterNode(
                idAllocator.getNextId(),
                builder.values(),
                deleteTarget,
                columnA,
                columnB,
                ImmutableList.of(columnA, columnB),
                ImmutableList.of("a", "b"),
                Optional.empty(),
                Optional.of(new StatisticAggregations(ImmutableMap.of(columnA, new AggregationNode.Aggregation(sumCall, sumCall.getArguments(), false, Optional.empty(), Optional.empty(), Optional.empty())), ImmutableList.of(columnA))),
                Optional.of(new StatisticAggregationsDescriptor<>(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of())));
        validatePlan(node);
    }

    @Test
    public void testVacuumTableNode()
    {
        TableWriterNode.DeleteTarget deleteTarget = new TableWriterNode.DeleteTarget(
                tableHandle,
                new SchemaTableName("sch", "tab"));
        PlanNode node = new VacuumTableNode(
                idAllocator.getNextId(),
                tableHandle,
                deleteTarget,
                columnA,
                columnB,
                "p1",
                false,
                ImmutableList.of(),
                Optional.of(new StatisticAggregations(ImmutableMap.of(columnA, new AggregationNode.Aggregation(sumCall, sumCall.getArguments(), false, Optional.empty(), Optional.empty(), Optional.empty())), ImmutableList.of(columnA))),
                Optional.of(new StatisticAggregationsDescriptor<>(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of())));
        validatePlan(node);
    }

    @Test
    public void testTableDeleteNode()
    {
        PlanNode node = new TableDeleteNode(
                idAllocator.getNextId(),
                builder.values(),
                Optional.of(sumCall),
                tableHandle,
                ImmutableMap.of(),
                columnA);
        validatePlan(node);
    }

    private void validatePlan(PlanNode root)
    {
        getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            new ExternalFunctionPushDownChecker().validate(root, session, metadata, new TypeAnalyzer(new SqlParser(), metadata), TypeProvider.empty(), WarningCollector.NOOP);
            return null;
        });
    }
}
