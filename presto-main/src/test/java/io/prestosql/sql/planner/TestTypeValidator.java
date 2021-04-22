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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.sql.expression.Types.FrameBoundType;
import io.prestosql.spi.sql.expression.Types.WindowFrameType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.sanity.TypeValidator;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.Expression;
import io.prestosql.testing.TestingMetadata.TestingColumnHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.testing.TestingHandles.TEST_TABLE_HANDLE;

@Test(singleThreaded = true)
public class TestTypeValidator
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final TypeValidator TYPE_VALIDATOR = new TypeValidator();
    private static final FunctionAndTypeManager FUNCTION_MANAGER = createTestMetadataManager().getFunctionAndTypeManager();
    private static final FunctionHandle SUM = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(DOUBLE));

    private PlanSymbolAllocator planSymbolAllocator;
    private TableScanNode baseTableScan;
    private Symbol columnA;
    private Symbol columnB;
    private Symbol columnC;
    private Symbol columnD;
    private Symbol columnE;

    @BeforeMethod
    public void setUp()
    {
        planSymbolAllocator = new PlanSymbolAllocator();
        columnA = planSymbolAllocator.newSymbol("a", BIGINT);
        columnB = planSymbolAllocator.newSymbol("b", INTEGER);
        columnC = planSymbolAllocator.newSymbol("c", DOUBLE);
        columnD = planSymbolAllocator.newSymbol("d", DATE);
        columnE = planSymbolAllocator.newSymbol("e", VarcharType.createVarcharType(3));  // varchar(3), to test type only coercion

        Map<Symbol, ColumnHandle> assignments = ImmutableMap.<Symbol, ColumnHandle>builder()
                .put(columnA, new TestingColumnHandle("a"))
                .put(columnB, new TestingColumnHandle("b"))
                .put(columnC, new TestingColumnHandle("c"))
                .put(columnD, new TestingColumnHandle("d"))
                .put(columnE, new TestingColumnHandle("e"))
                .build();

        baseTableScan = new TableScanNode(
                newId(),
                TEST_TABLE_HANDLE,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                Optional.empty(),
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                new UUID(0, 0),
                0,
                false);
    }

    @Test
    public void testValidProject()
    {
        Expression expression1 = new Cast(toSymbolReference(columnB), StandardTypes.BIGINT);
        Expression expression2 = new Cast(toSymbolReference(columnC), StandardTypes.BIGINT);
        Assignments assignments = Assignments.builder()
                .put(planSymbolAllocator.newSymbol(expression1, BIGINT), castToRowExpression(expression1))
                .put(planSymbolAllocator.newSymbol(expression2, BIGINT), castToRowExpression(expression2))
                .build();
        PlanNode node = new ProjectNode(
                newId(),
                baseTableScan,
                assignments);

        assertTypesValid(node);
    }

    @Test
    public void testValidUnion()
    {
        Symbol outputSymbol = planSymbolAllocator.newSymbol("output", DATE);
        ListMultimap<Symbol, Symbol> mappings = ImmutableListMultimap.<Symbol, Symbol>builder()
                .put(outputSymbol, columnD)
                .put(outputSymbol, columnD)
                .build();

        PlanNode node = new UnionNode(
                newId(),
                ImmutableList.of(baseTableScan, baseTableScan),
                mappings,
                ImmutableList.copyOf(mappings.keySet()));

        assertTypesValid(node);
    }

    @Test
    public void testValidWindow()
    {
        Symbol windowSymbol = planSymbolAllocator.newSymbol("sum", DOUBLE);
        FunctionHandle functionHandle = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(DOUBLE));

        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrameType.RANGE,
                FrameBoundType.UNBOUNDED_PRECEDING,
                Optional.empty(),
                FrameBoundType.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(
                call("sum", functionHandle, DOUBLE, ImmutableList.of(VariableReferenceSymbolConverter.toVariableReference(columnC, DOUBLE))),
                ImmutableList.of(VariableReferenceSymbolConverter.toVariableReference(columnC, DOUBLE)),
                frame);

        WindowNode.Specification specification = new WindowNode.Specification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
                newId(),
                baseTableScan,
                specification,
                ImmutableMap.of(windowSymbol, function),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        assertTypesValid(node);
    }

    @Test
    public void testValidAggregation()
    {
        Symbol aggregationSymbol = planSymbolAllocator.newSymbol("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationSymbol, new Aggregation(
                        new CallExpression("sum",
                                SUM,
                                DOUBLE,
                                ImmutableList.of(castToRowExpression(toSymbolReference(columnC)))),
                        ImmutableList.of(castToRowExpression(toSymbolReference(columnC))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnA, columnB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                AggregationNode.AggregationType.HASH,
                Optional.empty());

        assertTypesValid(node);
    }

    @Test
    public void testValidTypeOnlyCoercion()
    {
        Expression expression = new Cast(toSymbolReference(columnB), StandardTypes.BIGINT);
        Assignments assignments = Assignments.builder()
                .put(planSymbolAllocator.newSymbol(expression, BIGINT), castToRowExpression(expression))
                .put(planSymbolAllocator.newSymbol(toSymbolReference(columnE), VARCHAR), castToRowExpression(toSymbolReference(columnE))) // implicit coercion from varchar(3) to varchar
                .build();
        PlanNode node = new ProjectNode(newId(), baseTableScan, assignments);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of symbol 'expr(_[0-9]+)?' is expected to be bigint, but the actual type is integer")
    public void testInvalidProject()
    {
        Expression expression1 = new Cast(toSymbolReference(columnB), StandardTypes.INTEGER);
        Expression expression2 = new Cast(toSymbolReference(columnA), StandardTypes.INTEGER);
        Assignments assignments = Assignments.builder()
                .put(planSymbolAllocator.newSymbol(expression1, BIGINT), castToRowExpression(expression1)) // should be INTEGER
                .put(planSymbolAllocator.newSymbol(expression1, INTEGER), castToRowExpression(expression2))
                .build();
        PlanNode node = new ProjectNode(
                newId(),
                baseTableScan,
                assignments);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of symbol 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidAggregationFunctionCall()
    {
        Symbol aggregationSymbol = planSymbolAllocator.newSymbol("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationSymbol, new Aggregation(
                        new CallExpression(
                                "sum",
                                SUM,
                                DOUBLE,
                                ImmutableList.of(castToRowExpression(toSymbolReference(columnA)))),
                        ImmutableList.of(castToRowExpression(toSymbolReference(columnA))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnA, columnB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                AggregationNode.AggregationType.HASH,
                Optional.empty());

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of symbol 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidAggregationFunctionSignature()
    {
        Symbol aggregationSymbol = planSymbolAllocator.newSymbol("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationSymbol, new Aggregation(
                        new CallExpression(
                                "sum",
                                FUNCTION_MANAGER.lookupFunction("sum", fromTypes(BIGINT)), // should be DOUBLE
                                DOUBLE,
                                ImmutableList.of(castToRowExpression(toSymbolReference(columnC)))),
                        ImmutableList.of(castToRowExpression(toSymbolReference(columnC))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnA, columnB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                AggregationNode.AggregationType.HASH,
                Optional.empty());

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of symbol 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidWindowFunctionCall()
    {
        Symbol windowSymbol = planSymbolAllocator.newSymbol("sum", DOUBLE);
        FunctionHandle functionHandle = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(DOUBLE));

        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrameType.RANGE,
                FrameBoundType.UNBOUNDED_PRECEDING,
                Optional.empty(),
                FrameBoundType.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(
                call("sum", functionHandle, BIGINT, ImmutableList.of(VariableReferenceSymbolConverter.toVariableReference(columnA, BIGINT))),
                ImmutableList.of(VariableReferenceSymbolConverter.toVariableReference(columnA, BIGINT)),
                frame);

        WindowNode.Specification specification = new WindowNode.Specification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
                newId(),
                baseTableScan,
                specification,
                ImmutableMap.of(windowSymbol, function),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of symbol 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint")
    public void testInvalidWindowFunctionSignature()
    {
        Symbol windowSymbol = planSymbolAllocator.newSymbol("sum", DOUBLE);
        FunctionHandle functionHandle = FUNCTION_MANAGER.lookupFunction("sum", fromTypes(BIGINT)); // should be DOUBLE

        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrameType.RANGE,
                FrameBoundType.UNBOUNDED_PRECEDING,
                Optional.empty(),
                FrameBoundType.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(
                call("sum", functionHandle, BIGINT, ImmutableList.of(VariableReferenceSymbolConverter.toVariableReference(columnC, DOUBLE))),
                ImmutableList.of(VariableReferenceSymbolConverter.toVariableReference(columnC, DOUBLE)),
                frame);

        WindowNode.Specification specification = new WindowNode.Specification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
                newId(),
                baseTableScan,
                specification,
                ImmutableMap.of(windowSymbol, function),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        assertTypesValid(node);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "type of symbol 'output(_[0-9]+)?' is expected to be date, but the actual type is bigint")
    public void testInvalidUnion()
    {
        Symbol outputSymbol = planSymbolAllocator.newSymbol("output", DATE);
        ListMultimap<Symbol, Symbol> mappings = ImmutableListMultimap.<Symbol, Symbol>builder()
                .put(outputSymbol, columnD)
                .put(outputSymbol, columnA) // should be a symbol with DATE type
                .build();

        PlanNode node = new UnionNode(
                newId(),
                ImmutableList.of(baseTableScan, baseTableScan),
                mappings,
                ImmutableList.copyOf(mappings.keySet()));

        assertTypesValid(node);
    }

    private void assertTypesValid(PlanNode node)
    {
        Metadata metadata = createTestMetadataManager();
        TYPE_VALIDATOR.validate(node, TEST_SESSION, metadata, new TypeAnalyzer(SQL_PARSER, metadata), planSymbolAllocator.getTypes(), WarningCollector.NOOP);
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }
}
