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
package io.prestosql.sql;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.sql.relational.optimizer.ExpressionOptimizer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.block.BlockAssertions.toValues;
import static io.prestosql.metadata.CastType.CAST;
import static io.prestosql.metadata.CastType.JSON_TO_ARRAY_CAST;
import static io.prestosql.metadata.CastType.JSON_TO_MAP_CAST;
import static io.prestosql.metadata.CastType.JSON_TO_ROW_CAST;
import static io.prestosql.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.Signature.internalOperator;
import static io.prestosql.spi.relation.SpecialForm.Form.IF;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertEquals;

public class TestExpressionOptimizer
{
    private FunctionAndTypeManager functionAndTypeManager;
    private ExpressionOptimizer optimizer;

    @BeforeClass
    public void setUp()
    {
        functionAndTypeManager = createTestFunctionAndTypeManager();
        optimizer = new ExpressionOptimizer(createTestMetadataManager().getFunctionAndTypeManager(), TEST_SESSION);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        optimizer = null;
    }

    @Test(timeOut = 10_000)
    public void testPossibleExponentialOptimizationTime()
    {
        RowExpression expression = constant(1L, BIGINT);
        for (int i = 0; i < 100; i++) {
            Signature signature = internalOperator(OperatorType.ADD, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT));

            expression = new CallExpression(signature.getName().getObjectName(), new BuiltInFunctionHandle(signature), BIGINT, ImmutableList.of(expression, constant(1L, BIGINT)), Optional.empty());
        }
        optimizer.optimize(expression);
    }

    @Test
    public void testIfConstantOptimization()
    {
        assertEquals(optimizer.optimize(ifExpression(constant(true, BOOLEAN), 1L, 2L)), constant(1L, BIGINT));
        assertEquals(optimizer.optimize(ifExpression(constant(false, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));
        assertEquals(optimizer.optimize(ifExpression(constant(null, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));

        FunctionHandle bigintEquals = functionAndTypeManager.resolveOperatorFunctionHandle(EQUAL, fromTypes(BIGINT, BIGINT));
        RowExpression condition = new CallExpression(EQUAL.name(), bigintEquals, BOOLEAN, ImmutableList.of(constant(3L, BIGINT), constant(3L, BIGINT)), Optional.empty());
        assertEquals(optimizer.optimize(ifExpression(condition, 1L, 2L)), constant(1L, BIGINT));
    }

    @Test
    public void testCastWithJsonParseOptimization()
    {
        FunctionHandle jsonParseFunctionHandle = functionAndTypeManager.lookupFunction("json_parse", fromTypes(VARCHAR));

        // constant
        FunctionHandle jsonCastFunctionHandle = functionAndTypeManager.lookupCast(CAST, JSON.getTypeSignature(), parseTypeSignature("array(integer)"));
        RowExpression jsonCastExpression = new CallExpression(CAST.name(), jsonCastFunctionHandle, new ArrayType(INTEGER), ImmutableList.of(call("json_parse", jsonParseFunctionHandle, JSON, constant(utf8Slice("[1, 2]"), VARCHAR))), Optional.empty());
        RowExpression resultExpression = optimizer.optimize(jsonCastExpression);
        assertInstanceOf(resultExpression, ConstantExpression.class);
        Object resultValue = ((ConstantExpression) resultExpression).getValue();
        assertInstanceOf(resultValue, IntArrayBlock.class);
        assertEquals(toValues(INTEGER, (IntArrayBlock) resultValue), ImmutableList.of(1, 2));

        // varchar to array
        jsonCastFunctionHandle = functionAndTypeManager.lookupCast(CAST, JSON.getTypeSignature(), parseTypeSignature("array(varchar)"));
        jsonCastExpression = call(CAST.name(), jsonCastFunctionHandle, new ArrayType(VARCHAR), ImmutableList.of(call("json_parse", jsonParseFunctionHandle, JSON, field(1, VARCHAR))));
        resultExpression = optimizer.optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(JSON_TO_ARRAY_CAST.name(), functionAndTypeManager.lookupCast(JSON_TO_ARRAY_CAST, VARCHAR.getTypeSignature(), parseTypeSignature("array(varchar)")), new ArrayType(VARCHAR), field(1, VARCHAR)));

        // varchar to row
        jsonCastFunctionHandle = functionAndTypeManager.lookupCast(CAST, JSON.getTypeSignature(), parseTypeSignature("row(varchar,bigint)"));
        jsonCastExpression = call(CAST.name(), jsonCastFunctionHandle, RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)), ImmutableList.of(call("json_parse", jsonParseFunctionHandle, JSON, field(1, VARCHAR))));
        resultExpression = optimizer.optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(JSON_TO_ROW_CAST.name(), functionAndTypeManager.lookupCast(JSON_TO_ROW_CAST, VARCHAR.getTypeSignature(), parseTypeSignature("row(varchar,bigint)")), RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)), field(1, VARCHAR)));

        // varchar to map
        jsonCastFunctionHandle = functionAndTypeManager.lookupCast(CAST, JSON.getTypeSignature(), parseTypeSignature("map(integer,varchar)"));
        jsonCastExpression = call(CAST.name(), jsonCastFunctionHandle, mapType(INTEGER, VARCHAR), ImmutableList.of(call("json_parse", jsonParseFunctionHandle, JSON, field(1, VARCHAR))));
        resultExpression = optimizer.optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(JSON_TO_MAP_CAST.name(), functionAndTypeManager.lookupCast(JSON_TO_MAP_CAST, VARCHAR.getTypeSignature(), parseTypeSignature("map(integer, varchar)")), mapType(INTEGER, VARCHAR), field(1, VARCHAR)));
    }

    private static RowExpression ifExpression(RowExpression condition, long trueValue, long falseValue)
    {
        return new SpecialForm(IF, BIGINT, ImmutableList.of(condition, constant(trueValue, BIGINT), constant(falseValue, BIGINT)));
    }
}
