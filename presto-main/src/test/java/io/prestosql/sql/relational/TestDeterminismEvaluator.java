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
package io.prestosql.sql.relational;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.Signature.internalOperator;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDeterminismEvaluator
{
    @Test
    public void testDeterminismEvaluator()
    {
        RowExpressionDeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(createTestMetadataManager());

        CallExpression random = new CallExpression(QualifiedObjectName.valueOfDefaultFunction("random").getObjectName(),
                new BuiltInFunctionHandle(new Signature(
                        QualifiedObjectName.valueOfDefaultFunction("random"),
                        SCALAR,
                        parseTypeSignature(StandardTypes.BIGINT),
                        parseTypeSignature(StandardTypes.BIGINT))),
                BIGINT,
                singletonList(constant(10L, BIGINT)),
                Optional.empty());
        assertFalse(determinismEvaluator.isDeterministic(random));

        InputReferenceExpression col0 = field(0, BIGINT);
        Signature lessThan = internalOperator(LESS_THAN, BOOLEAN, ImmutableList.of(BIGINT, BIGINT));

        CallExpression lessThanExpression = new CallExpression(lessThan.getName().getObjectName(), new BuiltInFunctionHandle(lessThan), BOOLEAN, ImmutableList.of(col0, constant(10L, BIGINT)), Optional.empty());
        assertTrue(determinismEvaluator.isDeterministic(lessThanExpression));

        CallExpression lessThanRandomExpression = new CallExpression(lessThan.getName().getObjectName(), new BuiltInFunctionHandle(lessThan), BOOLEAN, ImmutableList.of(col0, random), Optional.empty());
        assertFalse(determinismEvaluator.isDeterministic(lessThanRandomExpression));
    }
}
