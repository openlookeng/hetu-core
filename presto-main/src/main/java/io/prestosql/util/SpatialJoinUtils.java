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
package io.prestosql.util;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.analyzer.TypeSignatureProvider;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.expressions.LogicalRowExpressions.extractConjuncts;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;

public class SpatialJoinUtils
{
    public static final String ST_CONTAINS = "st_contains";
    public static final String ST_WITHIN = "st_within";
    public static final String ST_INTERSECTS = "st_intersects";
    public static final String ST_DISTANCE = "st_distance";

    private SpatialJoinUtils() {}

    /**
     * Returns a subset of conjuncts matching one of the following shapes:
     * - ST_Contains(...)
     * - ST_Within(...)
     * - ST_Intersects(...)
     * <p>
     * Doesn't check or guarantee anything about function arguments.
     */
    public static List<CallExpression> extractSupportedSpatialFunctions(RowExpression filterExpression, FunctionAndTypeManager functionAndTypeManager)
    {
        return extractConjuncts(filterExpression).stream()
                .filter(CallExpression.class::isInstance)
                .map(CallExpression.class::cast)
                .filter(call -> isSupportedSpatialFunction(call, functionAndTypeManager))
                .collect(toImmutableList());
    }

    private static boolean isSupportedSpatialFunction(CallExpression call, FunctionAndTypeManager functionAndTypeManager)
    {
        String functionName = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle()).getName().getObjectName();
        return functionName.equalsIgnoreCase(ST_CONTAINS) || functionName.equalsIgnoreCase(ST_WITHIN)
                || functionName.equalsIgnoreCase(ST_INTERSECTS);
    }

    /**
     * Returns a subset of conjuncts matching one the following shapes:
     * - ST_Distance(...) <= ...
     * - ST_Distance(...) < ...
     * - ... >= ST_Distance(...)
     * - ... > ST_Distance(...)
     * <p>
     * Doesn't check or guarantee anything about ST_Distance functions arguments
     * or the other side of the comparison.
     */
    public static List<CallExpression> extractSupportedSpatialComparisons(RowExpression filterExpression, FunctionAndTypeManager functionAndTypeManager)
    {
        return extractConjuncts(filterExpression).stream()
                .filter(CallExpression.class::isInstance)
                .map(CallExpression.class::cast)
                .filter(call -> isSupportedSpatialComparison(call, functionAndTypeManager))
                .collect(toImmutableList());
    }

    private static boolean isSupportedSpatialComparison(CallExpression expression, FunctionAndTypeManager functionAndTypeManager)
    {
        String functionName = functionAndTypeManager.getFunctionMetadata(expression.getFunctionHandle()).getName().getObjectName();
        if (!Signature.isMangleOperator(functionName)) {
            return false;
        }
        OperatorType operatorType = Signature.unmangleOperator(functionName);
        if (operatorType.equals(LESS_THAN) || operatorType.equals(LESS_THAN_OR_EQUAL)) {
            return isSTDistance(expression.getArguments().get(0), functionAndTypeManager);
        }

        if (operatorType.equals(GREATER_THAN) || operatorType.equals(GREATER_THAN_OR_EQUAL)) {
            return isSTDistance(expression.getArguments().get(1), functionAndTypeManager);
        }
        return false;
    }

    private static boolean isSTDistance(RowExpression expression, FunctionAndTypeManager functionAndTypeManager)
    {
        if (expression instanceof CallExpression) {
            return (functionAndTypeManager.getFunctionMetadata(((CallExpression) expression).getFunctionHandle())).getName().getObjectName().equalsIgnoreCase(ST_DISTANCE);
        }
        return false;
    }

    public static FunctionHandle getFlippedFunctionHandle(CallExpression callExpression, FunctionAndTypeManager functionAndTypeManager)
    {
        FunctionMetadata callExpressionMetadata = functionAndTypeManager.getFunctionMetadata(callExpression.getFunctionHandle());
        checkArgument(callExpressionMetadata.getOperatorType().isPresent());
        OperatorType operatorType = flip(callExpressionMetadata.getOperatorType().get());
        List<TypeSignatureProvider> typeProviderList = fromTypes(callExpression.getArguments().stream().map(RowExpression::getType).collect(toImmutableList()));
        checkArgument(typeProviderList.size() == 2, "Expected there to be only two arguments in type provider");
        return functionAndTypeManager.resolveOperatorFunctionHandle(
                operatorType,
                ImmutableList.of(typeProviderList.get(1), typeProviderList.get(0)));
    }

    public static OperatorType flip(OperatorType operatorType)
    {
        switch (operatorType) {
            case EQUAL:
                return EQUAL;
            case NOT_EQUAL:
                return NOT_EQUAL;
            case LESS_THAN:
                return GREATER_THAN;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN_OR_EQUAL;
            case GREATER_THAN:
                return LESS_THAN;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN_OR_EQUAL;
            case IS_DISTINCT_FROM:
                return IS_DISTINCT_FROM;
            default:
                throw new IllegalArgumentException("Unsupported comparison: " + operatorType);
        }
    }
}
