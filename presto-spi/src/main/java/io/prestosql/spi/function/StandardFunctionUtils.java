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
package io.prestosql.spi.function;

import io.prestosql.spi.sql.expression.Time;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.spi.function.Signature.unmangleOperator;

public class StandardFunctionUtils
{
    private static final String OPERATOR_PREFIX = "$operator$";
    private static final Set<String> timeExtractFields = Arrays.stream(Time.ExtractField.values())
            .map(Time.ExtractField::name)
            .map(String::toLowerCase)
            .collect(toImmutableSet());

    private StandardFunctionUtils() {}

    public static boolean isNotFunction(Signature signature)
    {
        return signature.getName().equalsIgnoreCase("NOT");
    }

    public static boolean isLikeFunction(Signature signature)
    {
        return signature.getName().equalsIgnoreCase("LIKE");
    }

    public static boolean isOperator(Signature signature)
    {
        return signature.getName().startsWith(OPERATOR_PREFIX);
    }

    public static boolean isCastFunction(Signature signature)
    {
        return isOperator(signature) && OperatorType.CAST.equals(unmangleOperator(signature.getName()));
    }

    public static boolean isArithmeticFunction(Signature signature)
    {
        return isOperator(signature) && unmangleOperator(signature.getName()).isArithmeticOperator();
    }

    public static boolean isNegateFunction(Signature signature)
    {
        return isOperator(signature) && OperatorType.NEGATION.equals(unmangleOperator(signature.getName()));
    }

    public static boolean isComparisonFunction(Signature signature)
    {
        return isOperator(signature) && unmangleOperator(signature.getName()).isComparisonOperator();
    }

    public static boolean isArrayConstructor(Signature signature)
    {
        return signature.getName().equalsIgnoreCase("ARRAY_CONSTRUCTOR");
    }

    public static boolean isSubscriptFunction(Signature signature)
    {
        return isOperator(signature) && OperatorType.SUBSCRIPT.equals(unmangleOperator(signature.getName()));
    }

    public static boolean isTryFunction(Signature signature)
    {
        return signature.getName().equalsIgnoreCase("try");
    }

    public static boolean isTimeExtractFunction(Signature signature)
    {
        return timeExtractFields.contains(signature.getName().toLowerCase(Locale.ENGLISH));
    }
}
