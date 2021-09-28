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
package io.hetu.core;

import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.function.Signature.internalOperator;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class HeuristicIndexTestUtils
{
    /**
     * construct a CallExpression instance for Operator function
     */
    public static CallExpression simplePredicate(OperatorType operatorType, String name, Type type, Object value)
    {
        Signature sig = internalOperator(operatorType, BOOLEAN.getTypeSignature(), type.getTypeSignature());
        VariableReferenceExpression varRef = new VariableReferenceExpression(name, VARCHAR);
        ConstantExpression constantExpression = new ConstantExpression(value, type);
        List<RowExpression> arguments = new ArrayList<>(2);
        arguments.add(varRef);
        arguments.add(constantExpression);
        return new CallExpression(operatorType.name(), new BuiltInFunctionHandle(sig), BOOLEAN, arguments, Optional.empty());
    }

    private HeuristicIndexTestUtils()
    {
    }
}
