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
package io.prestosql.spi.sql;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpressionVisitor;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;

/**
 * Convert RowExpression to a formatted String used for make a sql statement in PushDown
 */
public interface RowExpressionConverter<C>
        extends RowExpressionVisitor<String, C>
{
    @Override
    default String visitCall(CallExpression call, C context)
    {
        throw new PrestoException(NOT_SUPPORTED, "Not support convert CallExpression");
    }

    @Override
    default String visitSpecialForm(SpecialForm specialForm, C context)
    {
        throw new PrestoException(NOT_SUPPORTED, "Not support convert SpecialForm");
    }

    @Override
    default String visitConstant(ConstantExpression literal, C context)
    {
        throw new PrestoException(NOT_SUPPORTED, "Not support convert ConstantExpression");
    }

    @Override
    default String visitVariableReference(VariableReferenceExpression reference, C context)
    {
        throw new PrestoException(NOT_SUPPORTED, "Not support convert VariableReference");
    }

    @Override
    default String visitInputReference(InputReferenceExpression reference, C context)
    {
        throw new PrestoException(NOT_SUPPORTED, "Not support convert InputReference");
    }

    @Override
    default String visitLambda(LambdaDefinitionExpression lambda, C context)
    {
        throw new PrestoException(NOT_SUPPORTED, "Not support convert Lambda");
    }
}
