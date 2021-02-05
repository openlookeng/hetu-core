/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.plugin.mysql.optimization;

import io.prestosql.plugin.jdbc.optimization.BaseJdbcRowExpressionConverter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import static io.prestosql.plugin.mysql.optimization.MySqlPushDownUtils.getCastExpression;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.function.StandardFunctionUtils.isArrayConstructor;
import static io.prestosql.spi.function.StandardFunctionUtils.isCastFunction;
import static io.prestosql.spi.function.StandardFunctionUtils.isSubscriptFunction;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class MySqlRowExpressionConverter
        extends BaseJdbcRowExpressionConverter
{
    public MySqlRowExpressionConverter(RowExpressionService rowExpressionService)
    {
        super(rowExpressionService);
    }

    @Override
    public String visitCall(CallExpression call, Void context)
    {
        Signature signature = call.getSignature();
        if (isArrayConstructor(signature)) {
            throw new PrestoException(NOT_SUPPORTED, "MySql connector does not support array constructor");
        }
        if (isSubscriptFunction(signature)) {
            throw new PrestoException(NOT_SUPPORTED, "MySql connector does not support subscript expression");
        }
        if (isCastFunction(signature)) {
            // deal with literal, when generic literal expression translate to rowExpression, it will be
            // translated to a 'CAST' rowExpression with a varchar type 'CONSTANT' rowExpression, in some
            // case, 'CAST' is superfluous
            RowExpression argument = call.getArguments().get(0);
            Type type = call.getType();
            if (argument instanceof ConstantExpression && argument.getType().equals(VARCHAR)) {
                String value = argument.accept(this, null);
                if (type instanceof VarcharType
                        || type instanceof CharType
                        || type instanceof VarbinaryType
                        || type instanceof DecimalType
                        || type instanceof RealType
                        || type instanceof DoubleType) {
                    return value;
                }
            }
            if (call.getType().getDisplayName().equals(LIKE_PATTERN_NAME)) {
                return call.getArguments().get(0).accept(this, null);
            }
            return getCastExpression(call.getArguments().get(0).accept(this, null), call.getType());
        }
        return super.visitCall(call, context);
    }
}
