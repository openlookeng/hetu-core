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

package io.hetu.core.heuristicindex.util;

import io.airlift.log.Logger;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;

public class TypeUtils
{
    private static final Logger LOG = Logger.get(TypeUtils.class);

    private TypeUtils() {}

    public static Object extractSingleValue(Expression expression)
    {
        if (expression instanceof Cast) {
            return extractSingleValue(((Cast) expression).getExpression());
        }
        else if (expression instanceof BooleanLiteral) {
            return ((BooleanLiteral) expression).getValue();
        }
        else if (expression instanceof DecimalLiteral) {
            String value = ((DecimalLiteral) expression).getValue();
            return new BigDecimal(value);
        }
        else if (expression instanceof DoubleLiteral) {
            return ((DoubleLiteral) expression).getValue();
        }
        else if (expression instanceof LongLiteral) {
            return ((LongLiteral) expression).getValue();
        }
        else if (expression instanceof StringLiteral) {
            return ((StringLiteral) expression).getValue();
        }
        else if (expression instanceof TimeLiteral) {
            return ((TimeLiteral) expression).getValue();
        }
        else if (expression instanceof TimestampLiteral) {
            String value = ((TimestampLiteral) expression).getValue();
            return Timestamp.valueOf(value).getTime();
        }
        else if (expression instanceof GenericLiteral) {
            GenericLiteral genericLiteral = (GenericLiteral) expression;

            if (genericLiteral.getType().equalsIgnoreCase("bigint")) {
                return Long.valueOf(genericLiteral.getValue());
            }
            else if (genericLiteral.getType().equalsIgnoreCase("real")) {
                return (long) Float.floatToIntBits(Float.parseFloat(genericLiteral.getValue()));
            }
            else if (genericLiteral.getType().equalsIgnoreCase("tinyint")) {
                return Byte.valueOf(genericLiteral.getValue()).longValue();
            }
            else if (genericLiteral.getType().equalsIgnoreCase("smallint")) {
                return Short.valueOf(genericLiteral.getValue()).longValue();
            }
            else if (genericLiteral.getType().equalsIgnoreCase("date")) {
                return LocalDate.parse(genericLiteral.getValue()).toEpochDay();
            }
        }

        throw new UnsupportedOperationException("Not Implemented Exception: " + expression.toString());
    }
}
