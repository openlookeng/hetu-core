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
package io.prestosql.plugin.mysql.optimization;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_QUERY_GENERATOR_FAILURE;
import static java.lang.String.format;

public class MySqlPushDownUtils
{
    private MySqlPushDownUtils() {}

    public static String getCastExpression(String expression, Type type)
    {
        // My Sql only support cast type:[DATE, DATETIME, TIME, CHAR, SIGNED, UNSIGNED, BINARY, DECIMAL]
        return format("CAST(%s AS %s)", expression, toNativeType(type));
    }

    public static String toNativeType(Type type)
    {
        if (type instanceof TinyintType
                || type instanceof SmallintType
                || type instanceof IntegerType
                || type instanceof BigintType) {
            return "SIGNED";
        }
        if (type instanceof CharType || type instanceof VarcharType) {
            return "CHAR";
        }
        if (type instanceof VarbinaryType) {
            return "BINARY";
        }
        if (type instanceof DateType) {
            return "DATE";
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return format("DECIMAL(%d, %d)", decimalType.getPrecision(), decimalType.getScale());
        }
        throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, "Mysql does not support cast the type " + type);
    }
}
