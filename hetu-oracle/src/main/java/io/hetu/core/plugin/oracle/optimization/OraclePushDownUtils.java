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
package io.hetu.core.plugin.oracle.optimization;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_QUERY_GENERATOR_FAILURE;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.CHAR;
import static io.prestosql.spi.type.StandardTypes.DATE;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.REAL;
import static io.prestosql.spi.type.StandardTypes.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.StandardTypes.TINYINT;
import static io.prestosql.spi.type.StandardTypes.VARBINARY;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class OraclePushDownUtils
{
    private static final char SINGLE_QUOTE = '\'';
    private static final String CHAR_TYPE_PREFIX = "char(";
    private static final String DECIMAL_TYPE_PREFIX = "decimal(";
    private static final String VARCHAR_TYPE_PREFIX = "varchar(";

    private OraclePushDownUtils() {}

    public static String getCastExpression(String expression, Type type)
    {
        String typeName = type.getDisplayName().toLowerCase(ENGLISH);
        if (typeName.startsWith(CHAR_TYPE_PREFIX) && isStringLiteral(expression)) {
            // CAST('57834' AS char(10)) returns '57834     ' which cause to equality mismatch in Hetu
            // If the data type is char(<fixed-length>), the following logic makes sure that the <fixed-length>
            // is equal to the length of the input
            final int lengthOfQuotes = 2;
            typeName = CHAR_TYPE_PREFIX + (expression.length() - lengthOfQuotes) + ")";
        }
        return format("CAST(%s AS %s)", expression, toNativeType(typeName));
    }

    public static String toNativeType(String type)
    {
        String lowerType = type.toLowerCase(ENGLISH);
        switch (lowerType) {
            case TINYINT:
                return "number(3)";

            case SMALLINT:
                return "number(5)";

            case INTEGER:
                return "number(10)";

            case BIGINT:
                return "number(19)";

            case REAL:
                return "binary_float";

            case DOUBLE:
                return "binary_double";

            case VARCHAR:
                return "nclob";

            case VARBINARY:
                return "blob";

            case TIMESTAMP:
                return "timestamp(3)";

            case TIMESTAMP_WITH_TIME_ZONE:
                return "timestamp(3) with time zone";

            case CHAR:
            case DATE:
                return lowerType;

            default:
                if (lowerType.startsWith(DECIMAL_TYPE_PREFIX)) {
                    return lowerType.replace("decimal", "number");
                }
                else if (lowerType.startsWith(VARCHAR_TYPE_PREFIX)) {
                    return lowerType.replace("varchar", "varchar2");
                }
                else if (lowerType.startsWith(CHAR_TYPE_PREFIX)) {
                    return lowerType;
                }
                throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, "Oracle does not support the type " + type);
        }
    }

    private static boolean isStringLiteral(String expression)
    {
        char first = expression.charAt(0);
        char last = expression.charAt(expression.length() - 1);
        // In Hetu, identifier names can be surrounded byt double quotes
        return first == SINGLE_QUOTE && last == SINGLE_QUOTE;
    }
}
