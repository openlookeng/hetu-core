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

package io.hetu.core.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.sql.expression.OrderBy;
import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.spi.sql.expression.Time;
import io.prestosql.sql.builder.BaseSqlQueryWriter;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

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

/**
 * Implementation of BaseSqlQueryWriter. It knows how to write
 * Oracle SQL for the Hetu's logical plan.
 *
 * @since 2019-07-18
 */

public class OracleSqlQueryWriter
        extends BaseSqlQueryWriter
{
    private static final char SINGLE_QUOTE = '\'';

    private static final int VARIABLE_ARGUMENTS = -1;

    private static final String CHAR_TYPE_PREFIX = "char(";

    private static final String DECIMAL_TYPE_PREFIX = "decimal(";

    private static final String VARCHAR_TYPE_PREFIX = "varchar(";

    private static final Map<String, Integer> BLACKLISTED_FUNCTIONS;

    OracleSqlQueryWriter()
    {
        super(BLACKLISTED_FUNCTIONS);
    }

    private static boolean isStringLiteral(String expression)
    {
        char first = expression.charAt(0);
        char last = expression.charAt(expression.length() - 1);
        // In Hetu, identifier names can be surrounded byt double quotes
        return first == SINGLE_QUOTE && last == SINGLE_QUOTE;
    }

    @Override
    public String lambdaArgumentDeclaration(String identifier)
    {
        throw new UnsupportedOperationException("Oracle Connector does not support lambda");
    }

    @Override
    public String lambdaExpression(List<String> arguments, String body)
    {
        throw new UnsupportedOperationException("Oracle Connector does not support lambda");
    }

    @Override
    public String decimalLiteral(String value)
    {
        return "'" + value + "'";
    }

    @Override
    public String arrayConstructor(List<String> values)
    {
        throw new UnsupportedOperationException("Oracle connector does not support array constructor");
    }

    @Override
    public String subscriptExpression(String base, String index)
    {
        throw new UnsupportedOperationException("Oracle connector does not support subscript expression");
    }

    @Override
    public String genericLiteral(String type, String value)
    {
        // https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements003.htm
        String lowerType = type.toLowerCase(Locale.ENGLISH);
        switch (lowerType) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case REAL:
            case DOUBLE:
                return value;

            case VARCHAR:
            case CHAR:
            case VARBINARY:
                return stringLiteral(value);

            case DATE:
                return lowerType + " " + stringLiteral(value);

            default:
                if (lowerType.startsWith(DECIMAL_TYPE_PREFIX)) {
                    return value;
                }
                else if (lowerType.startsWith(VARCHAR_TYPE_PREFIX)) {
                    return stringLiteral(value);
                }
                else if (lowerType.startsWith(CHAR_TYPE_PREFIX)) {
                    return stringLiteral(value);
                }
                // TIMESTAMP, and TIMESTAMP WITH TIME ZONE requires time format which is not available
                throw new UnsupportedOperationException("Oracle does not support the type " + type);
        }
    }

    @Override
    public String toNativeType(String type)
    {
        String lowerType = type.toLowerCase(Locale.ENGLISH);
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
                throw new UnsupportedOperationException("Oracle does not support the type " + type);
        }
    }

    @Override
    public String cast(String expression, String type, boolean isSafe, boolean isTypeOnly)
    {
        String newType = type;
        if (type.toLowerCase(Locale.ENGLISH).startsWith(CHAR_TYPE_PREFIX) && isStringLiteral(expression)) {
            // CAST('57834' AS char(10)) returns '57834     ' which cause to equality mismatch in Hetu
            // If the data type is char(<fixed-length>), the following logic makes sure that the <fixed-length>
            // is equal to the length of the input
            final int lengthOfQuotes = 2;
            newType = CHAR_TYPE_PREFIX + (expression.length() - lengthOfQuotes) + ")";
        }
        return super.cast(expression, newType, isSafe, isTypeOnly);
    }

    @Override
    public String functionCall(QualifiedName name, boolean isDistinct, List<String> argumentsList,
            Optional<String> orderBy, Optional<String> filter, Optional<String> window)
    {
        String functionName = name.toString();
        final int noOfArgs = argumentsList.size();
        final boolean isDecorated = window.isPresent() || filter.isPresent() || orderBy.isPresent();
        if (noOfArgs == 1 && !isDecorated && !isDistinct && getExtractFieldMap().contains(functionName.toUpperCase(Locale.ENGLISH))) {
            try {
                Time.ExtractField field = Time.ExtractField.valueOf(functionName.toUpperCase(Locale.ENGLISH));
                return extract(argumentsList.get(0), field);
            }
            catch (IllegalArgumentException ignored) {
                throw new IllegalArgumentException("Illegal argument: ", ignored);
            }
        }
        if ("at_timezone".equals(functionName) && noOfArgs == 2) {
            return this.atTimeZone(argumentsList.get(0), argumentsList.get(1));
        }
        return super.functionCall(name, isDistinct, argumentsList, orderBy, filter, window);
    }

    private Set<String> getExtractFieldMap()
    {
        Set<String> set = new HashSet<>();
        Time.ExtractField[] fields = Time.ExtractField.values();
        for (Time.ExtractField field : fields) {
            set.add(field.name());
        }
        return set;
    }

    /**
     * isBlacklistedFunction
     *
     * @param qualifiedName qualifiedName
     * @param noOfArgs noOfArgs
     * @return
     */
    @Override
    public boolean isBlacklistedFunction(String qualifiedName, int noOfArgs)
    {
        return false;
    }

    @Override
    public String select(List<Selection> symbols, String from)
    {
        StringJoiner selection = new StringJoiner(", ");
        for (Selection symbol : symbols) {
            if (symbol.getAlias()
                    .toLowerCase(Locale.ENGLISH)
                    .equals(symbol.getExpression().toLowerCase(Locale.ENGLISH))) {
                selection.add(symbol.getExpression());
            }
            else {
                selection.add(symbol.getExpression() + " AS " + symbol.getAlias());
            }
        }
        return "(SELECT " + selection.toString() + " FROM " + from + ")";
    }

    @Override
    public String limit(List<Selection> symbols, long count, String from)
    {
        return select(symbols, from + " WHERE ROWNUM <= " + count);
    }

    @Override
    public String topN(List<Selection> symbols, List<OrderBy> orderings, long count, String from)
    {
        return limit(symbols, count, sort(symbols, orderings, from));
    }

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
        builder.put("concat", VARIABLE_ARGUMENTS);
        BLACKLISTED_FUNCTIONS = builder.build();
    }
}
