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
package io.prestosql.sql.builder;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.sql.SqlQueryWriter;
import io.prestosql.spi.sql.expression.Operators;
import io.prestosql.spi.sql.expression.OrderBy;
import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.spi.sql.expression.Time;
import io.prestosql.spi.sql.expression.Types;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.StringJoiner;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class BaseSqlQueryWriter
        implements SqlQueryWriter
{
    private static final String INTERNAL_FUNCTION_PREFIX = "$";
    private static final String DYNAMIC_FILTER_FUNCTION_NAME = "$internal$dynamic_filter_function";

    private final ThreadLocal<DecimalFormat> doubleFormatter = ThreadLocal.withInitial(
            () -> new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));
    private final Map<String, Integer> blacklistedFunctions;

    public BaseSqlQueryWriter()
    {
        this(Collections.emptyMap());
    }

    /**
     * Create SqlQueryWriter with the blacklisted functions. The blacklisted functions map
     * should have the function name in lower case as the key and the expected number of
     * parameters as the value. If the function is a variable argument function (can take
     * any number of arguments), use a negative number (preferably -1) as the value.
     *
     * @param blacklistedFunctions the map of blacklisted functions
     */
    public BaseSqlQueryWriter(Map<String, Integer> blacklistedFunctions)
    {
        requireNonNull(blacklistedFunctions, "supportingFunctions cannot be null");
        this.blacklistedFunctions = blacklistedFunctions;
    }

    @Override
    public String row(List<String> expressions)
    {
        return "ROW (" + Joiner.on(", ").join(expressions) + ")";
    }

    @Override
    public String atTimeZone(String value, String timezone)
    {
        return value + " AT TIME ZONE " + timezone;
    }

    @Override
    public String currentUser()
    {
        throw new UnsupportedOperationException("Cannot push CURRENT_USER to remote database");
    }

    @Override
    public String currentPath()
    {
        throw new UnsupportedOperationException("Cannot push CURRENT_PATH to remote database");
    }

    @Override
    public String currentTime(Time.Function function, Integer precision)
    {
        throw new UnsupportedOperationException("Cannot push current time functions to remote database");
    }

    @Override
    public String extract(String expression, Time.ExtractField field)
    {
        return "EXTRACT(" + field + " FROM " + expression + ")";
    }

    @Override
    public String booleanLiteral(boolean value)
    {
        return String.valueOf(value);
    }

    @Override
    public String stringLiteral(String value)
    {
        return formatStringLiteral(value);
    }

    @Override
    public String charLiteral(String value)
    {
        return "CHAR " + formatStringLiteral(value);
    }

    @Override
    public String binaryLiteral(String hexValue)
    {
        return "X'" + hexValue + "'";
    }

    @Override
    public String parameter(Optional<List<String>> parameters, int position)
    {
        if (parameters.isPresent()) {
            checkArgument(position < parameters.get().size(), "Invalid parameter number %s.  Max value is %s", position, parameters.get().size() - 1);
            return parameters.get().get(position);
        }
        return "?";
    }

    @Override
    public String arrayConstructor(List<String> values)
    {
        return "ARRAY[" + Joiner.on(",").join(values) + "]";
    }

    @Override
    public String subscriptExpression(String base, String index)
    {
        return base + "[" + index + "]";
    }

    @Override
    public String longLiteral(long value)
    {
        return Long.toString(value);
    }

    @Override
    public String doubleLiteral(double value)
    {
        return doubleFormatter.get().format(value);
    }

    @Override
    public String decimalLiteral(String value)
    {
        // TODO return node value without "DECIMAL '..'" when FeaturesConfig#parseDecimalLiteralsAsDouble switch is removed
        return "DECIMAL '" + value + "'";
    }

    @Override
    public String genericLiteral(String type, String value)
    {
        return type + " " + formatStringLiteral(value);
    }

    @Override
    public String timeLiteral(String value)
    {
        return "TIME '" + value + "'";
    }

    @Override
    public String timestampLiteral(String value)
    {
        return "TIMESTAMP '" + value + "'";
    }

    @Override
    public String nullLiteral()
    {
        return "null";
    }

    @Override
    public String intervalLiteral(Time.IntervalSign signLiteral, String value, Time.IntervalField startField, Optional<Time.IntervalField> endField)
    {
        String sign = (signLiteral == Time.IntervalSign.NEGATIVE) ? " - " : " ";
        StringBuilder builder = new StringBuilder()
                .append("INTERVAL")
                .append(sign)
                .append("'").append(value).append("' ")
                .append(startField);

        endField.ifPresent(field -> builder.append(" TO ").append(field));
        return builder.toString();
    }

    @Override
    public String subqueryExpression(String query)
    {
        return "(" + query + ")";
    }

    @Override
    public String exists(String subquery)
    {
        return "(EXISTS " + subquery + ")";
    }

    @Override
    public String identifier(String value, boolean delimited)
    {
        if (!delimited) {
            return value;
        }
        else {
            return '"' + value.replace("\"", "\"\"") + '"';
        }
    }

    @Override
    public String lambdaArgumentDeclaration(String identifier)
    {
        return identifier;
    }

    @Override
    public String dereferenceExpression(String base, String field)
    {
        return base + "." + field;
    }

    @Override
    public String fieldReference(int fieldIndex)
    {
        // add colon so this won't parse
        return ":input(" + fieldIndex + ")";
    }

    @Override
    public String functionCall(QualifiedName name, boolean distinct, List<String> argumentsList, Optional<String> orderBy, Optional<String> filter, Optional<String> window)
    {
        String functionName = formatQualifiedName(name);
        if (this.isBlacklistedFunction(functionName, argumentsList.size())) {
            // Replace dynamic filter function name with true for sub-query pushdown
            if (DYNAMIC_FILTER_FUNCTION_NAME.equals(name.toString())) {
                return "true";
            }
            throw new UnsupportedOperationException("The connector does not support the function " + functionName);
        }
        StringBuilder builder = new StringBuilder();

        String arguments = joinExpressions(argumentsList);
        if (argumentsList.isEmpty() && "count".equalsIgnoreCase(name.getSuffix())) {
            arguments = "*";
        }
        if (distinct) {
            arguments = "DISTINCT " + arguments;
        }

        builder.append(formatQualifiedName(name))
                .append('(').append(arguments);
        orderBy.ifPresent(exp -> builder.append(' ').append(exp));
        builder.append(')');
        filter.ifPresent(exp -> builder.append(" FILTER ").append(exp));
        window.ifPresent(exp -> builder.append(" OVER ").append(exp));

        return builder.toString();
    }

    @Override
    public String lambdaExpression(List<String> arguments, String body)
    {
        StringBuilder builder = new StringBuilder();

        builder.append('(');
        Joiner.on(", ").appendTo(builder, arguments);
        builder.append(") -> ");
        builder.append(body);
        return builder.toString();
    }

    @Override
    public String bindExpression(List<String> values, String function)
    {
        return "\"$INTERNAL$BIND\"(" +
                Joiner.on(", ").join(values) +
                function +
                '(';
    }

    @Override
    public String logicalBinaryExpression(Operators.LogicalOperator operator, String left, String right)
    {
        return formatBinaryExpression(operator.toString(), left, right);
    }

    @Override
    public String notExpression(String value)
    {
        return "(NOT " + value + ")";
    }

    @Override
    public String comparisonExpression(Operators.ComparisonOperator operator, String left, String right)
    {
        return formatBinaryExpression(operator.getValue(), left, right);
    }

    @Override
    public String isNullPredicate(String value)
    {
        return "(" + value + " IS NULL)";
    }

    @Override
    public String isNotNullPredicate(String value)
    {
        return "(" + value + " IS NOT NULL)";
    }

    @Override
    public String nullIfExpression(String first, String second)
    {
        return "NULLIF(" + first + ", " + second + ')';
    }

    @Override
    public String ifExpression(String condition, String trueValue, Optional<String> falseValue)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("IF(")
                .append(condition)
                .append(", ")
                .append(trueValue);
        falseValue.ifPresent(value -> builder.append(", ").append(value));
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String tryExpression(String innerExpression)
    {
        return "TRY(" + innerExpression + ")";
    }

    @Override
    public String coalesceExpression(List<String> operands)
    {
        return "COALESCE(" + joinExpressions(operands) + ")";
    }

    @Override
    public String arithmeticUnary(Operators.Sign sign, String value)
    {
        switch (sign) {
            case MINUS:
                // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
                String separator = value.startsWith("-") ? " " : "";
                return "-" + separator + value;
            case PLUS:
                return "+" + value;
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + sign);
        }
    }

    @Override
    public String arithmeticBinary(Operators.ArithmeticOperator operator, String left, String right)
    {
        return formatBinaryExpression(operator.getValue(), left, right);
    }

    @Override
    public String likePredicate(String value, String pattern, Optional<String> escape)
    {
        StringBuilder builder = new StringBuilder();

        builder.append('(')
                .append(value)
                .append(" LIKE ")
                .append(pattern);

        escape.ifPresent(val -> builder.append(" ESCAPE ")
                .append(val));

        builder.append(')');

        return builder.toString();
    }

    @Override
    public String allColumns(Optional<QualifiedName> prefix)
    {
        return prefix.map(name -> name + ".*").orElse("*");
    }

    @Override
    public String cast(String expression, String type, boolean safe, boolean typeOnly)
    {
        return (safe ? "TRY_CAST" : "CAST") +
                "(" + expression + " AS " + toNativeType(type) + ")";
    }

    @Override
    public String searchedCaseExpression(List<String> whenCaluses, Optional<String> defaultValue)
    {
        ImmutableList.Builder<String> parts = ImmutableList.builder();
        parts.add("CASE");
        parts.addAll(whenCaluses);
        defaultValue.ifPresent((value) -> parts.add("ELSE").add(value));
        parts.add("END");
        return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    public String simpleCaseExpression(String operand, List<String> whenCaluses, Optional<String> defaultValue)
    {
        ImmutableList.Builder<String> parts = ImmutableList.builder();
        parts.add("CASE").add(operand);
        parts.addAll(whenCaluses);
        defaultValue.ifPresent((value) -> parts.add("ELSE").add(value));
        parts.add("END");
        return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    public String whenClause(String operand, String result)
    {
        return "WHEN " + operand + " THEN " + result;
    }

    @Override
    public String betweenPredicate(String value, String min, String max)
    {
        return "(" + value + " BETWEEN " + min + " AND " + max + ")";
    }

    @Override
    public String inPredicate(String value, String valueList)
    {
        return "(" + value + " IN " + valueList + ")";
    }

    @Override
    public String inListExpression(List<String> values)
    {
        return "(" + joinExpressions(values) + ")";
    }

    @Override
    public String filter(String value)
    {
        if ("false".equals(value)) {
            return "(WHERE 1=0)";
        }
        else if ("true".equals(value)) {
            return "(WHERE 1=1)";
        }
        return "(WHERE " + value + ')';
    }

    @Override
    public String groupByIdElement(List<List<String>> groSets)
    {
        // default impl will write it as Hetu grammar
        List<List<String>> bewGroSet = new ArrayList<>();
        for (int i = groSets.size() - 1; i >= 0; i--) {
            bewGroSet.add(groSets.get(i));
        }
        return bewGroSet.toString().replace('[', '(').replace(']', ')');
    }

    @Override
    public String formatWindowColumn(String functionName, List<String> args, String windows)
    {
        String signatureStr = this.functionCall(new QualifiedName(Collections.singletonList(functionName)),
                false, args, Optional.empty(), Optional.empty(), Optional.empty());
        return " " + signatureStr + " OVER " + windows;
    }

    @Override
    public String window(List<String> partitionBy, Optional<String> orderBy, Optional<String> frame)
    {
        List<String> parts = new ArrayList<>();

        if (!partitionBy.isEmpty()) {
            parts.add("PARTITION BY " + joinExpressions(partitionBy));
        }
        orderBy.ifPresent(parts::add);
        frame.ifPresent(parts::add);

        return '(' + Joiner.on(' ').join(parts) + ')';
    }

    @Override
    public String windowFrame(Types.WindowFrameType type, String start, Optional<String> end)
    {
        StringBuilder builder = new StringBuilder();

        builder.append(type.toString()).append(' ');

        if (end.isPresent()) {
            builder.append("BETWEEN ")
                    .append(start)
                    .append(" AND ")
                    .append(end.get());
        }
        else {
            builder.append(start);
        }

        return builder.toString();
    }

    @Override
    public String frameBound(Types.FrameBoundType type, Optional<String> value)
    {
        switch (type) {
            case UNBOUNDED_PRECEDING:
                return "UNBOUNDED PRECEDING";
            case PRECEDING:
                if (!value.isPresent()) {
                    throw new UnsupportedOperationException("Unsupported empty value in " + type);
                }
                return value.get() + " PRECEDING";
            case CURRENT_ROW:
                return "CURRENT ROW";
            case FOLLOWING:
                if (!value.isPresent()) {
                    throw new UnsupportedOperationException("Unsupported empty value in " + type);
                }
                return value.get() + " FOLLOWING";
            case UNBOUNDED_FOLLOWING:
                return "UNBOUNDED FOLLOWING";
        }
        throw new IllegalArgumentException("unhandled type: " + type);
    }

    @Override
    public String quantifiedComparisonExpression(Operators.ComparisonOperator operator, Types.Quantifier quantifier, String value, String subquery)
    {
        return "(" + value + ' ' + operator.getValue() + ' ' + quantifier + ' ' + subquery + ")";
    }

    @Override
    public String groupingOperation(List<String> groupingColumns)
    {
        return "GROUPING (" + joinExpressions(groupingColumns) + ")";
    }

    @Override
    @SuppressWarnings("Duplicates")
    public String formatStringLiteral(String literal)
    {
        literal = literal.replace("'", "''");
        if (CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(literal)) {
            return "'" + literal + "'";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("U&'");
        PrimitiveIterator.OfInt iterator = literal.codePoints().iterator();
        while (iterator.hasNext()) {
            int codePoint = iterator.nextInt();
            checkArgument(codePoint >= 0, "Invalid UTF-8 encoding in characters: %s", literal);
            if (isAsciiPrintable(codePoint)) {
                char ch = (char) codePoint;
                if (ch == '\\') {
                    builder.append(ch);
                }
                builder.append(ch);
            }
            else if (codePoint <= 0xFFFF) {
                builder.append('\\');
                builder.append(format("%04X", codePoint));
            }
            else {
                builder.append("\\+");
                builder.append(format("%06X", codePoint));
            }
        }
        builder.append("'");
        return builder.toString();
    }

    @Override
    public String joinExpressions(List<String> expressions)
    {
        return Joiner.on(", ").join(expressions);
    }

    @Override
    public boolean isBlacklistedFunction(String qualifiedName, int noOfArgs)
    {
        if (qualifiedName.contains(INTERNAL_FUNCTION_PREFIX)) {
            // Internal functions such as `$literal$time with time zone` cannot be pushed down
            return true;
        }
        Integer args = this.blacklistedFunctions.get(qualifiedName.toLowerCase(Locale.ENGLISH));
        return args != null && (args < 0 || args == noOfArgs);
    }

    @Override
    public String orderBy(List<OrderBy> orders)
    {
        StringJoiner joiner = new StringJoiner(", ");
        for (OrderBy orderBy : orders) {
            StringJoiner orderItem = new StringJoiner(" ");
            orderItem.add(orderBy.getSymbol());
            SortOrder sortOrder = orderBy.getType();
            orderItem.add(sortOrder.isAscending() ? "ASC" : "DESC");
            orderItem.add(sortOrder.isNullsFirst() ? "NULLS FIRST" : "NULLS LAST");
            joiner.merge(orderItem);
        }
        return " ORDER BY " + joiner.toString();
    }

    @Override
    public String qualifiedName(String tableName, String symbolName)
    {
        return tableName + "." + symbolName;
    }

    @Override
    public String queryAlias(String id)
    {
        return "table" + id;
    }

    @Override
    public String formatIdentifier(Optional<Map<String, Selection>> qualifiedNames, String identifier)
    {
        if (qualifiedNames.isPresent()) {
            identifier = qualifiedNames.get().get(identifier).getExpression();
        }
        return identifier;
    }

    @Override
    public String formatQualifiedName(QualifiedName name)
    {
        return name.getParts().stream()
                .map(identifier -> formatIdentifier(Optional.empty(), identifier))
                .collect(joining("."));
    }

    @Override
    public String formatBinaryExpression(String operator, String left, String right)
    {
        return '(' + left + ' ' + operator + ' ' + right + ')';
    }

    @Override
    public String toNativeType(String type)
    {
        return type;
    }

    @Override
    public String select(List<Selection> symbols, String from)
    {
        if (symbols.size() == 0) {
            return "(SELECT * FROM " + from + ")";
        }
        StringJoiner selection = new StringJoiner(", ");
        for (Selection symbol : symbols) {
            if (symbol.isAliased()) {
                selection.add(symbol.getExpression() + " AS " + symbol.getAlias());
            }
            else {
                selection.add(symbol.getExpression());
            }
        }
        return "(SELECT " + selection.toString() + " FROM " + from + ")";
    }

    @Override
    public String join(List<Selection> symbols, Types.JoinType type, String left, String leftId, String right, String rightId, List<String> criteria, Optional<String> filter)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(left);
        builder.append(' ');
        builder.append(leftId);
        builder.append(' ');
        builder.append(type.getJoinLabel());
        builder.append(' ');
        builder.append(right);
        builder.append(' ');
        builder.append(rightId);
        builder.append(' ');

        // Cross Join does not have criteria
        if (!criteria.isEmpty() || filter.isPresent()) {
            // Filter requires ON
            builder.append(" ON ");
            StringJoiner joiner = new StringJoiner(" AND ");
            criteria.forEach(joiner::add);
            filter.ifPresent(joiner::add);
            builder.append(joiner.toString());
        }
        return select(symbols, builder.toString());
    }

    @Override
    public String aggregation(List<Selection> symbols, Optional<List<String>> groupingKeysOp, Optional<String> groupIdElementOP, String from)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(from);
        if (groupingKeysOp.isPresent()) {
            List<String> groupingKeys = groupingKeysOp.get();
            if (!groupingKeys.isEmpty()) {
                builder.append(" GROUP BY ");
                builder.append(Joiner.on(", ").join(groupingKeys));
            }
        }
        else if (groupIdElementOP.isPresent()) {
            String groupEleStr = groupIdElementOP.get();
            builder.append(" GROUP BY GROUPING SETS ");
            builder.append(groupEleStr);
        }
        return select(symbols, builder.toString());
    }

    @Override
    public String limit(List<Selection> symbols, long count, String from)
    {
        return select(symbols, from + " LIMIT " + count);
    }

    @Override
    public String filter(List<Selection> symbols, String predicate, String from)
    {
        if ("false".equals(predicate)) {
            return select(symbols, from + " WHERE 1=0 ");
        }
        else if ("true".equals(predicate)) {
            return select(symbols, from + " WHERE 1=1 ");
        }
        else {
            return select(symbols, from + " WHERE " + predicate);
        }
    }

    @Override
    public String sort(List<Selection> symbols, List<OrderBy> orderings, String from)
    {
        return select(symbols, from + orderBy(orderings));
    }

    @Override
    public String topN(List<Selection> symbols, List<OrderBy> orderings, long count, String from)
    {
        return select(symbols, from + orderBy(orderings) + " LIMIT " + count);
    }

    @Override
    public String setOperator(List<Selection> symbols, Types.SetOperator type, List<String> relations)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("  (");
        boolean first = true;
        for (String relation : relations) {
            builder.append(" ");
            builder.append(first ? "" : type.getLabel());
            builder.append(" ");
            builder.append(relation);
            first = false;
        }
        builder.append(") ");
        return select(symbols, builder.toString());
    }

    private static boolean isAsciiPrintable(int codePoint)
    {
        return codePoint < 0x7F && codePoint >= 0x20;
    }
}
