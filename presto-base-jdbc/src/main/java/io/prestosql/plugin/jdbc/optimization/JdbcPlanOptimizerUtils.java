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
package io.prestosql.plugin.jdbc.optimization;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.spi.sql.expression.Types;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_QUERY_GENERATOR_FAILURE;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.stream.Collectors.toList;

public class JdbcPlanOptimizerUtils
{
    public static final String GROUPING_COLUMN_SUFFIX = "$gid";
    public static final String REPLACED_GROUPING_COLUMN_SUFFIX = "_gid";
    public static final String DISTINCT_SUFFIX = "$distinct";
    public static final String DERIVED_TABLE_PREFIX = "hetu_table_";
    public static final String JOIN_LEFT_TABLE_PREFIX = "hetu_left_";
    public static final String JOIN_RIGHT_TABLE_PREFIX = "hetu_right_";

    private JdbcPlanOptimizerUtils() {}

    public static List<Selection> getSelectionsFromSymbolsMap(Map<Symbol, Symbol> symbols)
    {
        return symbols.entrySet().stream().map(entry -> new Selection(entry.getValue().getName(), entry.getKey().getName())).collect(toList());
    }

    public static boolean isSameCatalog(List<JdbcQueryGeneratorContext> contexts)
    {
        if (contexts == null || contexts.isEmpty()) {
            throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, "context is null or empty");
        }
        CatalogName catalog = contexts.get(0).getCatalogName().get();
        for (JdbcQueryGeneratorContext context : contexts) {
            if (!context.getCatalogName().get().equals(catalog)) {
                return false;
            }
        }
        return true;
    }

    public static String parentheses(String inputString)
    {
        return "(" + inputString + ")";
    }

    public static Optional<String> getDerivedTable(String tableExpression, int identifier)
    {
        return Optional.of(parentheses(tableExpression) + " " + DERIVED_TABLE_PREFIX + identifier);
    }

    public static String frameBound(Types.FrameBoundType type, Optional<String> value)
    {
        switch (type) {
            case UNBOUNDED_PRECEDING:
                return "UNBOUNDED PRECEDING";
            case PRECEDING:
                if (!value.isPresent()) {
                    throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, "Unsupported empty value in " + type);
                }
                return value.get() + " PRECEDING";
            case CURRENT_ROW:
                return "CURRENT ROW";
            case FOLLOWING:
                if (!value.isPresent()) {
                    throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, "Unsupported empty value in " + type);
                }
                return value.get() + " FOLLOWING";
            case UNBOUNDED_FOLLOWING:
                return "UNBOUNDED FOLLOWING";
        }
        throw new PrestoException(JDBC_QUERY_GENERATOR_FAILURE, "unhandled type: " + type);
    }

    public static String quote(String quote, String name)
    {
        name = name.replace(quote, quote + quote);
        return quote + name + quote;
    }

    public static boolean isAggregationDistinct(AggregationNode.Aggregation aggregation)
    {
        if (aggregation.isDistinct()) {
            return true;
        }
        if (aggregation.getMask().isPresent()) {
            if (aggregation.getMask().get().getName().contains(DISTINCT_SUFFIX)) {
                return true;
            }
            throw new PrestoException(NOT_SUPPORTED, "Unsupported mask in push down: " + aggregation.getMask().get());
        }
        return false;
    }

    public static LinkedHashMap<String, Selection> getProjectSelections(LinkedHashMap<String, Selection> oldSelection)
    {
        LinkedHashMap<String, Selection> newSelection = new LinkedHashMap<>();
        oldSelection.forEach((name, selection) -> newSelection.put(name, new Selection(name)));
        return newSelection;
    }

    public static String replaceGroupingSetColumns(String sql)
    {
        return sql.replace(GROUPING_COLUMN_SUFFIX, REPLACED_GROUPING_COLUMN_SUFFIX);
    }

    public static String getGroupingSetColumn(String column)
    {
        if (column.endsWith(GROUPING_COLUMN_SUFFIX)) {
            return column.replace(GROUPING_COLUMN_SUFFIX, REPLACED_GROUPING_COLUMN_SUFFIX);
        }
        return column;
    }
}
