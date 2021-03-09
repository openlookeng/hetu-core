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

package io.prestosql.cube;

import io.hetu.core.spi.cube.CubeStatement;
import io.hetu.core.spi.cube.aggregator.AggregationSignature;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.optimizations.StarTreeAggregationRule;
import io.prestosql.sql.planner.planprinter.RowExpressionFormatter;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class CubeStatementGenerator
{
    private CubeStatementGenerator()
    {
        //utility class
    }

    public static CubeStatement generate(String fromTable,
            AggregationNode aggregationNode,
            FilterNode filterNode,
            Map<String, Object> symbolMappings)
    {
        CubeStatement.Builder builder = CubeStatement.newBuilder();
        builder.from(fromTable);
        Map<Symbol, AggregationNode.Aggregation> aggregations = aggregationNode.getAggregations();
        // Extract selection and aggregation
        for (Symbol symbol : aggregationNode.getOutputSymbols()) {
            Object column = symbolMappings.get(symbol.getName());
            if (column instanceof ColumnHandle) {
                // Selection
                String columnName = ((ColumnHandle) column).getColumnName();
                builder.select(columnName);
            }
            else if (aggregations.containsKey(symbol)) {
                // Aggregation
                Map<Symbol, AggregationSignature> signatures = Collections.emptyMap();
                AggregationNode.Aggregation aggregation = aggregations.get(symbol);
                List<RowExpression> arguments = aggregation.getArguments();
                if (arguments.isEmpty()) {
                    signatures = createSignature(aggregation, symbol, null);
                }
                else if (arguments.size() == 1) {
                    RowExpression argument = arguments.get(0);
                    if (OriginalExpressionUtils.isExpression(argument)) {
                        Expression argAsExpr = OriginalExpressionUtils.castToExpression(argument);
                        if (argAsExpr instanceof SymbolReference) {
                            signatures = createSignature(aggregation, symbol, symbolMappings.get(((SymbolReference) argAsExpr).getName()));
                        }
                    }
                    else if (argument instanceof VariableReferenceExpression) {
                        signatures = createSignature(aggregation, symbol, symbolMappings.get(((VariableReferenceExpression) argument).getName()));
                    }
                }
                if (signatures.isEmpty()) {
                    throw new IllegalArgumentException("Failed to generate aggregator signature");
                }

                for (Map.Entry<Symbol, AggregationSignature> aggEntry : signatures.entrySet()) {
                    builder.aggregate(aggEntry.getValue());
                }
            }
            else {
                throw new IllegalArgumentException("Column " + column + " is not an actual column or expressions");
            }
        }

        // Extract group by
        for (Symbol symbol : aggregationNode.getGroupingKeys()) {
            Object column = symbolMappings.get(symbol.getName());
            if (column instanceof ColumnHandle) {
                builder.groupBy(((ColumnHandle) column).getColumnName());
            }
            else {
                // Don't know how to handle it
                throw new IllegalArgumentException("Column " + symbol + " is not an actual column");
            }
        }

        if (filterNode != null) {
            RowExpression predicate = filterNode.getPredicate();
            Set<Symbol> expressionSymbols = SymbolsExtractor.extractUnique(predicate);
            for (Symbol symbol : expressionSymbols) {
                Object column = symbolMappings.get(symbol.getName());
                if (column instanceof ColumnHandle) {
                    builder.select(((ColumnHandle) column).getColumnName());
                    builder.groupBy(((ColumnHandle) column).getColumnName());
                }
                else {
                    // Don't know how to handle it
                    throw new IllegalArgumentException("Column " + symbol + " is not an actual column");
                }
            }

            if (OriginalExpressionUtils.isExpression(predicate)) {
                Expression expression = OriginalExpressionUtils.castToExpression(predicate);
                builder.where(ExpressionFormatter.formatExpression(expression, Optional.empty()));
            }
            else {
                String predicateString = new RowExpressionFormatter().formatRowExpression(predicate);
                builder.where(predicateString);
            }
        }
        return builder.build();
    }

    public static Map<Symbol, AggregationSignature> createSignature(AggregationNode.Aggregation aggregation, Symbol symbol, Object argument)
    {
        String aggregationName = aggregation.getSignature().getName();
        boolean distinct = aggregation.isDistinct();
        Map<Symbol, AggregationSignature> signature = Collections.emptyMap();
        if (argument instanceof ColumnHandle) {
            String columnName = ((ColumnHandle) argument).getColumnName();
            if (StarTreeAggregationRule.SUM.equals(aggregationName)) {
                // SUM aggregation
                signature = Collections.singletonMap(symbol, AggregationSignature.sum(columnName, distinct));
            }
            else if (StarTreeAggregationRule.AVG.equals(aggregationName)) {
                // AVG aggregation
                signature = Collections.singletonMap(symbol, AggregationSignature.avg(columnName, distinct));
            }
            else if (StarTreeAggregationRule.COUNT.equals(aggregationName)) {
                // COUNT(columnName)
                signature = Collections.singletonMap(symbol, AggregationSignature.count(columnName, distinct));
            }
            else if (StarTreeAggregationRule.MIN.equals(aggregationName)) {
                // MIN aggregation
                signature = Collections.singletonMap(symbol, AggregationSignature.min(columnName, distinct));
            }
            else if (StarTreeAggregationRule.MAX.equals(aggregationName)) {
                // MAX aggregation
                signature = Collections.singletonMap(symbol, AggregationSignature.max(columnName, distinct));
            }
        }
        else if (argument == null || (argument instanceof LongLiteral && ((LongLiteral) argument).getValue() == 1)) {
            // COUNT aggregation
            if (StarTreeAggregationRule.COUNT.equals(aggregationName) && !aggregation.isDistinct()) {
                // COUNT(1)
                // TODO: Validate the assumption: Non distinct count is always count(*)
                signature = Collections.singletonMap(symbol, AggregationSignature.count());
            }
        }
        return signature;
    }

    public static class AverageAggregatorSource
    {
        private final Symbol sum;
        private final Symbol count;

        public AverageAggregatorSource(Symbol sum, Symbol count)
        {
            this.sum = sum;
            this.count = count;
        }

        public Symbol getSum()
        {
            return sum;
        }

        public Symbol getCount()
        {
            return count;
        }
    }
}
