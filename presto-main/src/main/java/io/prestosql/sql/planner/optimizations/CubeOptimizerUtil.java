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

package io.prestosql.sql.planner.optimizations;

import io.hetu.core.spi.cube.CubeAggregateFunction;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.hetu.core.spi.cube.CubeAggregateFunction.SUPPORTED_FUNCTIONS;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;

public class CubeOptimizerUtil
{
    private CubeOptimizerUtil()
    {
        //util class
    }

    /**
     * Checks if aggregation node can be optimized by this rule. Only if the AggregationNode has
     * supporting functions, it will return true.
     *
     * @param aggregationNode the aggregation node
     * @return true if aggregators are supported
     */
    static boolean isSupportedAggregation(AggregationNode aggregationNode)
    {
        if (aggregationNode.getOutputSymbols().isEmpty()) {
            return false;
        }

        return aggregationNode.getAggregations()
                .values()
                .stream()
                .allMatch(CubeOptimizerUtil::isSupported);
    }

    static boolean isSupported(AggregationNode.Aggregation aggregation)
    {
        return SUPPORTED_FUNCTIONS.contains(aggregation.getFunctionCall().getDisplayName()) &&
                aggregation.getFunctionCall().getArguments().size() <= 1 &&
                (!aggregation.isDistinct() || aggregation.getFunctionCall().getDisplayName().equals(CubeAggregateFunction.COUNT.getName()));
    }

    /**
     * Checks if projection node can be optimized by this rule. Only if the PlanNode is a ProjectNode
     * and only has SymbolReference or Literal expressions in projections.
     *
     * @param planNode the ProjectNode
     * @return true if the planNode meets the requirements of ProjectNode
     */
    static boolean supportedProjectNode(Optional<PlanNode> planNode)
    {
        if (!planNode.isPresent()) {
            // Project node is optional
            return true;
        }

        if (!(planNode.get() instanceof ProjectNode)) {
            return false;
        }

        for (Map.Entry<Symbol, RowExpression> assignment : ((ProjectNode) planNode.get()).getAssignments().entrySet()) {
            RowExpression rowExpression = assignment.getValue();
            if (OriginalExpressionUtils.isExpression(rowExpression)) {
                Expression expression = castToExpression(assignment.getValue());
                if (expression instanceof SymbolReference || expression instanceof Literal) {
                    continue;
                }

                if (expression instanceof Cast) {
                    expression = ((Cast) expression).getExpression();
                    return (expression instanceof SymbolReference || expression instanceof Literal);
                }
            }
            else {
                if (rowExpression instanceof VariableReferenceExpression) {
                    continue;
                }
                if (rowExpression instanceof CallExpression) {
                    // Extract the column symbols from CAST expressions
                    while (rowExpression instanceof CallExpression) {
                        rowExpression = ((CallExpression) rowExpression).getArguments().get(0);
                    }
                    return rowExpression instanceof VariableReferenceExpression;
                }
            }
            return false;
        }
        return true;
    }

    /**
     * Construct a map of symbols mapping to constant value or the underlying column name.
     *
     * @param projections the list of ProjectNodes in between the aggregation node and the tableScan node
     * @param filterNode filter node of the aggregation sub-tree
     * @param tableScanNode table scans under the join node
     * @param tableMetadata table metadata
     * @return output symbols to constant or actual column name mapping
     */
    public static Map<String, Object> buildSymbolMappings(AggregationNode aggregationNode,
            List<ProjectNode> projections,
            Optional<PlanNode> filterNode,
            TableScanNode tableScanNode,
            TableMetadata tableMetadata)
    {
        // Initialize a map with outputSymbols mapping to themselves
        Map<String, Object> symbolMapping = new HashMap<>();
        Set<String> tableColumns = tableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toSet());
        aggregationNode.getOutputSymbols().stream().map(Symbol::getName).forEach(symbol -> symbolMapping.put(symbol, symbol));
        aggregationNode.getAggregations().values().forEach(aggregation -> SymbolsExtractor.extractUnique(aggregation).stream().map(Symbol::getName)
                .forEach(symbol -> symbolMapping.put(symbol, symbol)));
        //Filter node sometime contains the Join condition. So exclude that from symbol mapping
        filterNode.ifPresent(planNode -> (SymbolsExtractor.extractUnique(((FilterNode) planNode).getPredicate())).stream()
                .map(Symbol::getName)
                .filter(tableColumns::contains)
                .forEach(symbol -> symbolMapping.put(symbol, symbol)));

        // Track how a symbol name is renamed throughout all project nodes
        for (ProjectNode projection : projections) {
            // ProjectNode is identity
            for (Map.Entry<String, Object> symbolEntry : symbolMapping.entrySet()) {
                Optional<Object> mappedValue = extractMappedValue(new Symbol(String.valueOf(symbolEntry.getValue())), projection);
                mappedValue.ifPresent(symbolEntry::setValue);
            }
        }

        // Update the map by actual outputSymbols being mapped by the symbol
        Map<Symbol, ColumnHandle> assignments = tableScanNode.getAssignments();
        for (Map.Entry<String, Object> symbolMappingEntry : symbolMapping.entrySet()) {
            Object symbolName = symbolMappingEntry.getValue();
            //read remaining Symbol name entries from map
            if (symbolName instanceof String) {
                ColumnHandle columnHandle = assignments.get(new Symbol((String) symbolName));
                if (columnHandle != null) {
                    symbolMappingEntry.setValue(columnHandle);
                }
            }
        }
        return symbolMapping;
    }

    private static Optional<Object> extractMappedValue(Symbol symbol, ProjectNode projectNode)
    {
        Map<Symbol, RowExpression> assignments = projectNode.getAssignments().getMap();
        RowExpression rowExpression = assignments.get(symbol);
        if (rowExpression == null) {
            return Optional.empty();
        }
        if (OriginalExpressionUtils.isExpression(rowExpression)) {
            Expression expression = castToExpression(rowExpression);
            if (expression instanceof Cast) {
                expression = ((Cast) expression).getExpression();
            }

            if (expression instanceof SymbolReference) {
                return Optional.of(((SymbolReference) expression).getName());
            }
            else if (expression instanceof Literal) {
                return Optional.of(expression);
            }
        }
        else {
            if (rowExpression instanceof CallExpression) {
                // Extract the column symbols from CAST expressions
                while (rowExpression instanceof CallExpression) {
                    rowExpression = ((CallExpression) rowExpression).getArguments().get(0);
                }
            }

            if (!(rowExpression instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }
            return Optional.of(((VariableReferenceExpression) rowExpression).getName());
        }
        return Optional.empty();
    }
}
