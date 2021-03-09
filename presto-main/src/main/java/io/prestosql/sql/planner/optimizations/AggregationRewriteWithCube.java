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

package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.aggregator.AggregationSignature;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ColumnNotFoundException;
import io.prestosql.spi.connector.CubeNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.prestosql.spi.StandardErrorCode.CUBE_ERROR;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;

public class AggregationRewriteWithCube
{
    private static final Logger log = Logger.get(AggregationRewriteWithCube.class);
    private final Session session;
    private final SymbolAllocator symbolAllocator;
    private final Metadata metadata;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<String, Object> symbolMappings;
    private final CubeMetadata cubeMetadata;
    private final TypeProvider typeProvider;

    public AggregationRewriteWithCube(Metadata metadata, Session session, PlanSymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Map<String, Object> symbolMappings, CubeMetadata cubeMetadata)
    {
        this.session = session;
        this.symbolAllocator = symbolAllocator;
        this.metadata = metadata;
        this.idAllocator = idAllocator;
        this.symbolMappings = symbolMappings;
        this.cubeMetadata = cubeMetadata;
        this.typeProvider = symbolAllocator.getTypes();
    }

    public PlanNode rewrite(AggregationNode originalAggregationNode, PlanNode filterNode)
    {
        QualifiedObjectName starTreeTableName = QualifiedObjectName.valueOf(cubeMetadata.getCubeTableName());
        TableHandle cubeTableHandle = metadata.getTableHandle(session, starTreeTableName)
                .orElseThrow(() -> new CubeNotFoundException(starTreeTableName.toString()));
        Map<String, ColumnHandle> cubeColumnsMap = metadata.getColumnHandles(session, cubeTableHandle);
        CubeRewriteResult cubeRewriteResult = createScanNode(originalAggregationNode, filterNode, cubeTableHandle, cubeColumnsMap);
        PlanNode planNode = cubeRewriteResult.getTableScanNode();

        // Add filter node
        if (filterNode != null) {
            Expression expression = castToExpression(((FilterNode) filterNode).getPredicate());
            expression = rewriteSymbolReferenceUsingColumnName(expression, symbolMappings);
            planNode = new FilterNode(idAllocator.getNextId(), planNode, castToRowExpression(expression));
        }

        // Add group by
        List<Symbol> groupings = new ArrayList<>(originalAggregationNode.getGroupingKeys().size());
        for (Symbol symbol : originalAggregationNode.getGroupingKeys()) {
            Object column = symbolMappings.get(symbol.getName());
            if (column instanceof ColumnHandle) {
                groupings.add(new Symbol(((ColumnHandle) column).getColumnName()));
            }
        }

        // Rewrite AggregationNode using Cube table
        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregationsBuilder = ImmutableMap.builder();
        for (CubeRewriteResult.AggregatorSource aggregatorSource : cubeRewriteResult.getAggregationColumns()) {
            TypeSignature typeSignature = cubeRewriteResult.getSymbolMetadataMap().get(aggregatorSource.getOriginalAggSymbol()).getType().getTypeSignature();
            ColumnHandle cubeColHandle = cubeRewriteResult.getTableScanNode().getAssignments().get(aggregatorSource.getScanSymbol());
            ColumnMetadata cubeColumnMetadata = metadata.getColumnMetadata(session, cubeTableHandle, cubeColHandle);
            AggregationSignature aggregationSignature = cubeMetadata.getAggregationSignature(cubeColumnMetadata.getName())
                    .orElseThrow(() -> new ColumnNotFoundException(new SchemaTableName(starTreeTableName.getSchemaName(), starTreeTableName.getObjectName()), cubeColHandle.getColumnName()));
            String aggFunction = AggregationSignature.COUNT_FUNCTION_NAME.equals(aggregationSignature.getFunction()) ? "sum" : aggregationSignature.getFunction();
            Signature signature = new Signature(aggFunction, AGGREGATE, typeSignature, typeSignature);
            SymbolReference argument = toSymbolReference(aggregatorSource.getScanSymbol());
            aggregationsBuilder.put(aggregatorSource.getOriginalAggSymbol(), new AggregationNode.Aggregation(
                    signature,
                    ImmutableList.of(OriginalExpressionUtils.castToRowExpression(argument)),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));
        }

        planNode = new AggregationNode(idAllocator.getNextId(),
                planNode,
                aggregationsBuilder.build(),
                singleGroupingSet(groupings),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());

        // If there was an AVG aggregation, map it to AVG = SUM/COUNT
        if (!cubeRewriteResult.getAvgAggregationColumns().isEmpty()) {
            Set<Symbol> generatedSymbols = new HashSet<>();
            cubeRewriteResult.getAvgAggregationColumns().forEach(source -> {
                generatedSymbols.add(source.getCount());
                generatedSymbols.add(source.getSum());
            });
            Map<Symbol, Expression> assignments = new HashMap<>();
            // Add all original symbols as symbol reference
            for (Symbol symbol : originalAggregationNode.getOutputSymbols()) {
                Object originalColumn = symbolMappings.get(symbol.getName());
                if (originalColumn instanceof ColumnHandle) {
                    //Refer to outputs of TableScanNode
                    ColumnHandle cubeColumn = cubeColumnsMap.get(((ColumnHandle) originalColumn).getColumnName());
                    Symbol cubeScanSymbol = new Symbol(cubeColumn.getColumnName());
                    generatedSymbols.add(cubeScanSymbol);
                    assignments.put(cubeScanSymbol, toSymbolReference(cubeScanSymbol));
                }
            }
            for (Symbol symbol : cubeRewriteResult.getTableScanNode().getOutputSymbols()) {
                if (!generatedSymbols.contains(symbol)) {
                    assignments.put(symbol, toSymbolReference(symbol));
                }
            }

            // Add AVG = SUM / COUNT
            for (CubeRewriteResult.AverageAggregatorSource avgAggSource : cubeRewriteResult.getAvgAggregationColumns()) {
                ArithmeticBinaryExpression division = new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.DIVIDE,
                        toSymbolReference(avgAggSource.getSum()), new Cast(toSymbolReference(avgAggSource.getCount()), cubeRewriteResult.getSymbolMetadataMap().get(avgAggSource.getSum()).getType().getTypeSignature().toString()));
                Type avgType = typeProvider.get(avgAggSource.getOriginalAggSymbol());
                assignments.put(avgAggSource.getOriginalAggSymbol(), new Cast(division, avgType.getTypeSignature().toString()));
            }
            planNode = new ProjectNode(idAllocator.getNextId(),
                    planNode,
                    new Assignments(assignments
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, entry -> castToRowExpression(entry.getValue())))));
        }

        // Safety check to remove redundant symbols and rename original column names to intermediate names
        if (!planNode.getOutputSymbols().equals(originalAggregationNode.getOutputSymbols())) {
            // Map new symbol names to the old symbols
            Map<Symbol, Expression> assignments = new HashMap<>();
            for (Symbol originalAggOutputSymbol : originalAggregationNode.getOutputSymbols()) {
                Object originalColumn = symbolMappings.get(originalAggOutputSymbol.getName());
                if (originalColumn instanceof ColumnHandle) {
                    //Refer to outputs of TableScanNode
                    ColumnHandle cubeColumn = cubeColumnsMap.get(((ColumnHandle) originalColumn).getColumnName());
                    // Intermediate nodes may have a different name for the original column name
                    assignments.put(originalAggOutputSymbol, new SymbolReference(cubeColumn.getColumnName()));
                }
                else {
                    // Should be an expression and must have the same name in the new plan node
                    assignments.put(originalAggOutputSymbol, toSymbolReference(originalAggOutputSymbol));
                }
            }
            planNode = new ProjectNode(idAllocator.getNextId(),
                    planNode,
                    new Assignments(assignments
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, entry -> castToRowExpression(entry.getValue())))));
        }
        return planNode;
    }

    private static Expression rewriteSymbolReferenceUsingColumnName(Expression expression, Map<String, Object> mapping)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Map<String, Object>>()
        {
            @Override
            public Expression rewriteSymbolReference(SymbolReference node, Map<String, Object> symbolMapping, ExpressionTreeRewriter<Map<String, Object>> treeRewriter)
            {
                ColumnHandle columnHandle = (ColumnHandle) symbolMapping.get(node.getName());
                return new SymbolReference(columnHandle.getColumnName());
            }
        }, expression, mapping);
    }

    public CubeRewriteResult createScanNode(AggregationNode originalAggregationNode, PlanNode filterNode, TableHandle cubeTableHandle, Map<String, ColumnHandle> cubeColumnsMap)
    {
        Set<Symbol> cubeScanSymbols = new HashSet<>();
        Map<Symbol, ColumnHandle> symbolAssignments = new HashMap<>();
        Set<CubeRewriteResult.DimensionSource> dimensionSymbols = new HashSet<>();
        Set<CubeRewriteResult.AggregatorSource> aggregationColumns = new HashSet<>();
        Set<CubeRewriteResult.AverageAggregatorSource> averageAggregationColumns = new HashSet<>();
        Map<Symbol, ColumnMetadata> symbolMetadataMap = new HashMap<>();

        Set<Symbol> filterSymbols = new HashSet<>();
        if (filterNode != null) {
            filterSymbols.addAll(SymbolsExtractor.extractUnique(((FilterNode) filterNode).getPredicate()));
        }
        for (Symbol filterSymbol : filterSymbols) {
            if (symbolMappings.containsKey(filterSymbol.getName()) && symbolMappings.get(filterSymbol.getName()) instanceof ColumnHandle) {
                //output symbol references of the columns in original table
                ColumnHandle originalColumn = (ColumnHandle) symbolMappings.get(filterSymbol.getName());
                ColumnHandle cubeScanColumn = cubeColumnsMap.get(originalColumn.getColumnName());
                Symbol cubeScanSymbol = new Symbol(cubeScanColumn.getColumnName());
                cubeScanSymbols.add(cubeScanSymbol);
                symbolAssignments.put(cubeScanSymbol, cubeScanColumn);
                dimensionSymbols.add(new CubeRewriteResult.DimensionSource(filterSymbol, cubeScanSymbol));
            }
        }

        for (Symbol originalAggOutputSymbol : originalAggregationNode.getOutputSymbols()) {
            if (symbolMappings.containsKey(originalAggOutputSymbol.getName()) && symbolMappings.get(originalAggOutputSymbol.getName()) instanceof ColumnHandle) {
                //output symbol references of the columns in original table - column part of group by clause
                ColumnHandle originalColumn = (ColumnHandle) symbolMappings.get(originalAggOutputSymbol.getName());
                ColumnHandle cubeScanColumn = cubeColumnsMap.get(originalColumn.getColumnName());
                Symbol cubeScanSymbol = new Symbol(cubeScanColumn.getColumnName());
                cubeScanSymbols.add(cubeScanSymbol);
                symbolAssignments.put(cubeScanSymbol, cubeScanColumn);
                dimensionSymbols.add(new CubeRewriteResult.DimensionSource(originalAggOutputSymbol, cubeScanSymbol));
            }
            else if (originalAggregationNode.getAggregations().containsKey(originalAggOutputSymbol)) {
                //output symbol is mapped to an aggregation
                AggregationNode.Aggregation aggregation = originalAggregationNode.getAggregations().get(originalAggOutputSymbol);
                String aggFunction = aggregation.getSignature().getName();
                List<Expression> arguments = aggregation.getArguments() == null ? null : aggregation.getArguments()
                        .stream()
                        .map(OriginalExpressionUtils::castToExpression).collect(Collectors.toList());
                if (arguments != null && !arguments.isEmpty() && (!(arguments.get(0) instanceof SymbolReference))) {
                    log.info("Not a symbol reference in aggregation function. Agg Function = %s, Arguments = %s", aggFunction, arguments);
                    continue;
                }
                Object mappedValue = arguments == null || arguments.isEmpty() ? null : symbolMappings.get(((SymbolReference) arguments.get(0)).getName());
                if (mappedValue == null || (mappedValue instanceof LongLiteral && ((LongLiteral) mappedValue).getValue() == 1)) {
                    // COUNT aggregation
                    if (StarTreeAggregationRule.COUNT.equals(aggFunction) && !aggregation.isDistinct()) {
                        // COUNT(1)
                        AggregationSignature aggregationSignature = AggregationSignature.count();
                        String cubeColumnName = cubeMetadata.getColumn(aggregationSignature)
                                .orElseThrow(() -> new PrestoException(CUBE_ERROR, "Cannot find column associated with aggregation " + aggregationSignature));
                        ColumnHandle cubeColHandle = cubeColumnsMap.get(cubeColumnName);
                        if (!symbolAssignments.containsValue(cubeColHandle)) {
                            ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, cubeTableHandle, cubeColHandle);
                            cubeScanSymbols.add(originalAggOutputSymbol);
                            symbolMetadataMap.put(originalAggOutputSymbol, columnMetadata);
                            symbolAssignments.put(originalAggOutputSymbol, cubeColHandle);
                            aggregationColumns.add(new CubeRewriteResult.AggregatorSource(originalAggOutputSymbol, originalAggOutputSymbol));
                        }
                    }
                }
                else if (mappedValue instanceof ColumnHandle) {
                    String originalColumnName = ((ColumnHandle) mappedValue).getColumnName();
                    boolean distinct = originalAggregationNode.getAggregations().get(originalAggOutputSymbol).isDistinct();
                    switch (aggFunction) {
                        case "min":
                        case "max":
                        case "sum":
                        case "count":
                            AggregationSignature aggregationSignature = new AggregationSignature(aggFunction, originalColumnName, distinct);
                            String cubeColumnName = cubeMetadata.getColumn(aggregationSignature)
                                    .orElseThrow(() -> new PrestoException(CUBE_ERROR, "Cannot find column associated with aggregation " + aggregationSignature));
                            ColumnHandle cubeColHandle = cubeColumnsMap.get(cubeColumnName);
                            if (!symbolAssignments.containsValue(cubeColHandle)) {
                                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, cubeTableHandle, cubeColHandle);
                                symbolAssignments.put(originalAggOutputSymbol, cubeColHandle);
                                symbolMetadataMap.put(originalAggOutputSymbol, columnMetadata);
                                cubeScanSymbols.add(originalAggOutputSymbol);
                                aggregationColumns.add(new CubeRewriteResult.AggregatorSource(originalAggOutputSymbol, originalAggOutputSymbol));
                            }
                            break;
                        case "avg":
                            AggregationSignature sumSignature = new AggregationSignature(AggregationSignature.SUM_FUNCTION_NAME, originalColumnName, distinct);
                            String sumColumnName = cubeMetadata.getColumn(sumSignature)
                                    .orElseThrow(() -> new PrestoException(CUBE_ERROR, "Cannot find column associated with aggregation " + sumSignature));
                            ColumnHandle sumColumnHandle = cubeColumnsMap.get(sumColumnName);
                            Symbol sumSymbol = null;
                            if (!symbolAssignments.containsValue(sumColumnHandle)) {
                                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, cubeTableHandle, sumColumnHandle);
                                sumSymbol = symbolAllocator.newSymbol("sum_" + originalColumnName + "_" + originalAggOutputSymbol.getName(), columnMetadata.getType());
                                cubeScanSymbols.add(sumSymbol);
                                symbolAssignments.put(sumSymbol, sumColumnHandle);
                                symbolMetadataMap.put(sumSymbol, columnMetadata);
                                aggregationColumns.add(new CubeRewriteResult.AggregatorSource(sumSymbol, sumSymbol));
                            }
                            else {
                                for (Map.Entry<Symbol, ColumnHandle> assignment : symbolAssignments.entrySet()) {
                                    if (assignment.getValue().equals(sumColumnHandle)) {
                                        sumSymbol = assignment.getKey();
                                        break;
                                    }
                                }
                            }
                            AggregationSignature countSignature = new AggregationSignature(AggregationSignature.COUNT_FUNCTION_NAME, originalColumnName, distinct);
                            String countColumnName = cubeMetadata.getColumn(countSignature)
                                    .orElseThrow(() -> new PrestoException(CUBE_ERROR, "Cannot find column associated with aggregation " + countSignature));
                            ColumnHandle countColumnHandle = cubeColumnsMap.get(countColumnName);
                            Symbol countSymbol = null;
                            if (!symbolAssignments.containsValue(countColumnHandle)) {
                                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, cubeTableHandle, countColumnHandle);
                                countSymbol = symbolAllocator.newSymbol("count_" + originalColumnName + "_" + originalAggOutputSymbol.getName(), columnMetadata.getType());
                                cubeScanSymbols.add(countSymbol);
                                symbolAssignments.put(countSymbol, countColumnHandle);
                                symbolMetadataMap.put(countSymbol, columnMetadata);
                                aggregationColumns.add(new CubeRewriteResult.AggregatorSource(countSymbol, countSymbol));
                            }
                            else {
                                for (Map.Entry<Symbol, ColumnHandle> assignment : symbolAssignments.entrySet()) {
                                    if (assignment.getValue().equals(countColumnHandle)) {
                                        countSymbol = assignment.getKey();
                                        break;
                                    }
                                }
                            }
                            averageAggregationColumns.add(new CubeRewriteResult.AverageAggregatorSource(originalAggOutputSymbol, sumSymbol, countSymbol));
                            break;
                        default:
                            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Unsupported aggregation function " + aggFunction);
                    }
                }
                else {
                    log.info("Aggregation function argument is not a Column Handle. Agg Function = %s, Arguments = %s", aggFunction, arguments);
                }
            }
        }
        TableScanNode tableScanNode = TableScanNode.newInstance(idAllocator.getNextId(), cubeTableHandle, new ArrayList<>(cubeScanSymbols), symbolAssignments, ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false);
        return new CubeRewriteResult(tableScanNode, symbolMetadataMap, dimensionSymbols, aggregationColumns, averageAggregationColumns);
    }
}
