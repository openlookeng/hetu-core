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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.hetu.core.spi.cube.CubeAggregateFunction;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.aggregator.AggregationSignature;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ColumnNotFoundException;
import io.prestosql.spi.connector.CubeNotFoundException;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.FunctionHandle;
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
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.hetu.core.spi.cube.CubeAggregateFunction.COUNT;
import static io.hetu.core.spi.cube.CubeAggregateFunction.SUM;
import static io.prestosql.spi.StandardErrorCode.CUBE_ERROR;
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
    private final Map<String, Symbol> rewrittenMappings = new HashMap<>();
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
        QualifiedObjectName starTreeTableName = QualifiedObjectName.valueOf(cubeMetadata.getCubeName());
        TableHandle cubeTableHandle = metadata.getTableHandle(session, starTreeTableName)
                .orElseThrow(() -> new CubeNotFoundException(starTreeTableName.toString()));
        Map<String, ColumnHandle> cubeColumnsMap = metadata.getColumnHandles(session, cubeTableHandle);
        TableMetadata cubeTableMetadata = metadata.getTableMetadata(session, cubeTableHandle);
        List<ColumnMetadata> cubeColumnMetadataList = cubeTableMetadata.getColumns();
        // Add group by
        List<Symbol> groupings = new ArrayList<>(originalAggregationNode.getGroupingKeys().size());
        for (Symbol symbol : originalAggregationNode.getGroupingKeys()) {
            Object column = symbolMappings.get(symbol.getName());
            if (column instanceof ColumnHandle) {
                groupings.add(new Symbol(((ColumnHandle) column).getColumnName()));
            }
        }

        Set<String> cubeGroups = cubeMetadata.getGroup();
        boolean exactGroupsMatch = false;
        if (groupings.size() == cubeGroups.size()) {
            exactGroupsMatch = groupings.stream().map(Symbol::getName).map(String::toLowerCase).allMatch(cubeGroups::contains);
        }

        CubeRewriteResult cubeRewriteResult = createScanNode(originalAggregationNode, filterNode, cubeTableHandle, cubeColumnsMap, cubeColumnMetadataList, exactGroupsMatch);
        PlanNode planNode = cubeRewriteResult.getTableScanNode();

        // Add filter node
        if (filterNode != null) {
            Expression expression = castToExpression(((FilterNode) filterNode).getPredicate());
            expression = rewriteExpression(expression, rewrittenMappings);
            planNode = new FilterNode(idAllocator.getNextId(), planNode, castToRowExpression(expression));
        }

        if (!exactGroupsMatch) {
            Map<Symbol, Symbol> cubeScanToAggOutputMap = new HashMap<>();
            // Rewrite AggregationNode using Cube table
            ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregationsBuilder = ImmutableMap.builder();
            for (CubeRewriteResult.AggregatorSource aggregatorSource : cubeRewriteResult.getAggregationColumns()) {
                ColumnHandle cubeColHandle = cubeRewriteResult.getTableScanNode().getAssignments().get(aggregatorSource.getScanSymbol());
                ColumnMetadata cubeColumnMetadata = cubeRewriteResult.getSymbolMetadataMap().get(aggregatorSource.getScanSymbol());
                Type type = cubeColumnMetadata.getType();
                AggregationSignature aggregationSignature = cubeMetadata.getAggregationSignature(cubeColumnMetadata.getName())
                        .orElseThrow(() -> new ColumnNotFoundException(new SchemaTableName(starTreeTableName.getSchemaName(), starTreeTableName.getObjectName()), cubeColHandle.getColumnName()));
                String aggFunction = COUNT.getName().equals(aggregationSignature.getFunction()) ? "sum" : aggregationSignature.getFunction();
                SymbolReference argument = toSymbolReference(aggregatorSource.getScanSymbol());
                FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupFunction(aggFunction, TypeSignatureProvider.fromTypeSignatures(type.getTypeSignature()));
                cubeScanToAggOutputMap.put(aggregatorSource.getScanSymbol(), aggregatorSource.getOriginalAggSymbol());
                aggregationsBuilder.put(aggregatorSource.getOriginalAggSymbol(), new AggregationNode.Aggregation(
                        new CallExpression(
                                aggFunction,
                                functionHandle,
                                type,
                                ImmutableList.of(OriginalExpressionUtils.castToRowExpression(argument))),
                        ImmutableList.of(OriginalExpressionUtils.castToRowExpression(argument)),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
            }

            List<Symbol> groupingKeys = originalAggregationNode.getGroupingKeys()
                    .stream()
                    .map(Symbol::getName)
                    .map(rewrittenMappings::get)
                    .collect(Collectors.toList());

            planNode = new AggregationNode(idAllocator.getNextId(),
                    planNode,
                    aggregationsBuilder.build(),
                    singleGroupingSet(groupingKeys),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty(),
                    AggregationNode.AggregationType.HASH,
                    Optional.empty());
            AggregationNode aggNode = (AggregationNode) planNode;

            if (!cubeRewriteResult.getAvgAggregationColumns().isEmpty()) {
                if (!cubeRewriteResult.getComputeAvgDividingSumByCount()) {
                    Map<Symbol, Expression> aggregateAssignments = new HashMap<>();
                    for (CubeRewriteResult.AggregatorSource aggregatorSource : cubeRewriteResult.getAggregationColumns()) {
                        aggregateAssignments.put(aggregatorSource.getOriginalAggSymbol(), toSymbolReference(aggregatorSource.getScanSymbol()));
                    }
                    planNode = new ProjectNode(idAllocator.getNextId(),
                            aggNode,
                            new Assignments(aggregateAssignments
                                    .entrySet()
                                    .stream()
                                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> castToRowExpression(entry.getValue())))));
                }
                else {
                    // If there was an AVG aggregation, map it to AVG = SUM/COUNT
                    Map<Symbol, Expression> projections = new HashMap<>();

                    aggNode.getGroupingKeys().forEach(symbol -> projections.put(symbol, toSymbolReference(symbol)));
                    aggNode.getAggregations().keySet()
                            .stream()
                            .filter(symbol -> symbolMappings.containsValue(symbol.getName()))
                            .forEach(aggSymbol -> projections.put(aggSymbol, toSymbolReference(aggSymbol)));

                    // Add AVG = SUM / COUNT
                    for (CubeRewriteResult.AverageAggregatorSource avgAggSource : cubeRewriteResult.getAvgAggregationColumns()) {
                        Symbol sumSymbol = cubeScanToAggOutputMap.get(avgAggSource.getSum());
                        Symbol countSymbol = cubeScanToAggOutputMap.get(avgAggSource.getCount());
                        Type avgResultType = typeProvider.get(avgAggSource.getOriginalAggSymbol());
                        ArithmeticBinaryExpression division = new ArithmeticBinaryExpression(
                                ArithmeticBinaryExpression.Operator.DIVIDE,
                                new Cast(toSymbolReference(sumSymbol), avgResultType.getTypeSignature().toString()),
                                new Cast(toSymbolReference(countSymbol), avgResultType.getTypeSignature().toString()));
                        projections.put(avgAggSource.getOriginalAggSymbol(), division);
                    }
                    planNode = new ProjectNode(idAllocator.getNextId(),
                            aggNode,
                            new Assignments(projections
                                    .entrySet()
                                    .stream()
                                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> castToRowExpression(entry.getValue())))));
                }
            }
        }

        // Safety check to remove redundant symbols and rename original column names to intermediate names
        if (!planNode.getOutputSymbols().equals(originalAggregationNode.getOutputSymbols())) {
            // Map new symbol names to the old symbols
            Map<Symbol, Expression> assignments = new HashMap<>();
            Set<Symbol> planNodeOutput = new HashSet<>(planNode.getOutputSymbols());
            for (Symbol originalAggOutputSymbol : originalAggregationNode.getOutputSymbols()) {
                if (!planNodeOutput.contains(originalAggOutputSymbol)) {
                    // Must be grouping key
                    assignments.put(originalAggOutputSymbol, toSymbolReference(rewrittenMappings.get(originalAggOutputSymbol.getName())));
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

    private static Expression rewriteExpression(Expression expression, Map<String, Symbol> mapping)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Map<String, Symbol>>()
        {
            @Override
            public Expression rewriteSymbolReference(SymbolReference node, Map<String, Symbol> rewrittenMappings, ExpressionTreeRewriter<Map<String, Symbol>> treeRewriter)
            {
                return toSymbolReference(rewrittenMappings.get(node.getName()));
            }
        }, expression, mapping);
    }

    public CubeRewriteResult createScanNode(AggregationNode originalAggregationNode, PlanNode filterNode, TableHandle cubeTableHandle,
            Map<String, ColumnHandle> cubeColumnsMap, List<ColumnMetadata> cubeColumnsMetadata, boolean exactGroupsMatch)
    {
        Set<Symbol> cubeScanSymbols = new HashSet<>();
        Map<Symbol, ColumnHandle> symbolAssignments = new HashMap<>();
        Set<CubeRewriteResult.DimensionSource> dimensionSymbols = new HashSet<>();
        Set<CubeRewriteResult.AggregatorSource> aggregationColumns = new HashSet<>();
        Set<CubeRewriteResult.AverageAggregatorSource> averageAggregationColumns = new HashSet<>();
        Map<Symbol, ColumnMetadata> symbolMetadataMap = new HashMap<>();
        Map<String, ColumnMetadata> columnMetadataMap = cubeColumnsMetadata.stream().collect(Collectors.toMap(ColumnMetadata::getName, Function.identity()));
        boolean computeAvgDividingSumByCount = true;
        Set<Symbol> filterSymbols = new HashSet<>();
        if (filterNode != null) {
            filterSymbols.addAll(SymbolsExtractor.extractUnique(((FilterNode) filterNode).getPredicate()));
        }

        for (Symbol filterSymbol : filterSymbols) {
            if (symbolMappings.containsKey(filterSymbol.getName()) && symbolMappings.get(filterSymbol.getName()) instanceof ColumnHandle) {
                //output symbol references of the columns in original table
                ColumnHandle originalColumn = (ColumnHandle) symbolMappings.get(filterSymbol.getName());
                ColumnHandle cubeScanColumn = cubeColumnsMap.get(originalColumn.getColumnName());
                ColumnMetadata columnMetadata = columnMetadataMap.get(cubeScanColumn.getColumnName());
                Symbol cubeScanSymbol = symbolAllocator.newSymbol(cubeScanColumn.getColumnName(), columnMetadata.getType());
                cubeScanSymbols.add(cubeScanSymbol);
                symbolAssignments.put(cubeScanSymbol, cubeScanColumn);
                symbolMetadataMap.put(cubeScanSymbol, columnMetadata);
                rewrittenMappings.put(filterSymbol.getName(), cubeScanSymbol);
                dimensionSymbols.add(new CubeRewriteResult.DimensionSource(filterSymbol, cubeScanSymbol));
            }
        }

        for (Symbol originalAggOutputSymbol : originalAggregationNode.getOutputSymbols()) {
            if (symbolMappings.containsKey(originalAggOutputSymbol.getName()) && symbolMappings.get(originalAggOutputSymbol.getName()) instanceof ColumnHandle) {
                //output symbol references of the columns in original table - column part of group by clause
                ColumnHandle originalColumn = (ColumnHandle) symbolMappings.get(originalAggOutputSymbol.getName());
                ColumnHandle cubeScanColumn = cubeColumnsMap.get(originalColumn.getColumnName());
                ColumnMetadata columnMetadata = columnMetadataMap.get(cubeScanColumn.getColumnName());
                if (!symbolAssignments.containsValue(cubeScanColumn)) {
                    Symbol cubeScanSymbol = symbolAllocator.newSymbol(cubeScanColumn.getColumnName(), columnMetadata.getType());
                    cubeScanSymbols.add(cubeScanSymbol);
                    symbolAssignments.put(cubeScanSymbol, cubeScanColumn);
                    symbolMetadataMap.put(cubeScanSymbol, columnMetadata);
                    rewrittenMappings.put(originalAggOutputSymbol.getName(), cubeScanSymbol);
                    dimensionSymbols.add(new CubeRewriteResult.DimensionSource(originalAggOutputSymbol, cubeScanSymbol));
                }
                else {
                    Symbol cubeScanSymbol = symbolAssignments.keySet()
                            .stream()
                            .filter(key -> cubeScanColumn.equals(symbolAssignments.get(key)))
                            .findFirst().get();
                    rewrittenMappings.put(originalAggOutputSymbol.getName(), cubeScanSymbol);
                    dimensionSymbols.add(new CubeRewriteResult.DimensionSource(originalAggOutputSymbol, cubeScanSymbol));
                }
            }
            else if (originalAggregationNode.getAggregations().containsKey(originalAggOutputSymbol)) {
                //output symbol is mapped to an aggregation
                AggregationNode.Aggregation aggregation = originalAggregationNode.getAggregations().get(originalAggOutputSymbol);
                String aggFunction = aggregation.getFunctionCall().getDisplayName();
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
                    if (CubeAggregateFunction.COUNT.getName().equals(aggFunction) && !aggregation.isDistinct()) {
                        // COUNT 1
                        AggregationSignature aggregationSignature = AggregationSignature.count();
                        String cubeColumnName = cubeMetadata.getColumn(aggregationSignature)
                                .orElseThrow(() -> new PrestoException(CUBE_ERROR, "Cannot find column associated with aggregation " + aggregationSignature));
                        ColumnHandle cubeColHandle = cubeColumnsMap.get(cubeColumnName);
                        if (!symbolAssignments.containsValue(cubeColHandle)) {
                            ColumnMetadata columnMetadata = columnMetadataMap.get(cubeColHandle.getColumnName());
                            Symbol cubeScanSymbol = symbolAllocator.newSymbol(cubeColHandle.getColumnName(), columnMetadata.getType());
                            cubeScanSymbols.add(cubeScanSymbol);
                            symbolMetadataMap.put(cubeScanSymbol, columnMetadata);
                            symbolAssignments.put(cubeScanSymbol, cubeColHandle);
                            rewrittenMappings.put(originalAggOutputSymbol.getName(), cubeScanSymbol);
                            aggregationColumns.add(new CubeRewriteResult.AggregatorSource(originalAggOutputSymbol, cubeScanSymbol));
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
                                ColumnMetadata columnMetadata = columnMetadataMap.get(cubeColHandle.getColumnName());
                                Symbol cubeScanSymbol = symbolAllocator.newSymbol(cubeColHandle.getColumnName(), columnMetadata.getType());
                                cubeScanSymbols.add(cubeScanSymbol);
                                symbolAssignments.put(cubeScanSymbol, cubeColHandle);
                                symbolMetadataMap.put(cubeScanSymbol, columnMetadata);
                                rewrittenMappings.put(originalAggOutputSymbol.getName(), cubeScanSymbol);
                                aggregationColumns.add(new CubeRewriteResult.AggregatorSource(originalAggOutputSymbol, cubeScanSymbol));
                            }
                            else {
                                ColumnMetadata columnMetadata = columnMetadataMap.get(cubeColHandle.getColumnName());
                                Symbol cubeScanSymbol = symbolAssignments.keySet()
                                        .stream()
                                        .filter(key -> cubeColHandle.equals(symbolAssignments.get(key)))
                                        .findFirst().get();
                                cubeScanSymbols.add(cubeScanSymbol);
                                symbolAssignments.put(cubeScanSymbol, cubeColHandle);
                                symbolMetadataMap.put(cubeScanSymbol, columnMetadata);
                                rewrittenMappings.put(originalAggOutputSymbol.getName(), cubeScanSymbol);
                                aggregationColumns.add(new CubeRewriteResult.AggregatorSource(originalAggOutputSymbol, cubeScanSymbol));
                            }
                            break;
                        case "avg":
                            AggregationSignature avgAggregationSignature = new AggregationSignature(aggFunction, originalColumnName, distinct);
                            if (exactGroupsMatch && cubeMetadata.getColumn(avgAggregationSignature).isPresent()) {
                                computeAvgDividingSumByCount = false;
                                String avgCubeColumnName = cubeMetadata.getColumn(avgAggregationSignature)
                                        .orElseThrow(() -> new PrestoException(CUBE_ERROR, "Cannot find column associated with aggregation " + avgAggregationSignature));
                                ColumnHandle avgCubeColHandle = cubeColumnsMap.get(avgCubeColumnName);
                                if (!symbolAssignments.containsValue(avgCubeColHandle)) {
                                    ColumnMetadata columnMetadata = columnMetadataMap.get(avgCubeColHandle.getColumnName());
                                    Symbol cubeScanSymbol = symbolAllocator.newSymbol(avgCubeColHandle.getColumnName(), columnMetadata.getType());
                                    cubeScanSymbols.add(cubeScanSymbol);
                                    symbolAssignments.put(cubeScanSymbol, avgCubeColHandle);
                                    symbolMetadataMap.put(cubeScanSymbol, columnMetadata);
                                    rewrittenMappings.put(originalAggOutputSymbol.getName(), cubeScanSymbol);
                                    aggregationColumns.add(new CubeRewriteResult.AggregatorSource(originalAggOutputSymbol, cubeScanSymbol));
                                }
                            }
                            else {
                                AggregationSignature sumSignature = new AggregationSignature(SUM.getName(), originalColumnName, distinct);
                                String sumColumnName = cubeMetadata.getColumn(sumSignature)
                                        .orElseThrow(() -> new PrestoException(CUBE_ERROR, "Cannot find column associated with aggregation " + sumSignature));
                                ColumnHandle sumColumnHandle = cubeColumnsMap.get(sumColumnName);
                                Symbol sumSymbol = null;
                                if (!symbolAssignments.containsValue(sumColumnHandle)) {
                                    ColumnMetadata columnMetadata = columnMetadataMap.get(sumColumnHandle.getColumnName());
                                    sumSymbol = symbolAllocator.newSymbol("sum_" + originalColumnName + "_" + originalAggOutputSymbol.getName(), columnMetadata.getType());
                                    cubeScanSymbols.add(sumSymbol);
                                    symbolAssignments.put(sumSymbol, sumColumnHandle);
                                    symbolMetadataMap.put(sumSymbol, columnMetadata);
                                    rewrittenMappings.put(sumSymbol.getName(), sumSymbol);
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
                                AggregationSignature countSignature = new AggregationSignature(COUNT.getName(), originalColumnName, distinct);
                                String countColumnName = cubeMetadata.getColumn(countSignature)
                                        .orElseThrow(() -> new PrestoException(CUBE_ERROR, "Cannot find column associated with aggregation " + countSignature));
                                ColumnHandle countColumnHandle = cubeColumnsMap.get(countColumnName);
                                Symbol countSymbol = null;
                                if (!symbolAssignments.containsValue(countColumnHandle)) {
                                    ColumnMetadata columnMetadata = columnMetadataMap.get(countColumnHandle.getColumnName());
                                    countSymbol = symbolAllocator.newSymbol("count_" + originalColumnName + "_" + originalAggOutputSymbol.getName(), columnMetadata.getType());
                                    cubeScanSymbols.add(countSymbol);
                                    symbolAssignments.put(countSymbol, countColumnHandle);
                                    symbolMetadataMap.put(countSymbol, columnMetadata);
                                    rewrittenMappings.put(countSymbol.getName(), countSymbol);
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
                            }
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

        //Scan output order is important for partitioned cubes. Otherwise, incorrect results may be produced.
        //Refer: https://gitee.com/openlookeng/hetu-core/issues/I4LAYC
        List<Symbol> scanOutput = new ArrayList<>(cubeScanSymbols);
        scanOutput.sort(Comparator.comparingInt(outSymbol -> cubeColumnsMetadata.indexOf(symbolMetadataMap.get(outSymbol))));
        TableScanNode tableScanNode = TableScanNode.newInstance(idAllocator.getNextId(), cubeTableHandle, scanOutput, symbolAssignments, ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false);
        return new CubeRewriteResult(tableScanNode, symbolMetadataMap, dimensionSymbols, aggregationColumns, averageAggregationColumns, computeAvgDividingSumByCount);
    }
}
