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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.hetu.core.spi.cube.CubeAggregateFunction;
import io.hetu.core.spi.cube.CubeFilter;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.aggregator.AggregationSignature;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.Session;
import io.prestosql.expressions.RowExpressionRewriter;
import io.prestosql.expressions.RowExpressionTreeRewriter;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ColumnNotFoundException;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.ExpressionDomainTranslator;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AssignmentUtils;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.SymbolReference;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static io.hetu.core.spi.cube.CubeAggregateFunction.COUNT;
import static io.hetu.core.spi.cube.CubeAggregateFunction.SUM;
import static io.prestosql.spi.StandardErrorCode.CUBE_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.connector.StandardWarningCode.EXPIRED_CUBE;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class CubeOptimizer
{
    private static final Logger log = Logger.get(CubeOptimizer.class);
    private final List<ProjectNode> intermediateProjections = new ArrayList<>();
    private final Map<String, ColumnWithTable> originalPlanMappings = new HashMap<>();
    private final Map<String, ColumnWithTable> columnRewritesMap = new HashMap<>();
    private final Map<ColumnWithTable, Symbol> optimizedPlanMappings = new HashMap<>();
    private Rule.Context context;
    private Metadata metadata;
    private CubeMetaStore cubeMetaStore;
    private List<CubeMetadata> matchingMetadataList = new ArrayList<>();
    private CubeMetadata cubeMetadata;
    private AggregationNode aggregationNode;
    private FilterNode filterNode;
    private JoinNode joinNode;
    private String sourceTableName;
    private TableScanNode sourceTableScanNode;
    private TableHandle sourceTableHandle;
    private TableMetadata sourceTableMetadata;
    private final Map<String, ColumnHandle> sourceTableColumnMap = new HashMap<>();
    private TableScanNode cubeScanNode;
    private TableHandle cubeTableHandle;
    private TableMetadata cubeTableMetadata;
    private final Map<String, ColumnHandle> cubeColumnsMap = new HashMap<>();
    private final Map<AggregationSignature, Symbol> originalAggregationsMap = new HashMap<>();
    private final Set<String> groupBy = new HashSet<>();
    private final Set<String> dimensions = new HashSet<>();
    private final Set<String> filterColumns = new HashSet<>();
    private boolean groupByFromOtherTable;

    private CubeOptimizer()
    {
        //private constructor
    }

    public static CubeOptimizer forPlan(
            Rule.Context context,
            Metadata metadata,
            CubeMetaStore cubeMetaStore,
            AggregationNode aggNode,
            List<ProjectNode> projections,
            FilterNode filterNode,
            TableScanNode tableScanNode)
    {
        CubeOptimizer optimizer = new CubeOptimizer();
        optimizer.context = requireNonNull(context, "Context is null");
        optimizer.metadata = requireNonNull(metadata, "Metadata is null");
        optimizer.cubeMetaStore = requireNonNull(cubeMetaStore, "CubeMetaStore is null");
        optimizer.aggregationNode = requireNonNull(aggNode, "Aggregation node is null");
        optimizer.intermediateProjections.addAll(requireNonNull(projections, "Project nodes are null"));
        optimizer.filterNode = filterNode;
        optimizer.sourceTableScanNode = requireNonNull(tableScanNode, "TableScanNode is null");
        return optimizer;
    }

    public static CubeOptimizer forPlan(
            Rule.Context context,
            Metadata metadata,
            CubeMetaStore cubeMetaStore,
            AggregationNode aggNode,
            List<ProjectNode> projections,
            FilterNode filterNode,
            JoinNode joinNode)
    {
        CubeOptimizer optimizer = new CubeOptimizer();
        optimizer.context = requireNonNull(context, "Context is null");
        optimizer.metadata = requireNonNull(metadata, "Metadata is null");
        optimizer.cubeMetaStore = requireNonNull(cubeMetaStore, "CubeMetaStore is null");
        optimizer.aggregationNode = requireNonNull(aggNode, "Aggregation node is null");
        optimizer.intermediateProjections.addAll(requireNonNull(projections, "Project nodes are null"));
        optimizer.filterNode = filterNode;
        optimizer.joinNode = requireNonNull(joinNode, "Join node is null");
        return optimizer;
    }

    public Optional<PlanNode> optimize()
    {
        parse();
        //Match based on filter conditions
        if (matchingMetadataList.isEmpty()) {
            return Optional.empty();
        }

        LongSupplier lastModifiedTimeSupplier = metadata.getTableLastModifiedTimeSupplier(context.getSession(), sourceTableHandle);
        if (lastModifiedTimeSupplier == null) {
            context.getWarningCollector().add(new PrestoWarning(EXPIRED_CUBE, "Unable to identify last modified time of " + sourceTableMetadata.getTable().toString() + ". Ignoring star tree cubes."));
            return Optional.empty();
        }

        //Filter out cubes that were created before the source table was updated
        long lastModifiedTime = lastModifiedTimeSupplier.getAsLong();
        //There was a problem retrieving last modified time, we should skip using star tree rather than failing the query
        if (lastModifiedTime == -1L) {
            return Optional.empty();
        }

        matchingMetadataList = matchingMetadataList.stream()
                .filter(cubeMetadata -> cubeMetadata.getSourceTableLastUpdatedTime() >= lastModifiedTime)
                .collect(Collectors.toList());

        if (matchingMetadataList.isEmpty()) {
            context.getWarningCollector().add(new PrestoWarning(EXPIRED_CUBE, sourceTableMetadata.getTable().toString() + " has been modified after creating cubes. Ignoring expired cubes."));
            return Optional.empty();
        }
        //If multiple matching cubes found, then cube with fewer group by columns (could also mean least number of rows) and pick the most recent cube
        Comparator<CubeMetadata> byGroupSize = Comparator.comparingInt(cm -> cm.getGroup().size());
        Comparator<CubeMetadata> byLastModifiedTime = Comparator.comparingLong(CubeMetadata::getLastUpdatedTime).reversed();
        matchingMetadataList.sort(byGroupSize.thenComparing(byLastModifiedTime));
        cubeMetadata = matchingMetadataList.get(0);
        return Optional.ofNullable(rewrite());
    }

    PlanNode rewrite()
    {
        cubeTableHandle = metadata.getTableHandle(context.getSession(), QualifiedObjectName.valueOf(cubeMetadata.getCubeName())).get();
        cubeTableMetadata = metadata.getTableMetadata(context.getSession(), cubeTableHandle);
        cubeColumnsMap.putAll(metadata.getColumnHandles(context.getSession(), cubeTableHandle));
        CubeRewriteResult cubeRewriteResult = createScanNode();
        //Rewrite source table mapping with cube table mapping
        originalPlanMappings.forEach((symbol, columnWithTable) -> {
            ColumnWithTable rewrite = columnWithTable.getFQTableName().equalsIgnoreCase(sourceTableName) ? new ColumnWithTable(cubeTableMetadata.getQualifiedName().toString(), columnWithTable.getColumnName()) : columnWithTable;
            columnRewritesMap.put(symbol, rewrite);
        });
        PlanNode planNode = rewriteNodeRecursively(joinNode);
        planNode = rewriteFilterNode(planNode);
        planNode = rewriteAggregationNode(cubeRewriteResult, planNode);
        planNode = rewriteSymbols(planNode);
        return planNode;
    }

    public CubeRewriteResult createScanNode()
    {
        Set<Symbol> cubeScanSymbols = new HashSet<>();
        Map<Symbol, ColumnHandle> cubeColumnHandles = new HashMap<>();
        Set<CubeRewriteResult.DimensionSource> dimensionSymbols = new HashSet<>();
        Set<CubeRewriteResult.AggregatorSource> aggregationColumns = new HashSet<>();
        Set<CubeRewriteResult.AverageAggregatorSource> averageAggregationColumns = new HashSet<>();
        Map<Symbol, ColumnMetadata> symbolMetadataMap = new HashMap<>();

        SymbolAllocator symbolAllocator = context.getSymbolAllocator();

        dimensions.forEach(dimension -> {
            //assumed that source table dimension is same in cube table as well
            ColumnWithTable sourceColumn = originalPlanMappings.get(dimension);
            ColumnHandle cubeColumn = cubeColumnsMap.get(sourceColumn.getColumnName());
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(context.getSession(), cubeTableHandle, cubeColumn);
            Symbol cubeScanSymbol = symbolAllocator.newSymbol(cubeColumn.getColumnName(), columnMetadata.getType());
            cubeScanSymbols.add(cubeScanSymbol);
            cubeColumnHandles.put(cubeScanSymbol, cubeColumn);
            dimensionSymbols.add(new CubeRewriteResult.DimensionSource(cubeScanSymbol, cubeScanSymbol));
        });

        filterColumns.forEach(filterColumn -> {
            ColumnHandle cubeColumn = cubeColumnsMap.get(filterColumn);
            if (cubeColumn == null) {
                //If not part of the cubeColumns map, then the filter column must be part of cube source filter predicate.
                //Predicate rewrite will take care of remove those columns from predicate
                return;
            }
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(context.getSession(), cubeTableHandle, cubeColumn);
            Symbol cubeScanSymbol = symbolAllocator.newSymbol(cubeColumn.getColumnName(), columnMetadata.getType());
            cubeScanSymbols.add(cubeScanSymbol);
            cubeColumnHandles.put(cubeScanSymbol, cubeColumn);
            dimensionSymbols.add(new CubeRewriteResult.DimensionSource(cubeScanSymbol, cubeScanSymbol));
        });

        //process all aggregations except avg.
        originalAggregationsMap.forEach((originalAgg, originalAggOutputSymbol) -> {
            if (originalAgg.getFunction().equalsIgnoreCase(CubeAggregateFunction.AVG.toString())) {
                //skip average aggregation
                return;
            }
            String cubeColumnName = cubeMetadata.getColumn(originalAgg)
                    .orElseThrow(() -> new PrestoException(CUBE_ERROR, "Cannot find column associated with aggregation " + originalAgg));
            ColumnHandle cubeColHandle = cubeColumnsMap.get(cubeColumnName);
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(context.getSession(), cubeTableHandle, cubeColHandle);
            Symbol cubeScanSymbol = symbolAllocator.newSymbol(cubeColHandle.getColumnName(), columnMetadata.getType());
            cubeScanSymbols.add(cubeScanSymbol);
            cubeColumnHandles.put(cubeScanSymbol, cubeColHandle);
            symbolMetadataMap.put(originalAggOutputSymbol, columnMetadata);
            aggregationColumns.add(new CubeRewriteResult.AggregatorSource(originalAggOutputSymbol, cubeScanSymbol));
        });

        boolean computeAvgDividingSumByCount = true;
        //process average aggregation
        for (AggregationSignature originalAgg : originalAggregationsMap.keySet()) {
            if (!originalAgg.getFunction().equalsIgnoreCase(CubeAggregateFunction.AVG.toString())) {
                //process only average aggregation. skip others
                continue;
            }

            Symbol originalOutputSymbol = originalAggregationsMap.get(originalAgg);
            String aggSourceColumnName = originalAgg.getDimension();
            AggregationSignature avgAggregationSignature = new AggregationSignature(originalAgg.getFunction(), aggSourceColumnName, originalAgg.isDistinct());
            //exactGroupByMatch is True iff query group by clause by does not contain columns from other table. This is extremely unlikely for Join queries
            boolean exactGroupByMatch = !groupByFromOtherTable && groupBy.size() == cubeMetadata.getGroup().size() && groupBy.stream().map(String::toLowerCase).allMatch(cubeMetadata.getGroup()::contains);
            if (exactGroupByMatch && cubeMetadata.getColumn(avgAggregationSignature).isPresent()) {
                //Use AVG column for exact group match only if Cube has it. The Cubes created before shipping
                //this optimization will not have AVG column
                computeAvgDividingSumByCount = false;
                String avgCubeColumnName = cubeMetadata.getColumn(avgAggregationSignature)
                        .orElseThrow(() -> new PrestoException(CUBE_ERROR, "Cannot find column associated with aggregation " + avgAggregationSignature));
                ColumnHandle avgCubeColHandle = cubeColumnsMap.get(avgCubeColumnName);
                if (!cubeColumnHandles.containsValue(avgCubeColHandle)) {
                    ColumnMetadata columnMetadata = metadata.getColumnMetadata(context.getSession(), cubeTableHandle, avgCubeColHandle);
                    cubeColumnHandles.put(originalOutputSymbol, avgCubeColHandle);
                    symbolMetadataMap.put(originalOutputSymbol, columnMetadata);
                    cubeScanSymbols.add(originalOutputSymbol);
                    aggregationColumns.add(new CubeRewriteResult.AggregatorSource(originalOutputSymbol, originalOutputSymbol));
                }
            }
            else {
                AggregationSignature sumSignature = new AggregationSignature(SUM.getName(), aggSourceColumnName, originalAgg.isDistinct());
                String sumColumnName = cubeMetadata.getColumn(sumSignature)
                        .orElseThrow(() -> new PrestoException(CUBE_ERROR, "Cannot find column associated with aggregation " + sumSignature));
                ColumnHandle sumColumnHandle = cubeColumnsMap.get(sumColumnName);
                ColumnMetadata sumColumnMetadata = metadata.getColumnMetadata(context.getSession(), cubeTableHandle, sumColumnHandle);
                Symbol sumColumnSymbol = cubeColumnHandles.entrySet()
                        .stream()
                        .filter(entry -> sumColumnHandle.equals(entry.getValue()))
                        .findAny()
                        .map(Map.Entry::getKey)
                        .orElse(symbolAllocator.newSymbol("sum_" + aggSourceColumnName + "_" + originalOutputSymbol.getName(), sumColumnMetadata.getType()));
                if (!cubeScanSymbols.contains(sumColumnSymbol)) {
                    cubeScanSymbols.add(sumColumnSymbol);
                    cubeColumnHandles.put(sumColumnSymbol, sumColumnHandle);
                    symbolMetadataMap.put(sumColumnSymbol, sumColumnMetadata);
                    aggregationColumns.add(new CubeRewriteResult.AggregatorSource(sumColumnSymbol, sumColumnSymbol));
                }

                AggregationSignature countSignature = new AggregationSignature(COUNT.getName(), aggSourceColumnName, originalAgg.isDistinct());
                String countColumnName = cubeMetadata.getColumn(countSignature)
                        .orElseThrow(() -> new PrestoException(CUBE_ERROR, "Cannot find column associated with aggregation " + countSignature));
                ColumnHandle countColumnHandle = cubeColumnsMap.get(countColumnName);
                ColumnMetadata countColumnMetadata = metadata.getColumnMetadata(context.getSession(), cubeTableHandle, countColumnHandle);
                Symbol countColumnSymbol = cubeColumnHandles.entrySet()
                        .stream()
                        .filter(entry -> countColumnHandle.equals(entry.getValue()))
                        .findAny()
                        .map(Map.Entry::getKey)
                        .orElse(symbolAllocator.newSymbol("count_" + aggSourceColumnName + "_" + originalOutputSymbol.getName(), countColumnMetadata.getType()));
                if (!cubeScanSymbols.contains(countColumnSymbol)) {
                    cubeScanSymbols.add(countColumnSymbol);
                    cubeColumnHandles.put(countColumnSymbol, countColumnHandle);
                    symbolMetadataMap.put(countColumnSymbol, countColumnMetadata);
                    aggregationColumns.add(new CubeRewriteResult.AggregatorSource(countColumnSymbol, countColumnSymbol));
                }
                averageAggregationColumns.add(new CubeRewriteResult.AverageAggregatorSource(originalOutputSymbol, sumColumnSymbol, countColumnSymbol));
            }
        }

        //Scan output order is important for partitioned cubes. Otherwise, incorrect results may be produced.
        //Refer: https://gitee.com/openlookeng/hetu-core/issues/I4LAYC
        List<ColumnMetadata> cubeColumnMetadataList = cubeTableMetadata.getColumns();
        List<Symbol> scanOutput = new ArrayList<>(cubeScanSymbols);
        scanOutput.sort(Comparator.comparingInt(outSymbol -> cubeColumnMetadataList.indexOf(symbolMetadataMap.get(outSymbol))));

        cubeScanNode = TableScanNode.newInstance(context.getIdAllocator().getNextId(), cubeTableHandle, scanOutput, cubeColumnHandles, ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false);
        return new CubeRewriteResult(cubeScanNode, symbolMetadataMap, dimensionSymbols, aggregationColumns, averageAggregationColumns, computeAvgDividingSumByCount);
    }

    private PlanNode rewriteNodeRecursively(PlanNode node)
    {
        if (node instanceof ProjectNode) {
            ProjectNode projection = ((ProjectNode) node);
            PlanNode rewritten = rewriteNodeRecursively(projection.getSource());
            Assignments assignments = AssignmentUtils.identityAsSymbolReferences(rewritten.getOutputSymbols());
            return new ProjectNode(context.getIdAllocator().getNextId(), rewritten, assignments);
        }
        else if (node instanceof JoinNode) {
            JoinNode localJoinNode = (JoinNode) node;
            PlanNode rewrittenLeftChild = rewriteNodeRecursively(localJoinNode.getLeft());
            PlanNode rewrittenRightChild = rewriteNodeRecursively(localJoinNode.getRight());
            return rewriteJoinNode(localJoinNode, rewrittenLeftChild, rewrittenRightChild);
        }
        else if (node instanceof TableScanNode) {
            TableScanNode scanNode = (TableScanNode) node;
            TableMetadata tableMetadata = metadata.getTableMetadata(context.getSession(), scanNode.getTable());
            final String fqTableName = tableMetadata.getQualifiedName().toString();
            final boolean isSourceTable = sourceTableName.equalsIgnoreCase(fqTableName);
            //replace table scan with Cube table scan
            TableScanNode rewrittenScanNode = isSourceTable ? cubeScanNode : scanNode;
            final String rewrittenTableName = isSourceTable ? cubeTableMetadata.getQualifiedName().toString() : fqTableName;
            rewrittenScanNode.getAssignments().forEach((symbol, colHandle) -> optimizedPlanMappings.put(new ColumnWithTable(rewrittenTableName, colHandle.getColumnName()), symbol));
            return rewrittenScanNode;
        }
        else {
            throw new UnsupportedOperationException("Unexpected plan node. Expected TableScan, JoinNode or ProjectNode. Actual is " + node.getClass());
        }
    }

    private JoinNode rewriteJoinNode(JoinNode original, PlanNode rewrittenLeftChild, PlanNode rewrittenRightChild)
    {
        List<Symbol> joinOutputSymbols = new ArrayList<>(rewrittenLeftChild.getOutputSymbols());
        joinOutputSymbols.addAll(rewrittenRightChild.getOutputSymbols());

        List<JoinNode.EquiJoinClause> rewrittenJoinCriteria = new ArrayList<>();
        for (JoinNode.EquiJoinClause criteria : original.getCriteria()) {
            Symbol rewrittenLeft = optimizedPlanMappings.get(columnRewritesMap.get(criteria.getLeft().getName()));
            Symbol rewrittenRight = optimizedPlanMappings.get(columnRewritesMap.get(criteria.getRight().getName()));
            rewrittenJoinCriteria.add(new JoinNode.EquiJoinClause(rewrittenLeft, rewrittenRight));
        }

        Optional<Symbol> leftHashSymbol = original.getLeftHashSymbol()
                .map(Symbol::getName)
                .map(columnRewritesMap::get)
                .map(optimizedPlanMappings::get);

        Optional<Symbol> rightHashSymbol = original.getRightHashSymbol()
                .map(Symbol::getName)
                .map(columnRewritesMap::get)
                .map(optimizedPlanMappings::get);

        Optional<RowExpression> rewrittenJoinFilter = original.getFilter().map(rowExpression -> RowExpressionTreeRewriter.rewriteWith(new RowExpressionRewriter<Void>()
        {
            @Override
            public RowExpression rewriteVariableReference(VariableReferenceExpression variable, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
            {
                return new VariableReferenceExpression(optimizedPlanMappings.get(columnRewritesMap.get(variable.getName())).getName(), variable.getType());
            }
        }, rowExpression));

        Map<String, Symbol> rewrittenDynamicFilters = original.getDynamicFilters().entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> optimizedPlanMappings.get(columnRewritesMap.get(entry.getValue().getName()))));
        return new JoinNode(
                context.getIdAllocator().getNextId(),
                original.getType(),
                rewrittenLeftChild,
                rewrittenRightChild,
                rewrittenJoinCriteria,
                joinOutputSymbols,
                rewrittenJoinFilter,
                leftHashSymbol,
                rightHashSymbol,
                original.getDistributionType(),
                original.isSpillable(),
                rewrittenDynamicFilters);
    }

    private PlanNode rewriteFilterNode(PlanNode planNode)
    {
        if (filterNode == null) {
            return planNode;
        }
        Optional<RowExpression> rewrittenPredicate = rewriteFilterPredicate();
        return rewrittenPredicate.map(predicate -> new FilterNode(
                context.getIdAllocator().getNextId(),
                planNode,
                rewrittenPredicate.get())).map(PlanNode.class::cast).orElse(planNode);
    }

    private Optional<RowExpression> rewriteFilterPredicate()
    {
        //rewrite the expression by removing source filter predicate as cube would not have those columns necessarily
        Expression queryPredicate = castToExpression(filterNode.getPredicate());
        if (cubeMetadata.getCubeFilter() == null || cubeMetadata.getCubeFilter().getSourceTablePredicate() == null) {
            //nothing more to do. just rewrite the symbol reference of the original predicate
            return Optional.of(castToRowExpression(rewriteSymbolReferenceToTargetMapping(queryPredicate)));
        }

        SqlParser sqlParser = new SqlParser();
        Expression cubeSourcePredicate = sqlParser.createExpression(cubeMetadata.getCubeFilter().getSourceTablePredicate(), new ParsingOptions());
        cubeSourcePredicate = rewriteIdentifiersWithSymbolReference(cubeSourcePredicate);
        Set<Symbol> sourceFilterPredicateColumns = SymbolsExtractor.extractUnique(cubeSourcePredicate);
        Expression modifiedPredicate = ExpressionUtils.filterConjuncts(queryPredicate, expr -> !sourceFilterPredicateColumns.containsAll(SymbolsExtractor.extractUnique(expr)));
        return Optional.ofNullable(modifiedPredicate.equals(BooleanLiteral.TRUE_LITERAL) ? null : castToRowExpression(rewriteSymbolReferenceToTargetMapping(modifiedPredicate)));
    }

    private Expression rewriteIdentifiersWithSymbolReference(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new SymbolReference(node.getValue());
            }
        }, expression);
    }

    private Expression rewriteSymbolReferenceToTargetMapping(Expression predicate)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>() {
            @Override
            public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return toSymbolReference(optimizedPlanMappings.get(columnRewritesMap.get(node.getName())));
            }
        }, predicate);
    }

    private PlanNode rewriteAggregationNode(CubeRewriteResult cubeRewriteResult, PlanNode inputPlanNode)
    {
        TypeProvider typeProvider = context.getSymbolAllocator().getTypes();
        // Add group by
        List<Symbol> groupings = aggregationNode.getGroupingKeys()
                .stream()
                .map(Symbol::getName)
                .map(columnRewritesMap::get)
                .map(optimizedPlanMappings::get)
                .collect(Collectors.toList());

        Map<Symbol, Symbol> cubeScanToAggOutputMap = new HashMap<>();
        // Rewrite AggregationNode using Cube table
        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregationsBuilder = ImmutableMap.builder();
        for (CubeRewriteResult.AggregatorSource aggregatorSource : cubeRewriteResult.getAggregationColumns()) {
            Type type = cubeRewriteResult.getSymbolMetadataMap().get(aggregatorSource.getOriginalAggSymbol()).getType();
            TypeSignature typeSignature = type.getTypeSignature();
            ColumnHandle cubeColHandle = cubeRewriteResult.getTableScanNode().getAssignments().get(aggregatorSource.getScanSymbol());
            ColumnMetadata cubeColumnMetadata = metadata.getColumnMetadata(context.getSession(), cubeTableHandle, cubeColHandle);
            AggregationSignature aggregationSignature = cubeMetadata.getAggregationSignature(cubeColumnMetadata.getName())
                    .orElseThrow(() -> new ColumnNotFoundException(new SchemaTableName("", ""), cubeColHandle.getColumnName()));
            String aggFunction = COUNT.getName().equals(aggregationSignature.getFunction()) ? SUM.getName() : aggregationSignature.getFunction();
            SymbolReference argument = toSymbolReference(aggregatorSource.getScanSymbol());
            FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupFunction(aggFunction, TypeSignatureProvider.fromTypeSignatures(typeSignature));
            cubeScanToAggOutputMap.put(aggregatorSource.getScanSymbol(), aggregatorSource.getOriginalAggSymbol());
            aggregationsBuilder.put(aggregatorSource.getOriginalAggSymbol(), new AggregationNode.Aggregation(
                    new CallExpression(
                            aggFunction,
                            functionHandle,
                            type,
                            ImmutableList.of(castToRowExpression(argument))),
                    ImmutableList.of(castToRowExpression(argument)),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));
        }

        PlanNode planNode = inputPlanNode;
        AggregationNode aggNode = new AggregationNode(context.getIdAllocator().getNextId(),
                planNode,
                aggregationsBuilder.build(),
                singleGroupingSet(groupings),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                AggregationNode.AggregationType.HASH,
                Optional.empty());

        if (cubeRewriteResult.getAvgAggregationColumns().isEmpty()) {
            return aggNode;
        }
        if (!cubeRewriteResult.getComputeAvgDividingSumByCount()) {
            Map<Symbol, Expression> aggregateAssignments = new HashMap<>();
            for (CubeRewriteResult.AggregatorSource aggregatorSource : cubeRewriteResult.getAggregationColumns()) {
                aggregateAssignments.put(aggregatorSource.getOriginalAggSymbol(), toSymbolReference(aggregatorSource.getScanSymbol()));
            }
            planNode = new ProjectNode(context.getIdAllocator().getNextId(),
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
                    .filter(originalAggregationsMap::containsValue)
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
            return new ProjectNode(context.getIdAllocator().getNextId(),
                    aggNode,
                    new Assignments(projections
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, entry -> castToRowExpression(entry.getValue())))));
        }
        return planNode;
    }

    private PlanNode rewriteSymbols(PlanNode planNode)
    {
        // Safety check to remove redundant symbols and rename original column names to intermediate names
        if (planNode.getOutputSymbols().equals(aggregationNode.getOutputSymbols())) {
            return planNode;
        }
        // Map new symbol names to the old symbols
        Map<Symbol, Expression> aggAssignments = new HashMap<>();
        for (Symbol originalAggOutputSymbol : aggregationNode.getOutputSymbols()) {
            if (columnRewritesMap.containsKey(originalAggOutputSymbol.getName())) {
                //rewrite group by symbols if changed during intermediate projections
                aggAssignments.put(originalAggOutputSymbol, toSymbolReference(optimizedPlanMappings.get(columnRewritesMap.get(originalAggOutputSymbol.getName()))));
            }
            else {
                // retain the aggregate assignments as is
                aggAssignments.put(originalAggOutputSymbol, toSymbolReference(originalAggOutputSymbol));
            }
        }
        return new ProjectNode(context.getIdAllocator().getNextId(),
                planNode,
                new Assignments(aggAssignments
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> castToRowExpression(entry.getValue())))));
    }

    void parse()
    {
        parseJoinNode();
        parseFilterNode();
        parseIntermediateProjections();
        parseAggregation();
    }

    private void parseJoinNode()
    {
        parseNodeRecursively(joinNode);
        if (matchingMetadataList.isEmpty()) {
            throw new UnsupportedOperationException("Matching cubes not found. Unable to rewrite");
        }
    }

    private void parseNodeRecursively(PlanNode node)
    {
        if (node instanceof ProjectNode) {
            ProjectNode projection = ((ProjectNode) node);
            validateProjection(projection);
            parseNodeRecursively(projection.getSource());
            handleProjection(projection);
        }
        else if (node instanceof JoinNode) {
            JoinNode localJoinNode = (JoinNode) node;
            parseNodeRecursively(localJoinNode.getLeft());
            parseNodeRecursively(localJoinNode.getRight());
            localJoinNode.getOutputSymbols().stream().map(Symbol::getName).forEach(symbol -> originalPlanMappings.put(symbol, originalPlanMappings.get(symbol)));
            localJoinNode.getCriteria().forEach(equiJoinClause -> {
                //Join condition(s) must be defined on column from Source table
                ColumnWithTable leftColumn = originalPlanMappings.get(equiJoinClause.getLeft().getName());
                ColumnWithTable rightColumn = originalPlanMappings.get(equiJoinClause.getRight().getName());
                ColumnWithTable sourceTableColumn = leftColumn.getFQTableName().equalsIgnoreCase(sourceTableName) ? leftColumn : rightColumn;
                dimensions.add(sourceTableColumn.getColumnName());
                groupBy.add(sourceTableColumn.getColumnName());
                matchingMetadataList.removeIf(metadata -> {
                    //Retain Cube metadata only if the one of the join column is part of Cube
                    return !metadata.getDimensions().contains(sourceTableColumn.getColumnName());
                });
            });
        }
        else if (node instanceof TableScanNode) {
            TableScanNode scanNode = (TableScanNode) node;
            TableMetadata tableMetadata = metadata.getTableMetadata(context.getSession(), scanNode.getTable());
            scanNode.getOutputSymbols().forEach(output -> {
                ColumnWithTable columnWithTable = new ColumnWithTable(tableMetadata.getQualifiedName().toString(), scanNode.getAssignments().get(output).getColumnName());
                originalPlanMappings.put(output.getName(), columnWithTable);
            });
            //Assumption: Cubes are defined on only of the tables involved in Join. That table will be considered Source Table.
            List<CubeMetadata> metadataList = cubeMetaStore.getMetadataList(tableMetadata.getQualifiedName().toString());
            if (sourceTableScanNode == null && !metadataList.isEmpty()) {
                sourceTableScanNode = scanNode;
                sourceTableMetadata = tableMetadata;
                sourceTableName = sourceTableMetadata.getQualifiedName().toString();
                sourceTableHandle = sourceTableScanNode.getTable();
                matchingMetadataList.addAll(metadataList);
                sourceTableColumnMap.putAll(metadata.getColumnHandles(context.getSession(), sourceTableHandle));
            }
        }
        else {
            throw new UnsupportedOperationException("Unexpected plan node. Expected TableScan, JoinNode or ProjectNode. Actual is " + node.getClass());
        }
    }

    private void validateProjection(ProjectNode projection)
    {
        projection.getAssignments().forEach(((symbol, rowExpression) -> {
            if (OriginalExpressionUtils.isExpression(rowExpression)) {
                validateExpression(castToExpression(rowExpression));
            }
            else {
                validateRowExpression(rowExpression);
            }
        }));
    }

    private static void validateExpression(Expression inputExpression)
    {
        Expression expression = inputExpression;
        if (expression instanceof Cast) {
            while (expression instanceof Cast) {
                expression = ((Cast) expression).getExpression();
            }
        }
        if (!(expression instanceof SymbolReference || expression instanceof Literal)) {
            throw new UnsupportedOperationException("Unable to optimize the aggregation sub-tree with Cubes. Project node validation failed.");
        }
    }

    private static void validateRowExpression(RowExpression inputRowExpression)
    {
        RowExpression rowExpression = inputRowExpression;
        if (rowExpression instanceof CallExpression) {
            while (rowExpression instanceof CallExpression) {
                rowExpression = ((CallExpression) rowExpression).getArguments().get(0);
            }
        }

        if (!(rowExpression instanceof VariableReferenceExpression || rowExpression instanceof ConstantExpression)) {
            throw new UnsupportedOperationException("Unable to optimize the aggregation sub-tree with Cubes. Project node validation failed.");
        }
    }

    private void handleProjection(ProjectNode projection)
    {
        Assignments projectionAssignments = projection.getAssignments();
        projection.getOutputSymbols().forEach(output -> {
            Optional<Object> assignedValue = extractAssignedValue(projectionAssignments.get(output));
            assignedValue.ifPresent(value -> {
                if (value instanceof String && originalPlanMappings.containsKey(value)) {
                    //if symbol reference get original value from existing assignments
                    originalPlanMappings.put(output.getName(), originalPlanMappings.get(value));
                }
            });
        });
    }

    private Optional<Object> extractAssignedValue(RowExpression inputRowExpression)
    {
        RowExpression rowExpression = inputRowExpression;
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

    private void parseFilterNode()
    {
        if (filterNode == null) {
            return;
        }

        Expression queryPredicate = processAndFilterJoinConditionsIfExist(castToExpression(filterNode.getPredicate()));
        queryPredicate = rewriteSymbolReferenceUsingColumnName(queryPredicate);
        Pair<Expression, Expression> splitPredicate = splitJoinPredicate(queryPredicate, sourceTableColumnMap.keySet());
        matchingMetadataList.removeIf(cubeMetadata -> !filterPredicateMatches(splitPredicate.getLeft(), cubeMetadata, context.getSession(), context.getSymbolAllocator().getTypes()));
        if (matchingMetadataList.isEmpty()) {
            throw new UnsupportedOperationException("Matching cubes not found. Unable to rewrite");
        }
    }

    private Expression processAndFilterJoinConditionsIfExist(Expression filterPredicate)
    {
        List<Expression> remainingPredicates = new ArrayList<>();
        List<Expression> conjuncts = ExpressionUtils.extractConjuncts(filterPredicate);
        for (Expression conjunct : conjuncts) {
            if (!(conjunct instanceof ComparisonExpression)) {
                remainingPredicates.add(conjunct);
                continue;
            }
            ComparisonExpression eqExpression = (ComparisonExpression) conjunct;
            boolean isJoinCondition = eqExpression.getOperator() == ComparisonExpression.Operator.EQUAL
                    && eqExpression.getLeft() instanceof SymbolReference
                    && eqExpression.getRight() instanceof SymbolReference;
            if (!isJoinCondition) {
                remainingPredicates.add(conjunct);
                continue;
            }
            ColumnWithTable leftColumn = originalPlanMappings.get(((SymbolReference) eqExpression.getLeft()).getName());
            ColumnWithTable rightColumn = originalPlanMappings.get(((SymbolReference) eqExpression.getRight()).getName());
            ColumnWithTable sourceTableColumn = leftColumn.getFQTableName().equalsIgnoreCase(sourceTableName) ? leftColumn : rightColumn;
            dimensions.add(sourceTableColumn.getColumnName());
            groupBy.add(sourceTableColumn.getColumnName());
            matchingMetadataList.removeIf(metadata -> {
                //Retain Cube metadata only if the one of the join column is part of Cube
                return !metadata.getDimensions().contains(sourceTableColumn.getColumnName());
            });
        }
        return ExpressionUtils.and(remainingPredicates);
    }

    private Pair<Expression, Expression> splitJoinPredicate(Expression filterPredicate, Set<String> sourceTableColumns)
    {
        List<Expression> sourceTablePredicates = new ArrayList<>();
        List<Expression> otherPredicates = new ArrayList<>();
        List<Expression> conjuncts = ExpressionUtils.extractConjuncts(filterPredicate);
        conjuncts.forEach(conjunct -> {
            Set<String> columnNames = SymbolsExtractor.extractUnique(conjunct).stream().map(Symbol::getName).collect(Collectors.toSet());
            if (sourceTableColumns.containsAll(columnNames)) {
                filterColumns.addAll(columnNames);
                sourceTablePredicates.add(conjunct);
            }
            else {
                otherPredicates.add(conjunct);
            }
        });
        Expression sourceTablePredicate = sourceTablePredicates.isEmpty() ? null : ExpressionUtils.combineConjuncts(sourceTablePredicates);
        Expression otherTablePredicate = otherPredicates.isEmpty() ? null : ExpressionUtils.combineConjuncts(otherPredicates);
        return new ImmutablePair<>(sourceTablePredicate, otherTablePredicate);
    }

    private Expression rewriteSymbolReferenceUsingColumnName(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                ColumnWithTable columnHandle = originalPlanMappings.get(node.getName());
                return new SymbolReference(columnHandle.getColumnName());
            }
        }, expression);
    }

    private boolean filterPredicateMatches(Expression queryPredicate, CubeMetadata cubeMetadata, Session session, TypeProvider types)
    {
        CubeFilter cubeFilter = cubeMetadata.getCubeFilter();

        if (cubeFilter == null) {
            //Cube was built for entire table
            return queryPredicate == null || doesCubeContainQueryPredicateColumns(queryPredicate, cubeMetadata);
        }

        if (queryPredicate == null) {
            //Query statement has no WHERE clause but CUBE was built for subset of original data
            return false;
        }

        SqlParser sqlParser = new SqlParser();
        Expression cubeSourceTablePredicate = cubeFilter.getSourceTablePredicate() == null ? null : sqlParser.createExpression(cubeFilter.getSourceTablePredicate(), new ParsingOptions());
        Pair<Expression, Expression> queryPredicateSplit = splitQueryPredicate(queryPredicate, cubeSourceTablePredicate);
        if (!arePredicatesEqual(queryPredicateSplit.getLeft(), cubeSourceTablePredicate, metadata, session, types)) {
            log.debug("Cube source table predicate %s not matching query predicate %s", cubeSourceTablePredicate, queryPredicate);
            return false;
        }
        //Check if columns in query predicate are all part of the Cube.
        if ((cubeFilter.getCubePredicate() != null && queryPredicateSplit.getRight() == null)
                || (queryPredicateSplit.getRight() != null && !doesCubeContainQueryPredicateColumns(queryPredicateSplit.getRight(), cubeMetadata))) {
            // Query predicate does not exactly match Cube predicate
            // OR
            // Cube does not contain all columns in the remaining predicate
            return false;
        }
        if (cubeFilter.getCubePredicate() == null) {
            //Cube has no additional predicates to compare with. i.e. Cube can be used to optimize the query
            return true;
        }
        ExpressionDomainTranslator.ExtractionResult decomposedQueryPredicate = ExpressionDomainTranslator.fromPredicate(metadata, session, queryPredicateSplit.getRight(), types);
        if (!BooleanLiteral.TRUE_LITERAL.equals(decomposedQueryPredicate.getRemainingExpression())) {
            log.error("StarTree cube cannot support predicate %s", castToExpression(filterNode.getPredicate()));
            return false;
        }

        Expression cubePredicate = ExpressionUtils.rewriteIdentifiersToSymbolReferences(sqlParser.createExpression(cubeFilter.getCubePredicate(), new ParsingOptions()));
        ExpressionDomainTranslator.ExtractionResult decomposedCubePredicate = ExpressionDomainTranslator.fromPredicate(metadata, session, cubePredicate, types);
        if (!BooleanLiteral.TRUE_LITERAL.equals(decomposedCubePredicate.getRemainingExpression())) {
            //Can't create TupleDomain construct for this query predicate.
            //eg: (col1 = 1 AND col2 = 1) OR (col1 > 1 and col2 = 2)
            //Extract disjunctions from the expression and evaluate separately
            return atLeastMatchesOne(ExpressionUtils.extractDisjuncts(cubePredicate), decomposedQueryPredicate.getTupleDomain(), session, types);
        }
        return decomposedCubePredicate.getTupleDomain().contains(decomposedQueryPredicate.getTupleDomain());
    }

    private boolean doesCubeContainQueryPredicateColumns(Expression queryPredicate, CubeMetadata cubeMetadata)
    {
        Set<String> cubeColumns = new HashSet<>(cubeMetadata.getDimensions());
        Set<String> queryPredicateColumns = SymbolsExtractor.extractUnique(queryPredicate)
                .stream()
                .map(Symbol::getName)
                .collect(Collectors.toSet());
        return cubeColumns.containsAll(queryPredicateColumns);
    }

    /**
     * Split query predicate into two different expressions.
     * First expression that can be compared with source table.
     * Second expression is compared with Cube data range(defined as another predicate)
     */
    private Pair<Expression, Expression> splitQueryPredicate(Expression queryPredicate, Expression sourceTablePredicate)
    {
        if (sourceTablePredicate == null) {
            return new ImmutablePair<>(null, queryPredicate);
        }
        Set<Identifier> sourceFilterPredicateColumns = ExpressionUtils.getIdentifiers(sourceTablePredicate);
        List<Expression> conjuncts = ExpressionUtils.extractConjuncts(queryPredicate);
        List<Expression> sourceFilterPredicates = new ArrayList<>();
        List<Expression> remainingPredicates = new ArrayList<>();
        conjuncts.forEach(conjunct -> {
            List<Identifier> identifiers = SymbolsExtractor.extractUnique(conjunct)
                    .stream()
                    .map(Symbol::getName)
                    .map(Identifier::new)
                    .collect(Collectors.toList());
            if (sourceFilterPredicateColumns.containsAll(identifiers)) {
                sourceFilterPredicates.add(conjunct);
            }
            else {
                remainingPredicates.add(conjunct);
            }
        });
        Expression cubePredicate1 = sourceFilterPredicateColumns.isEmpty() ? null : ExpressionUtils.combineConjuncts(sourceFilterPredicates);
        Expression cubePredicate2 = remainingPredicates.isEmpty() ? null : ExpressionUtils.combineConjuncts(remainingPredicates);
        return new ImmutablePair<>(cubePredicate1, cubePredicate2);
    }

    private boolean arePredicatesEqual(Expression inputLeft, Expression inputRight, Metadata metadata, Session session, TypeProvider types)
    {
        Expression left = inputLeft;
        Expression right = inputRight;
        if (left == null && right == null) {
            return true;
        }
        else if (left == null || right == null) {
            return false;
        }
        left = ExpressionUtils.rewriteIdentifiersToSymbolReferences(left);
        right = ExpressionUtils.rewriteIdentifiersToSymbolReferences(right);
        ExpressionDomainTranslator.ExtractionResult leftDecomposed = ExpressionDomainTranslator.fromPredicate(metadata, session, left, types);
        ExpressionDomainTranslator.ExtractionResult rightDecomposed = ExpressionDomainTranslator.fromPredicate(metadata, session, right, types);
        if (!BooleanLiteral.TRUE_LITERAL.equals(leftDecomposed.getRemainingExpression()) || !BooleanLiteral.TRUE_LITERAL.equals(rightDecomposed.getRemainingExpression())) {
            log.error("Star tree cube cannot support predicate %s", left);
            return false;
        }
        return leftDecomposed.getTupleDomain().equals(rightDecomposed.getTupleDomain());
    }

    private boolean atLeastMatchesOne(List<Expression> cubePredicates, TupleDomain<Symbol> queryPredicate, Session session, TypeProvider types)
    {
        return cubePredicates.stream().anyMatch(cubePredicate -> {
            ExpressionDomainTranslator.ExtractionResult decomposedCubePredicate = ExpressionDomainTranslator.fromPredicate(metadata, session, cubePredicate, types);
            return BooleanLiteral.TRUE_LITERAL.equals(decomposedCubePredicate.getRemainingExpression())
                    && decomposedCubePredicate.getTupleDomain().contains(queryPredicate);
        });
    }

    private void parseIntermediateProjections()
    {
        for (ProjectNode intermediateProjection : Lists.reverse(intermediateProjections)) {
            validateProjection(intermediateProjection);
            handleProjection(intermediateProjection);
        }
    }

    private void parseAggregation()
    {
        aggregationNode.getAggregations().forEach((outputSymbol, aggregation) -> {
            AggregationSignature signature = getSignature(aggregation);
            originalAggregationsMap.put(signature, outputSymbol);
        });

        boolean hasCountDistinctAgg = originalAggregationsMap.keySet().stream().anyMatch(aggregationSignature -> aggregationSignature.getFunction().equalsIgnoreCase(COUNT.toString()) && aggregationSignature.isDistinct());
        boolean groupByColumnFromSourceTable = true;
        for (Symbol originalAggSymbol : aggregationNode.getGroupingKeys()) {
            ColumnWithTable columnWithTable = originalPlanMappings.get(originalAggSymbol.getName());
            if (columnWithTable.getFQTableName().equalsIgnoreCase(sourceTableMetadata.getQualifiedName().toString())) {
                //add only columns if its of the source table. Join columns are added as well to group by list
                dimensions.add(columnWithTable.getColumnName());
                groupBy.add(columnWithTable.getColumnName());
            }
            else {
                //Group by contains column other tables also - Not just from sourceTable
                groupByColumnFromSourceTable = false;
            }
        }
        groupByFromOtherTable = !groupByColumnFromSourceTable;
        matchingMetadataList.removeIf(cubeMetadata -> (hasCountDistinctAgg && (groupByFromOtherTable || !(cubeMetadata.getGroup().equals(groupBy)))) || (!cubeMetadata.getGroup().containsAll(groupBy)));
        if (matchingMetadataList.isEmpty()) {
            throw new UnsupportedOperationException("Matching cubes not found. Unable to rewrite");
        }
    }

    private AggregationSignature getSignature(AggregationNode.Aggregation aggregation)
    {
        boolean distinct = aggregation.isDistinct();
        String aggFunctionName = aggregation.getFunctionCall().getDisplayName();
        String columnName = getColumnName(aggregation);

        CubeAggregateFunction cubeAggregateFunction = CubeAggregateFunction.valueOf(aggFunctionName.toUpperCase(ENGLISH));
        switch (cubeAggregateFunction) {
            case SUM:
                return AggregationSignature.sum(columnName, distinct);
            case COUNT:
                return (columnName == null ? AggregationSignature.count() : AggregationSignature.count(columnName, distinct));
            case AVG:
                return AggregationSignature.avg(columnName, distinct);
            case MAX:
                return AggregationSignature.max(columnName, distinct);
            case MIN:
                return AggregationSignature.min(columnName, distinct);
            default:
                throw new PrestoException(NOT_SUPPORTED, format("Unsupported aggregation function : %s", aggFunctionName));
        }
    }

    private String getColumnName(AggregationNode.Aggregation aggregation)
    {
        List<Expression> arguments = aggregation.getArguments() == null ? ImmutableList.of() : aggregation.getArguments()
                .stream()
                .map(OriginalExpressionUtils::castToExpression).collect(Collectors.toList());
        if (arguments.size() > 1) {
            throw new UnsupportedOperationException("Cannot support more than one argument in aggregate function");
        }
        if (arguments.isEmpty() || (arguments.get(0) instanceof LongLiteral && ((LongLiteral) arguments.get(0)).getValue() == 1)) {
            return null;
        }
        Symbol only = Iterables.getOnlyElement(SymbolsExtractor.extractUnique(aggregation));
        ColumnWithTable columnHandle = originalPlanMappings.get(only.getName());
        if (!columnHandle.getFQTableName().equalsIgnoreCase(sourceTableMetadata.getQualifiedName().toString())) {
            throw new UnsupportedOperationException("No matching cube found. Aggregation not part of the cube.");
        }
        return columnHandle.getColumnName();
    }

    private static class ColumnWithTable
    {
        private final String fqTableName;
        private final String columnName;

        public ColumnWithTable(String fqTableName, String columnName)
        {
            this.fqTableName = fqTableName;
            this.columnName = columnName;
        }

        public String getFQTableName()
        {
            return fqTableName;
        }

        public String getColumnName()
        {
            return columnName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnWithTable that = (ColumnWithTable) o;
            return Objects.equals(fqTableName, that.fqTableName)
                    && Objects.equals(columnName, that.columnName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(fqTableName, columnName);
        }

        @Override
        public String toString()
        {
            return "ColumnWithTable{" +
                    "fqTableName='" + fqTableName + '\'' +
                    ", columnName='" + columnName + '\'' +
                    '}';
        }
    }
}
