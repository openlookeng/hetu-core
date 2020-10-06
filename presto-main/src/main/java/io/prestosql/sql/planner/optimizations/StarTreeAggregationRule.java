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

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeStatement;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.cube.CubeManager;
import io.prestosql.cube.CubeStatementGenerator;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.ExpressionDomainTranslator;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static io.prestosql.SystemSessionProperties.isEnableStarTreeIndex;
import static io.prestosql.cube.CubeManager.STAR_TREE;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.spi.connector.StandardWarningCode.EXPIRED_CUBE;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.anyPlan;
import static io.prestosql.sql.planner.plan.Patterns.optionalSource;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Objects.requireNonNull;

/**
 * This rule rewrites iceberg logical plans into a simple logical plan with {@link TableScanNode}
 * containing pre-aggregated values from the star-tree table. This rule connects to the
 * external system to retrieve the star-tree cube and there is a chance for the pre-aggregated
 * results to be obsolete.
 *
 * <p>
 * The rule can be enabled or disabled using the configuration property
 * <code>{@literal optimizer.enable-star-tree-index}</code> or the session propertyrowExpression
 * <code>{@literal enable_star_tree_index}</code>. The configuration property is defined in
 * {@link FeaturesConfig} and the session property is defined in
 * {@link SystemSessionProperties}.
 */
public class StarTreeAggregationRule
        implements Rule<AggregationNode>
{
    private static final Logger LOGGER = Logger.get(StarTreeAggregationRule.class);

    public static final String AVG = "avg";

    public static final String COUNT = "count";

    public static final String SUM = "sum";

    public static final String MIN = "min";

    public static final String MAX = "max";

    /**
     * Aggregation functions supported by the Star-Tree index.
     */
    private static final Set<String> SUPPORTED_FUNCTIONS = ImmutableSet.of(AVG, COUNT, SUM, MIN, MAX);

    private static final Capture<Optional<PlanNode>> OPTIONAL_PRE_PROJECT_ONE = newCapture();

    private static final Capture<Optional<PlanNode>> OPTIONAL_PRE_PROJECT_TWO = newCapture();

    private static final Capture<Optional<PlanNode>> OPTIONAL_FILTER = newCapture();

    private static final Capture<Optional<PlanNode>> OPTIONAL_POST_PROJECT = newCapture();

    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

    /*
     * This pattern captures any AggregationNode with SUPPORTED_FUNCTIONS followed by an
     * optional ProjectNode followed by an optional FilterNode followed by an ProjectNode
     * and ends with a TableScanNode.
     * <p>
     * AggregationNode
     * |- ProjectNode[Optional]
     * .  |- ProjectNode[Optional]
     * .  .  |- FilterNode[Optional]
     * .  .  .  |- ProjectNode[Optional]
     * .  .  .  .  |- TableScanNode
     */
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(StarTreeAggregationRule::isSupportedAggregation)
            .with(optionalSource(ProjectNode.class)
                    .matching(anyPlan().capturedAsIf(node -> node instanceof ProjectNode, OPTIONAL_PRE_PROJECT_ONE)
                            .with(optionalSource(ProjectNode.class)
                                    .matching(anyPlan().capturedAsIf(node -> node instanceof ProjectNode, OPTIONAL_PRE_PROJECT_TWO)
                                            .with(optionalSource(FilterNode.class)
                                                    .matching(anyPlan().capturedAsIf(x -> x instanceof FilterNode, OPTIONAL_FILTER)
                                                            .with(optionalSource(ProjectNode.class)
                                                                    .matching(anyPlan().capturedAsIf(node -> node instanceof ProjectNode, OPTIONAL_POST_PROJECT)
                                                                            .with(source().matching(
                                                                                    tableScan()
                                                                                            .capturedAs(TABLE_SCAN)))))))))));

    private final CubeManager cubeManager;

    private CubeMetaStore cubeMetaStore;

    private final Metadata metadata;

    public StarTreeAggregationRule(CubeManager cubeManager, Metadata metadata)
    {
        this.cubeManager = cubeManager;
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        if (isEnableStarTreeIndex(session) && this.cubeManager.getCubeProvider(STAR_TREE).isPresent()) {
            if (this.cubeMetaStore == null) {
                // Creating CubeMetaStore in the constructor is too early. By that time, plugins are not loaded
                // That's why, the cubeMetaStore is lazy loaded here
                synchronized (this) {
                    if (this.cubeMetaStore == null) {
                        Optional<CubeMetaStore> optionalCubeMetaStore = this.cubeManager.getMetaStore(STAR_TREE);
                        if (!optionalCubeMetaStore.isPresent()) {
                            return false;
                        }
                        this.cubeMetaStore = optionalCubeMetaStore.get();
                    }
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        long startOptimizationTime = System.currentTimeMillis();

        Optional<PlanNode> preProjectNodeOne = captures.get(OPTIONAL_PRE_PROJECT_ONE);
        Optional<PlanNode> preProjectNodeTwo = captures.get(OPTIONAL_PRE_PROJECT_TWO);
        Optional<PlanNode> postProjectNode = captures.get(OPTIONAL_POST_PROJECT);
        Optional<PlanNode> filterNode = captures.get(OPTIONAL_FILTER);
        TableScanNode tableScanNode = captures.get(TABLE_SCAN);

        // Check if project nodes are supported
        if (!supportedProjectNode(preProjectNodeOne) ||
                !supportedProjectNode(preProjectNodeTwo) ||
                !supportedProjectNode(postProjectNode)) {
            // Unsupported ProjectNode is detected
            return Result.empty();
        }

        List<ProjectNode> projectNodes = new LinkedList<>();
        preProjectNodeOne.ifPresent(planNode -> projectNodes.add((ProjectNode) planNode));
        preProjectNodeTwo.ifPresent(planNode -> projectNodes.add((ProjectNode) planNode));
        postProjectNode.ifPresent(planNode -> projectNodes.add((ProjectNode) planNode));

        Map<String, Object> symbolMappings = buildSymbolMappings(node, projectNodes, filterNode, tableScanNode);

        try {
            return optimize(node,
                    filterNode,
                    tableScanNode,
                    symbolMappings,
                    context.getSession(),
                    context.getSymbolAllocator(),
                    context.getIdAllocator(),
                    context.getWarningCollector());
        }
        catch (UnsupportedOperationException | IllegalArgumentException | IllegalStateException | PrestoException ex) {
            LOGGER.warn("Encountered exception '" + ex.getMessage() + "' while applying the StartTreeAggregationRule", ex);
            return Result.empty();
        }
        finally {
            long endOptimizationTime = System.currentTimeMillis();
            LOGGER.debug("Star-tree total optimization time: %d millis", (endOptimizationTime - startOptimizationTime));
        }
    }

    public Result optimize(AggregationNode aggregationNode,
            Optional<PlanNode> filterNode,
            TableScanNode tableScanNode,
            Map<String, Object> symbolMapping,
            Session session,
            PlanSymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        TableHandle tableHandle = tableScanNode.getTable();
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
        String tableName = tableMetadata.getQualifiedName().toString();
        CubeStatement statement = CubeStatementGenerator.generate(
                tableName,
                aggregationNode,
                filterNode.map(FilterNode.class::cast).orElse(null),
                symbolMapping);

        // Don't use star-tree for non-aggregate queries
        if (statement.getAggregations().isEmpty()) {
            return Result.empty();
        }

        List<CubeMetadata> cubeMetadataList = CubeMetadata.filter(this.cubeMetaStore.getMetadataList(statement.getFrom()), statement);

        //Compare FilterNode predicate with Cube predicates to evaluate which cube can be used.
        List<CubeMetadata> matchedCubeMetadataList = cubeMetadataList.stream()
                .filter(cubeMetadata -> filterPredicateMatches(filterNode.map(FilterNode.class::cast), cubeMetadata, session, symbolAllocator.getTypes()))
                .collect(Collectors.toList());

        //Match based on filter conditions
        if (matchedCubeMetadataList.isEmpty()) {
            return Result.empty();
        }

        LongSupplier lastModifiedTimeSupplier = metadata.getTableLastModifiedTimeSupplier(session, tableHandle);
        if (lastModifiedTimeSupplier != null) {
            long lastModifiedTime = lastModifiedTimeSupplier.getAsLong();
            matchedCubeMetadataList = matchedCubeMetadataList.stream()
                    .filter(cubeMetadata -> cubeMetadata.getLastUpdated() > lastModifiedTime)
                    .collect(Collectors.toList());
        }

        if (matchedCubeMetadataList.isEmpty()) {
            warningCollector.add(new PrestoWarning(EXPIRED_CUBE, tableName + " has been modified after creating cubes. Ignoring expired cubes."));
            return Result.empty();
        }

        //If multiple cubes are matching then lets select the recent built cube
        //so sort the cube based on the last updated time stamp
        matchedCubeMetadataList.sort(Comparator.comparingLong(CubeMetadata::getLastUpdated).reversed());

        AggregationRewriteWithCube aggregationRewriteWithCube = new AggregationRewriteWithCube(metadata, session, symbolAllocator, idAllocator, symbolMapping, matchedCubeMetadataList.get(0));
        return Result.ofPlanNode(aggregationRewriteWithCube.rewrite(aggregationNode, filterNode.orElse(null)));
    }

    private boolean filterPredicateMatches(Optional<FilterNode> filterNode, CubeMetadata cubeMetadata, Session session, TypeProvider types)
    {
        if (cubeMetadata.getPredicateString() == null) {
            return true;
        }
        if (!filterNode.isPresent()) {
            return false;
        }
        SqlParser sqlParser = new SqlParser();
        Expression cubePredicateAsExpr = sqlParser.createExpression(cubeMetadata.getPredicateString(), new ParsingOptions());
        cubePredicateAsExpr = ExpressionUtils.rewriteIdentifiersToSymbolReferences(cubePredicateAsExpr);

        RowExpression statementPredicate = filterNode.get().getPredicate();
        if (isExpression(statementPredicate)) {
            Expression statementPredicateAsExpr = castToExpression(statementPredicate);
            statementPredicateAsExpr = ExpressionUtils.rewriteIdentifiersToSymbolReferences(statementPredicateAsExpr);
            ExpressionDomainTranslator.ExtractionResult decomposeStatementPredicate = ExpressionDomainTranslator.fromPredicate(metadata, session, statementPredicateAsExpr, types);
            if (!BooleanLiteral.TRUE_LITERAL.equals(decomposeStatementPredicate.getRemainingExpression())) {
                LOGGER.error("StarTree index cannot support predicate %s", statementPredicate);
                return false;
            }
            ExpressionDomainTranslator.ExtractionResult decomposedCubePredicate = ExpressionDomainTranslator.fromPredicate(metadata, session, cubePredicateAsExpr, types);
            return decomposedCubePredicate.getTupleDomain().contains(decomposeStatementPredicate.getTupleDomain());
        }
        else {
            LOGGER.error("StarTree index cannot support predicate %s", statementPredicate);
            return false;
        }
    }

    /**
     * Construct a map of symbols mapping to constant value or the underlying column name.
     *
     * @param projections the list of ProjectNodes in between the aggregation node and the tableScan node
     * @param tableScanNode the table scan node
     * @return output symbols to constant or actual column name mapping
     */
    public static Map<String, Object> buildSymbolMappings(AggregationNode aggregationNode,
            List<ProjectNode> projections,
            Optional<PlanNode> filterNode,
            TableScanNode tableScanNode)
    {
        // Initialize a map with outputSymbols mapping to themselves
        Map<String, Object> symbolMapping = new HashMap<>();
        aggregationNode.getOutputSymbols().stream().map(Symbol::getName).forEach(symbol -> symbolMapping.put(symbol, symbol));
        aggregationNode.getAggregations().values().forEach(aggregation -> SymbolsExtractor.extractUnique(aggregation).stream().map(Symbol::getName)
                .forEach(symbol -> symbolMapping.put(symbol, symbol)));
        filterNode.ifPresent(planNode -> (SymbolsExtractor.extractUnique(((FilterNode) planNode).getPredicate())).stream()
                .map(Symbol::getName)
                .forEach(symbol -> symbolMapping.put(symbol, symbol)));

        // Track how a symbol name is renamed throughout all project nodes
        for (ProjectNode node : projections) {
            Map<Symbol, RowExpression> assignments = node.getAssignments().getMap();
            // ProjectNode is identity
            for (Map.Entry<String, Object> symbolEntry : symbolMapping.entrySet()) {
                RowExpression rowExpression = assignments.get(new Symbol(String.valueOf(symbolEntry.getValue())));
                if (rowExpression == null) {
                    continue;
                }
                if (OriginalExpressionUtils.isExpression(rowExpression)) {
                    Expression expression = castToExpression(rowExpression);
                    if (expression instanceof Cast) {
                        expression = ((Cast) expression).getExpression();
                    }

                    if (expression instanceof SymbolReference) {
                        symbolEntry.setValue(((SymbolReference) expression).getName());
                    }
                    else if (expression instanceof Literal) {
                        symbolEntry.setValue(expression);
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
                        continue;
                    }
                    symbolEntry.setValue(((VariableReferenceExpression) rowExpression).getName());
                }
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
                .allMatch(StarTreeAggregationRule::isSupported);
    }

    static boolean isSupported(AggregationNode.Aggregation aggregation)
    {
        return SUPPORTED_FUNCTIONS.contains(aggregation.getSignature().getName()) &&
                aggregation.getSignature().getArgumentTypes().size() <= 1 &&
                (!aggregation.isDistinct() || aggregation.getSignature().getName().equals(COUNT));
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
}
