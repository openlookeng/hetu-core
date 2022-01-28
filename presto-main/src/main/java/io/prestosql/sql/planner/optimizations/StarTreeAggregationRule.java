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

import io.airlift.log.Logger;
import io.hetu.core.spi.cube.CubeFilter;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeStatement;
import io.hetu.core.spi.cube.aggregator.AggregationSignature;
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
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.ExpressionDomainTranslator;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
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
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
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
            .matching(CubeOptimizerUtil::isSupportedAggregation)
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
        if (!CubeOptimizerUtil.supportedProjectNode(preProjectNodeOne) ||
                !CubeOptimizerUtil.supportedProjectNode(preProjectNodeTwo) ||
                !CubeOptimizerUtil.supportedProjectNode(postProjectNode)) {
            // Unsupported ProjectNode is detected
            return Result.empty();
        }

        List<ProjectNode> projectNodes = new LinkedList<>();
        preProjectNodeOne.ifPresent(planNode -> projectNodes.add((ProjectNode) planNode));
        preProjectNodeTwo.ifPresent(planNode -> projectNodes.add((ProjectNode) planNode));
        postProjectNode.ifPresent(planNode -> projectNodes.add((ProjectNode) planNode));
        TableMetadata tableMetadata = metadata.getTableMetadata(context.getSession(), tableScanNode.getTable());
        Map<String, Object> symbolMappings = CubeOptimizerUtil.buildSymbolMappings(node, projectNodes, filterNode, tableScanNode, tableMetadata);
        try {
            return optimize(node,
                    filterNode.orElse(null),
                    tableScanNode,
                    symbolMappings,
                    context.getSession(),
                    context.getSymbolAllocator(),
                    context.getIdAllocator(),
                    context.getWarningCollector());
        }
        catch (RuntimeException ex) {
            LOGGER.warn("Encountered exception '" + ex.getMessage() + "' while applying the StartTreeAggregationRule", ex);
            return Result.empty();
        }
        finally {
            long endOptimizationTime = System.currentTimeMillis();
            LOGGER.debug("Star-tree total optimization time: %d millis", (endOptimizationTime - startOptimizationTime));
        }
    }

    public Result optimize(AggregationNode aggregationNode,
            final PlanNode filterNode,
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
                symbolMapping);

        // Don't use star-tree for non-aggregate queries
        if (statement.getAggregations().isEmpty()) {
            return Result.empty();
        }
        boolean hasDistinct = statement.getAggregations().stream().anyMatch(AggregationSignature::isDistinct);
        // Do not use cube for queries that contains only count distinct aggregation and no group by clause
        // Example: SELECT COUNT(DISTINCT userid) FROM usage_history WHERE day BETWEEN 1 AND 7
        // Since cube is pre-aggregated, utilising it for such queries could return incorrect result
        if (aggregationNode.hasEmptyGroupingSet() && hasDistinct) {
            return Result.empty();
        }

        List<CubeMetadata> cubeMetadataList = CubeMetadata.filter(this.cubeMetaStore.getMetadataList(statement.getFrom()), statement);

        //Compare FilterNode predicate with Cube predicates to evaluate which cube can be used.
        List<CubeMetadata> matchedCubeMetadataList = cubeMetadataList.stream()
                .filter(cubeMetadata -> filterPredicateMatches((FilterNode) filterNode, cubeMetadata, session, symbolAllocator.getTypes()))
                .collect(Collectors.toList());

        //Match based on filter conditions
        if (matchedCubeMetadataList.isEmpty()) {
            return Result.empty();
        }

        LongSupplier lastModifiedTimeSupplier = metadata.getTableLastModifiedTimeSupplier(session, tableHandle);
        if (lastModifiedTimeSupplier == null) {
            warningCollector.add(new PrestoWarning(EXPIRED_CUBE, "Unable to identify last modified time of " + tableName + ". Ignoring star tree cubes."));
            return Result.empty();
        }

        //Filter out cubes that were created before the source table was updated
        long lastModifiedTime = lastModifiedTimeSupplier.getAsLong();
        //There was a problem retrieving last modified time, we should skip using star tree rather than failing the query
        if (lastModifiedTime == -1L) {
            return Result.empty();
        }
        matchedCubeMetadataList = matchedCubeMetadataList.stream()
                .filter(cubeMetadata -> cubeMetadata.getSourceTableLastUpdatedTime() >= lastModifiedTime)
                .collect(Collectors.toList());

        if (matchedCubeMetadataList.isEmpty()) {
            warningCollector.add(new PrestoWarning(EXPIRED_CUBE, tableName + " has been modified after creating cubes. Ignoring expired cubes."));
            return Result.empty();
        }

        //If multiple cubes are matching then lets select the recent built cube
        //so sort the cube based on the last updated time stamp
        matchedCubeMetadataList.sort(Comparator.comparingLong(CubeMetadata::getLastUpdatedTime).reversed());
        CubeMetadata matchedCubeMetadata = matchedCubeMetadataList.get(0);
        AggregationRewriteWithCube aggregationRewriteWithCube = new AggregationRewriteWithCube(metadata, session, symbolAllocator, idAllocator, symbolMapping, matchedCubeMetadata);
        return Result.ofPlanNode(aggregationRewriteWithCube.rewrite(aggregationNode, rewriteByRemovingSourceFilter(filterNode, matchedCubeMetadata)));
    }

    private FilterNode rewriteByRemovingSourceFilter(PlanNode filterNode, CubeMetadata matchedCubeMetadata)
    {
        FilterNode rewritten = (FilterNode) filterNode;
        if (filterNode != null && matchedCubeMetadata.getCubeFilter() != null && matchedCubeMetadata.getCubeFilter().getSourceTablePredicate() != null) {
            //rewrite the expression by removing source filter predicate as cube would not have those columns necessarily
            Expression predicate = castToExpression(((FilterNode) filterNode).getPredicate());
            SqlParser sqlParser = new SqlParser();
            Set<Identifier> sourceFilterPredicateColumns = ExpressionUtils.getIdentifiers(sqlParser.createExpression(matchedCubeMetadata.getCubeFilter().getSourceTablePredicate(), new ParsingOptions()));
            predicate = ExpressionUtils.filterConjuncts(predicate,
                    conjunct -> !sourceFilterPredicateColumns.containsAll(SymbolsExtractor.extractUnique(conjunct)
                            .stream()
                            .map(Symbol::getName)
                            .map(Identifier::new)
                            .collect(Collectors.toList())));
            rewritten = new FilterNode(filterNode.getId(), ((FilterNode) filterNode).getSource(), castToRowExpression(predicate));
        }
        return rewritten;
    }

    private boolean atLeastMatchesOne(List<Expression> cubePredicates, TupleDomain<Symbol> queryPredicate, Session session, TypeProvider types)
    {
        return cubePredicates.stream().anyMatch(cubePredicate -> {
            ExpressionDomainTranslator.ExtractionResult decomposedCubePredicate = ExpressionDomainTranslator.fromPredicate(metadata, session, cubePredicate, types);
            return BooleanLiteral.TRUE_LITERAL.equals(decomposedCubePredicate.getRemainingExpression())
                    && decomposedCubePredicate.getTupleDomain().contains(queryPredicate);
        });
    }

    private boolean filterPredicateMatches(FilterNode filterNode, CubeMetadata cubeMetadata, Session session, TypeProvider types)
    {
        CubeFilter cubeFilter = cubeMetadata.getCubeFilter();

        if (cubeFilter == null) {
            //Cube was built for entire table
            return filterNode == null || doesCubeContainQueryPredicateColumns(castToExpression(filterNode.getPredicate()), cubeMetadata);
        }

        if (filterNode == null) {
            //Query statement has no WHERE clause but CUBE was built for subset of original data
            return false;
        }

        SqlParser sqlParser = new SqlParser();
        Expression queryPredicate = castToExpression(filterNode.getPredicate());
        Expression sourceTablePredicate = cubeFilter.getSourceTablePredicate() == null ? null : sqlParser.createExpression(cubeFilter.getSourceTablePredicate(), new ParsingOptions());
        Pair<Expression, Expression> splitQueryPredicate = splitQueryPredicate(queryPredicate, sourceTablePredicate);
        if (!arePredicatesEqual(splitQueryPredicate.getLeft(), sourceTablePredicate, metadata, session, types)) {
            LOGGER.debug("Cube source table predicate %s not matching query predicate %s", sourceTablePredicate, queryPredicate);
            return false;
        }
        //Check if columns in query predicate are all part of the Cube.
        if ((cubeFilter.getCubePredicate() != null && splitQueryPredicate.getRight() == null)
                || (splitQueryPredicate.getRight() != null && !doesCubeContainQueryPredicateColumns(splitQueryPredicate.getRight(), cubeMetadata))) {
            // Query predicate does not exactly match Cube predicate
            // OR
            // Cube does not contain all columns in the remaining predicate
            return false;
        }
        if (cubeFilter.getCubePredicate() == null) {
            //Cube has no additional predicates to compare with. i.e. Cube can be used to optimize the query
            return true;
        }
        Expression cubePredicate = ExpressionUtils.rewriteIdentifiersToSymbolReferences(sqlParser.createExpression(cubeFilter.getCubePredicate(), new ParsingOptions()));
        ExpressionDomainTranslator.ExtractionResult decomposedQueryPredicate = ExpressionDomainTranslator.fromPredicate(metadata, session, splitQueryPredicate.getRight(), types);
        if (!BooleanLiteral.TRUE_LITERAL.equals(decomposedQueryPredicate.getRemainingExpression())) {
            LOGGER.error("StarTree cube cannot support predicate %s", castToExpression(filterNode.getPredicate()));
            return false;
        }
        ExpressionDomainTranslator.ExtractionResult decomposedCubePredicate = ExpressionDomainTranslator.fromPredicate(metadata, session, cubePredicate, types);
        if (!BooleanLiteral.TRUE_LITERAL.equals(decomposedCubePredicate.getRemainingExpression())) {
            //Can't create TupleDomain construct for this query predicate.
            //eg: (col1 = 1 AND col2 = 1) OR (col1 > 1 and col2 = 2)
            //Extract disjuncts from the Expression expression and evaluate separately
            return atLeastMatchesOne(ExpressionUtils.extractDisjuncts(cubePredicate), decomposedQueryPredicate.getTupleDomain(), session, types);
        }
        return decomposedCubePredicate.getTupleDomain().contains(decomposedQueryPredicate.getTupleDomain());
    }

    private boolean doesCubeContainQueryPredicateColumns(Expression queryPredicate, CubeMetadata cubeMetadata)
    {
        Set<Identifier> cubeColumns = new HashSet<>();
        cubeMetadata.getDimensions().stream().map(Identifier::new).forEach(cubeColumns::add);
        Set<Identifier> queryPredicateColumns = SymbolsExtractor.extractUnique(queryPredicate)
                .stream()
                .map(Symbol::getName)
                .map(Identifier::new)
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
            LOGGER.error("Star tree cube cannot support predicate %s", left);
            return false;
        }
        return leftDecomposed.getTupleDomain().equals(rightDecomposed.getTupleDomain());
    }
}
